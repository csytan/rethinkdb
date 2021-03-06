// Copyright 2010-2014 RethinkDB, all rights reserved.
#include "rdb_protocol/btree.hpp"

#include <functional>
#include <iterator>
#include <set>
#include <string>
#include <vector>

#include "errors.hpp"
#include <boost/optional.hpp>

#include "btree/backfill.hpp"
#include "btree/concurrent_traversal.hpp"
#include "btree/erase_range.hpp"
#include "btree/get_distribution.hpp"
#include "btree/operations.hpp"
#include "btree/parallel_traversal.hpp"
#include "btree/slice.hpp"
#include "buffer_cache/alt/serialize_onto_blob.hpp"
#include "concurrency/coro_pool.hpp"
#include "concurrency/queue/unlimited_fifo.hpp"
#include "containers/archive/boost_types.hpp"
#include "containers/archive/buffer_group_stream.hpp"
#include "containers/archive/buffer_stream.hpp"
#include "containers/scoped.hpp"
#include "rdb_protocol/geo/exceptions.hpp"
#include "rdb_protocol/geo/indexing.hpp"
#include "rdb_protocol/blob_wrapper.hpp"
#include "rdb_protocol/func.hpp"
#include "rdb_protocol/geo_traversal.hpp"
#include "rdb_protocol/lazy_json.hpp"
#include "rdb_protocol/pseudo_geometry.hpp"
#include "rdb_protocol/serialize_datum_onto_blob.hpp"
#include "rdb_protocol/shards.hpp"

#include "debug.hpp"

rdb_value_sizer_t::rdb_value_sizer_t(max_block_size_t bs) : block_size_(bs) { }

const rdb_value_t *rdb_value_sizer_t::as_rdb(const void *p) {
    return reinterpret_cast<const rdb_value_t *>(p);
}

int rdb_value_sizer_t::size(const void *value) const {
    return as_rdb(value)->inline_size(block_size_);
}

bool rdb_value_sizer_t::fits(const void *value, int length_available) const {
    return btree_value_fits(block_size_, length_available, as_rdb(value));
}

int rdb_value_sizer_t::max_possible_size() const {
    return blob::btree_maxreflen;
}

block_magic_t rdb_value_sizer_t::leaf_magic() {
    block_magic_t magic = { { 'r', 'd', 'b', 'l' } };
    return magic;
}

block_magic_t rdb_value_sizer_t::btree_leaf_magic() const {
    return leaf_magic();
}

max_block_size_t rdb_value_sizer_t::block_size() const { return block_size_; }

bool btree_value_fits(max_block_size_t bs, int data_length, const rdb_value_t *value) {
    return blob::ref_fits(bs, data_length, value->value_ref(), blob::btree_maxreflen);
}

// Remember that secondary indexes and the main btree both point to the same rdb
// value -- you don't want to double-delete that value!
void actually_delete_rdb_value(buf_parent_t parent, void *value) {
    blob_t blob(parent.cache()->max_block_size(),
                static_cast<rdb_value_t *>(value)->value_ref(),
                blob::btree_maxreflen);
    blob.clear(parent);
}

void detach_rdb_value(buf_parent_t parent, const void *value) {
    // This const_cast is ok, since `detach_subtrees` is one of the operations
    // that does not actually change value.
    void *non_const_value = const_cast<void *>(value);
    blob_t blob(parent.cache()->max_block_size(),
                static_cast<rdb_value_t *>(non_const_value)->value_ref(),
                blob::btree_maxreflen);
    blob.detach_subtrees(parent);
}

void rdb_get(const store_key_t &store_key, btree_slice_t *slice,
             superblock_t *superblock, point_read_response_t *response,
             profile::trace_t *trace) {
    keyvalue_location_t kv_location;
    rdb_value_sizer_t sizer(superblock->cache()->max_block_size());
    find_keyvalue_location_for_read(&sizer, superblock,
                                    store_key.btree_key(), &kv_location,
                                    &slice->stats, trace);

    if (!kv_location.value.has()) {
        response->data = ql::datum_t::null();
    } else {
        response->data = get_data(static_cast<rdb_value_t *>(kv_location.value.get()),
                                  buf_parent_t(&kv_location.buf));
    }
}

void kv_location_delete(keyvalue_location_t *kv_location,
                        const store_key_t &key,
                        repli_timestamp_t timestamp,
                        const deletion_context_t *deletion_context,
                        rdb_modification_info_t *mod_info_out) {
    // Notice this also implies that buf is valid.
    guarantee(kv_location->value.has());

    // As noted above, we can be sure that buf is valid.
    const max_block_size_t block_size = kv_location->buf.cache()->max_block_size();

    if (mod_info_out != NULL) {
        guarantee(mod_info_out->deleted.second.empty());

        mod_info_out->deleted.second.assign(
                kv_location->value_as<rdb_value_t>()->value_ref(),
                kv_location->value_as<rdb_value_t>()->value_ref()
                + kv_location->value_as<rdb_value_t>()->inline_size(block_size));
    }

    // Detach/Delete
    deletion_context->in_tree_deleter()->delete_value(buf_parent_t(&kv_location->buf),
                                                      kv_location->value.get());

    kv_location->value.reset();
    rdb_value_sizer_t sizer(block_size);
    null_key_modification_callback_t null_cb;
    apply_keyvalue_change(&sizer, kv_location, key.btree_key(), timestamp,
            deletion_context->balancing_detacher(), &null_cb);
}

MUST_USE ql::serialization_result_t
kv_location_set(keyvalue_location_t *kv_location,
                const store_key_t &key,
                ql::datum_t data,
                repli_timestamp_t timestamp,
                const deletion_context_t *deletion_context,
                rdb_modification_info_t *mod_info_out) {
    scoped_malloc_t<rdb_value_t> new_value(blob::btree_maxreflen);
    memset(new_value.get(), 0, blob::btree_maxreflen);

    const max_block_size_t block_size = kv_location->buf.cache()->max_block_size();
    {
        blob_t blob(block_size, new_value->value_ref(), blob::btree_maxreflen);
        ql::serialization_result_t res
            = datum_serialize_onto_blob(buf_parent_t(&kv_location->buf),
                                        &blob, data);
        if (bad(res)) return res;
    }

    if (mod_info_out) {
        guarantee(mod_info_out->added.second.empty());
        mod_info_out->added.second.assign(new_value->value_ref(),
            new_value->value_ref() + new_value->inline_size(block_size));
    }

    if (kv_location->value.has()) {
        deletion_context->in_tree_deleter()->delete_value(
                buf_parent_t(&kv_location->buf), kv_location->value.get());
        if (mod_info_out != NULL) {
            guarantee(mod_info_out->deleted.second.empty());
            mod_info_out->deleted.second.assign(
                    kv_location->value_as<rdb_value_t>()->value_ref(),
                    kv_location->value_as<rdb_value_t>()->value_ref()
                    + kv_location->value_as<rdb_value_t>()->inline_size(block_size));
        }
    }

    // Actually update the leaf, if needed.
    kv_location->value = std::move(new_value);
    null_key_modification_callback_t null_cb;
    rdb_value_sizer_t sizer(block_size);
    apply_keyvalue_change(&sizer, kv_location, key.btree_key(),
                          timestamp,
                          deletion_context->balancing_detacher(), &null_cb);
    return ql::serialization_result_t::SUCCESS;
}

MUST_USE ql::serialization_result_t
kv_location_set(keyvalue_location_t *kv_location,
                const store_key_t &key,
                const std::vector<char> &value_ref,
                repli_timestamp_t timestamp,
                const deletion_context_t *deletion_context) {
    // Detach/Delete the old value.
    if (kv_location->value.has()) {
        deletion_context->in_tree_deleter()->delete_value(
                buf_parent_t(&kv_location->buf), kv_location->value.get());
    }

    scoped_malloc_t<rdb_value_t> new_value(
            value_ref.data(), value_ref.data() + value_ref.size());

    // Update the leaf, if needed.
    kv_location->value = std::move(new_value);

    null_key_modification_callback_t null_cb;
    rdb_value_sizer_t sizer(kv_location->buf.cache()->max_block_size());
    apply_keyvalue_change(&sizer, kv_location, key.btree_key(), timestamp,
                          deletion_context->balancing_detacher(), &null_cb);
    return ql::serialization_result_t::SUCCESS;
}

MUST_USE ql::datum_t
make_replacement_pair(ql::datum_t old_val, ql::datum_t new_val) {
    // in this context, we know the array will have one element.
    // stats_merge later can impose user preferences.
    ql::datum_array_builder_t values(ql::configured_limits_t::unlimited);
    ql::datum_object_builder_t value_pair;
    bool conflict = value_pair.add("old_val", old_val)
        || value_pair.add("new_val", new_val);
    guarantee(!conflict);
    values.add(std::move(value_pair).to_datum());
    return std::move(values).to_datum();
}

batched_replace_response_t rdb_replace_and_return_superblock(
    const btree_loc_info_t &info,
    const btree_point_replacer_t *replacer,
    const deletion_context_t *deletion_context,
    promise_t<superblock_t *> *superblock_promise,
    rdb_modification_info_t *mod_info_out,
    profile::trace_t *trace)
{
    const return_changes_t return_changes = replacer->should_return_changes();
    const datum_string_t &primary_key = info.btree->primary_key;
    const store_key_t &key = *info.key;
    ql::datum_object_builder_t resp;
    try {
        keyvalue_location_t kv_location;
        rdb_value_sizer_t sizer(info.superblock->cache()->max_block_size());
        find_keyvalue_location_for_write(&sizer, info.superblock,
                                         info.key->btree_key(),
                                         deletion_context->balancing_detacher(),
                                         &kv_location,
                                         &info.btree->slice->stats,
                                         trace,
                                         superblock_promise);

        bool started_empty, ended_empty;
        ql::datum_t old_val;
        if (!kv_location.value.has()) {
            // If there's no entry with this key, pass NULL to the function.
            started_empty = true;
            old_val = ql::datum_t::null();
        } else {
            // Otherwise pass the entry with this key to the function.
            started_empty = false;
            old_val = get_data(kv_location.value_as<rdb_value_t>(),
                               buf_parent_t(&kv_location.buf));
            guarantee(old_val.get_field(primary_key, ql::NOTHROW).has());
        }
        guarantee(old_val.has());
        if (return_changes == return_changes_t::YES) {
            // first, fill with the old value.  Then, if `replacer` succeeds, fill with new value.
            bool conflict = resp.add("changes", make_replacement_pair(old_val, old_val));
            guarantee(!conflict);
        }

        ql::datum_t new_val = replacer->replace(old_val);
        if (return_changes == return_changes_t::YES) {
            resp.overwrite("changes", make_replacement_pair(old_val, new_val));
        }
        if (new_val.get_type() == ql::datum_t::R_NULL) {
            ended_empty = true;
        } else if (new_val.get_type() == ql::datum_t::R_OBJECT) {
            ended_empty = false;
            new_val.rcheck_valid_replace(
                old_val, ql::datum_t(), primary_key);
            ql::datum_t pk = new_val.get_field(primary_key, ql::NOTHROW);
            rcheck_target(
                &new_val, ql::base_exc_t::GENERIC,
                key.compare(store_key_t(pk.print_primary())) == 0,
                (started_empty
                 ? strprintf("Primary key `%s` cannot be changed (null -> %s)",
                             primary_key.to_std().c_str(), new_val.print().c_str())
                 : strprintf("Primary key `%s` cannot be changed (%s -> %s)",
                             primary_key.to_std().c_str(),
                             old_val.print().c_str(), new_val.print().c_str())));
        } else {
            rfail_typed_target(
                &new_val, "Inserted value must be an OBJECT (got %s):\n%s",
                new_val.get_type_name().c_str(), new_val.print().c_str());
        }

        // We use `conflict` below to store whether or not there was a key
        // conflict when constructing the stats object.  It defaults to `true`
        // so that we fail an assertion if we never update the stats object.
        bool conflict = true;

        // Figure out what operation we're doing (based on started_empty,
        // ended_empty, and the result of the function call) and then do it.
        if (started_empty) {
            if (ended_empty) {
                conflict = resp.add("skipped", ql::datum_t(1.0));
            } else {
                conflict = resp.add("inserted", ql::datum_t(1.0));
                r_sanity_check(new_val.get_field(primary_key, ql::NOTHROW).has());
                ql::serialization_result_t res =
                    kv_location_set(&kv_location, *info.key, new_val,
                                    info.btree->timestamp, deletion_context,
                                    mod_info_out);
                switch (res) {
                case ql::serialization_result_t::ARRAY_TOO_BIG:
                    rfail_typed_target(&new_val, "Array too large for disk writes"
                                       " (limit 100,000 elements)");
                    unreachable();
                case ql::serialization_result_t::SUCCESS:
                    break;
                default:
                    unreachable();
                }
                guarantee(mod_info_out->deleted.second.empty());
                guarantee(!mod_info_out->added.second.empty());
                mod_info_out->added.first = new_val;
            }
        } else {
            if (ended_empty) {
                conflict = resp.add("deleted", ql::datum_t(1.0));
                kv_location_delete(&kv_location, *info.key, info.btree->timestamp,
                                   deletion_context, mod_info_out);
                guarantee(!mod_info_out->deleted.second.empty());
                guarantee(mod_info_out->added.second.empty());
                mod_info_out->deleted.first = old_val;
            } else {
                r_sanity_check(
                    old_val.get_field(primary_key) == new_val.get_field(primary_key));
                if (old_val == new_val) {
                    conflict = resp.add("unchanged",
                                         ql::datum_t(1.0));
                } else {
                    conflict = resp.add("replaced", ql::datum_t(1.0));
                    r_sanity_check(new_val.get_field(primary_key, ql::NOTHROW).has());
                    ql::serialization_result_t res =
                        kv_location_set(&kv_location, *info.key, new_val,
                                        info.btree->timestamp, deletion_context,
                                        mod_info_out);
                    switch (res) {
                    case ql::serialization_result_t::ARRAY_TOO_BIG:
                        rfail_typed_target(&new_val, "Array too large for disk writes"
                                           " (limit 100,000 elements)");
                        unreachable();
                    case ql::serialization_result_t::SUCCESS:
                        break;
                    default:
                        unreachable();
                    }
                    guarantee(!mod_info_out->deleted.second.empty());
                    guarantee(!mod_info_out->added.second.empty());
                    mod_info_out->added.first = new_val;
                    mod_info_out->deleted.first = old_val;
                }
            }
        }
        guarantee(!conflict); // message never added twice
    } catch (const ql::base_exc_t &e) {
        resp.add_error(e.what());
    } catch (const interrupted_exc_t &e) {
        std::string msg = strprintf("interrupted (%s:%d)", __FILE__, __LINE__);
        resp.add_error(msg.c_str());
        // We don't rethrow because we're in a coroutine.  Theoretically the
        // above message should never make it back to a user because the calling
        // function will also be interrupted, but we document where it comes
        // from to aid in future debugging if that invariant becomes violated.
    }
    return std::move(resp).to_datum();
}


class one_replace_t : public btree_point_replacer_t {
public:
    one_replace_t(const btree_batched_replacer_t *_replacer, size_t _index)
        : replacer(_replacer), index(_index) { }

    ql::datum_t replace(const ql::datum_t &d) const {
        return replacer->replace(d, index);
    }
    return_changes_t should_return_changes() const { return replacer->should_return_changes(); }
private:
    const btree_batched_replacer_t *const replacer;
    const size_t index;
};

void do_a_replace_from_batched_replace(
    auto_drainer_t::lock_t,
    fifo_enforcer_sink_t *batched_replaces_fifo_sink,
    const fifo_enforcer_write_token_t &batched_replaces_fifo_token,
    const btree_loc_info_t &info,
    const one_replace_t one_replace,
    const ql::configured_limits_t &limits,
    promise_t<superblock_t *> *superblock_promise,
    rdb_modification_report_cb_t *sindex_cb,
    batched_replace_response_t *stats_out,
    profile::trace_t *trace,
    std::set<std::string> *conditions)
{
    fifo_enforcer_sink_t::exit_write_t exiter(
        batched_replaces_fifo_sink, batched_replaces_fifo_token);

    rdb_live_deletion_context_t deletion_context;
    rdb_modification_report_t mod_report(*info.key);
    ql::datum_t res = rdb_replace_and_return_superblock(
        info, &one_replace, &deletion_context, superblock_promise, &mod_report.info,
        trace);
    *stats_out = (*stats_out).merge(res, ql::stats_merge, limits, conditions);

    // KSI: What is this for?  are we waiting to get in line to call on_mod_report?
    // I guess so.

    // JD: Looks like this is a do_a_replace_from_batched_replace specific thing.
    exiter.wait();
    sindex_cb->on_mod_report(mod_report);
}

batched_replace_response_t rdb_batched_replace(
    const btree_info_t &info,
    scoped_ptr_t<superblock_t> *superblock,
    const std::vector<store_key_t> &keys,
    const ql::configured_limits_t &limits,
    const btree_batched_replacer_t *replacer,
    rdb_modification_report_cb_t *sindex_cb,
    profile::trace_t *trace) {

    fifo_enforcer_source_t batched_replaces_fifo_source;
    fifo_enforcer_sink_t batched_replaces_fifo_sink;

    ql::datum_t stats = ql::datum_t::empty_object();

    std::set<std::string> conditions;

    // We have to drain write operations before destructing everything above us,
    // because the coroutines being drained use them.
    {
        unlimited_fifo_queue_t<std::function<void()> > coro_queue;
        struct callback_t : public coro_pool_callback_t<std::function<void()> > {
            virtual void coro_pool_callback(std::function<void()> f, signal_t *) {
                f();
            }
        } callback;
        const size_t MAX_CONCURRENT_REPLACES = 8;
        coro_pool_t<std::function<void()> > coro_pool(
            MAX_CONCURRENT_REPLACES, &coro_queue, &callback);
        auto_drainer_t drainer;
        // Note the destructor ordering: We release the superblock before draining
        // on all the write operations.
        scoped_ptr_t<superblock_t> current_superblock(superblock->release());
        for (size_t i = 0; i < keys.size(); ++i) {
            // Pass out the point_replace_response_t.
            promise_t<superblock_t *> superblock_promise;
            coro_queue.push(
                std::bind(
                    &do_a_replace_from_batched_replace,
                    auto_drainer_t::lock_t(&drainer),
                    &batched_replaces_fifo_sink,
                    batched_replaces_fifo_source.enter_write(),

                    btree_loc_info_t(&info, current_superblock.release(), &keys[i]),
                    one_replace_t(replacer, i),
                    limits,

                    &superblock_promise,
                    sindex_cb,
                    &stats,
                    trace,
                    &conditions));
            current_superblock.init(superblock_promise.wait());
        }
    } // Make sure the drainer is destructed before the return statement.

    ql::datum_object_builder_t out(stats);
    out.add_warnings(conditions, limits);
    return std::move(out).to_datum();
}

void rdb_set(const store_key_t &key,
             ql::datum_t data,
             bool overwrite,
             btree_slice_t *slice,
             repli_timestamp_t timestamp,
             superblock_t *superblock,
             const deletion_context_t *deletion_context,
             point_write_response_t *response_out,
             rdb_modification_info_t *mod_info,
             profile::trace_t *trace,
             promise_t<superblock_t *> *pass_back_superblock) {
    keyvalue_location_t kv_location;
    rdb_value_sizer_t sizer(superblock->cache()->max_block_size());
    find_keyvalue_location_for_write(&sizer, superblock, key.btree_key(),
                                     deletion_context->balancing_detacher(),
                                     &kv_location, &slice->stats, trace,
                                     pass_back_superblock);
    const bool had_value = kv_location.value.has();

    /* update the modification report */
    if (kv_location.value.has()) {
        mod_info->deleted.first = get_data(kv_location.value_as<rdb_value_t>(),
                                           buf_parent_t(&kv_location.buf));
    }

    mod_info->added.first = data;

    if (overwrite || !had_value) {
        ql::serialization_result_t res =
            kv_location_set(&kv_location, key, data, timestamp, deletion_context,
                            mod_info);
        switch (res) {
        case ql::serialization_result_t::ARRAY_TOO_BIG:
            rfail_typed_target(&data, "Array too large for disk writes"
                               " (limit 100,000 elements)");
            unreachable();
        case ql::serialization_result_t::SUCCESS:
            break;
        default:
            unreachable();
        }
        guarantee(mod_info->deleted.second.empty() == !had_value &&
                  !mod_info->added.second.empty());
    }
    response_out->result =
        (had_value ? point_write_result_t::DUPLICATE : point_write_result_t::STORED);
}

class agnostic_rdb_backfill_callback_t : public agnostic_backfill_callback_t {
public:
    agnostic_rdb_backfill_callback_t(rdb_backfill_callback_t *cb,
                                     const key_range_t &kr,
                                     btree_slice_t *slice) :
        cb_(cb), kr_(kr), slice_(slice) { }

    void on_delete_range(const key_range_t &range, signal_t *interruptor) THROWS_ONLY(interrupted_exc_t) {
        rassert(kr_.is_superset(range));
        cb_->on_delete_range(range, interruptor);
    }

    void on_deletion(const btree_key_t *key, repli_timestamp_t recency, signal_t *interruptor) THROWS_ONLY(interrupted_exc_t) {
        rassert(kr_.contains_key(key->contents, key->size));
        cb_->on_deletion(key, recency, interruptor);
    }

    void on_pairs(buf_parent_t leaf_node,
                  const std::vector<repli_timestamp_t> &recencies,
                  const std::vector<const btree_key_t *> &keys,
                  const std::vector<const void *> &vals,
                  signal_t *interruptor) THROWS_ONLY(interrupted_exc_t) {

        std::vector<backfill_atom_t> chunk_atoms;
        chunk_atoms.reserve(keys.size());
        size_t current_chunk_size = 0;

        for (size_t i = 0; i < keys.size(); ++i) {
            rassert(kr_.contains_key(keys[i]->contents, keys[i]->size));
            const rdb_value_t *value = static_cast<const rdb_value_t *>(vals[i]);

            backfill_atom_t atom;
            atom.key.assign(keys[i]->size, keys[i]->contents);
            atom.value = get_data(value, leaf_node);
            atom.recency = recencies[i];
            chunk_atoms.push_back(atom);
            current_chunk_size += static_cast<size_t>(atom.key.size())
                + serialized_size<cluster_version_t::CLUSTER>(atom.value);

            if (current_chunk_size >= BACKFILL_MAX_KVPAIRS_SIZE) {
                // To avoid flooding the receiving node with overly large chunks
                // (which could easily make it run out of memory in extreme
                // cases), pass on what we have got so far. Then continue
                // with the remaining values.
                slice_->stats.pm_keys_read.record(chunk_atoms.size());
                slice_->stats.pm_total_keys_read += chunk_atoms.size();
                cb_->on_keyvalues(std::move(chunk_atoms), interruptor);
                chunk_atoms = std::vector<backfill_atom_t>();
                chunk_atoms.reserve(keys.size() - (i+1));
                current_chunk_size = 0;
            }
        }
        if (!chunk_atoms.empty()) {
            // Pass on the final chunk
            slice_->stats.pm_keys_read.record(chunk_atoms.size());
            slice_->stats.pm_total_keys_read += chunk_atoms.size();
            cb_->on_keyvalues(std::move(chunk_atoms), interruptor);
        }
    }

    void on_sindexes(const std::map<std::string, secondary_index_t> &sindexes, signal_t *interruptor) THROWS_ONLY(interrupted_exc_t) {
        cb_->on_sindexes(sindexes, interruptor);
    }

    rdb_backfill_callback_t *cb_;
    key_range_t kr_;
    btree_slice_t *slice_;
};

void rdb_backfill(btree_slice_t *slice, const key_range_t& key_range,
                  repli_timestamp_t since_when, rdb_backfill_callback_t *callback,
                  superblock_t *superblock,
                  buf_lock_t *sindex_block,
                  parallel_traversal_progress_t *p, signal_t *interruptor)
    THROWS_ONLY(interrupted_exc_t) {
    agnostic_rdb_backfill_callback_t agnostic_cb(callback, key_range, slice);
    rdb_value_sizer_t sizer(superblock->cache()->max_block_size());
    do_agnostic_btree_backfill(&sizer, key_range, since_when, &agnostic_cb,
                               superblock, sindex_block, p, interruptor);
}

void rdb_delete(const store_key_t &key, btree_slice_t *slice,
                repli_timestamp_t timestamp,
                superblock_t *superblock,
                const deletion_context_t *deletion_context,
                point_delete_response_t *response,
                rdb_modification_info_t *mod_info,
                profile::trace_t *trace) {
    keyvalue_location_t kv_location;
    rdb_value_sizer_t sizer(superblock->cache()->max_block_size());
    find_keyvalue_location_for_write(&sizer, superblock, key.btree_key(),
            deletion_context->balancing_detacher(), &kv_location, &slice->stats, trace);
    bool exists = kv_location.value.has();

    /* Update the modification report. */
    if (exists) {
        mod_info->deleted.first = get_data(kv_location.value_as<rdb_value_t>(),
                                           buf_parent_t(&kv_location.buf));
        kv_location_delete(&kv_location, key, timestamp, deletion_context, mod_info);
        guarantee(!mod_info->deleted.second.empty() && mod_info->added.second.empty());
    }
    response->result = (exists ? point_delete_result_t::DELETED : point_delete_result_t::MISSING);
}

void rdb_value_deleter_t::delete_value(buf_parent_t parent, const void *value) const {
    // To not destroy constness, we operate on a copy of the value
    rdb_value_sizer_t sizer(parent.cache()->max_block_size());
    scoped_malloc_t<rdb_value_t> value_copy(sizer.max_possible_size());
    memcpy(value_copy.get(), value, sizer.size(value));
    actually_delete_rdb_value(parent, value_copy.get());
}

void rdb_value_detacher_t::delete_value(buf_parent_t parent, const void *value) const {
    detach_rdb_value(parent, value);
}

class sindex_key_range_tester_t : public key_tester_t {
public:
    explicit sindex_key_range_tester_t(const key_range_t &key_range)
        : key_range_(key_range) { }

    bool key_should_be_erased(const btree_key_t *key) {
        std::string pk = ql::datum_t::extract_primary(
            key_to_unescaped_str(store_key_t(key)));

        return key_range_.contains_key(store_key_t(pk));
    }
private:
    key_range_t key_range_;
    DISABLE_COPYING(sindex_key_range_tester_t);
};

void sindex_erase_range(
        const key_range_t &key_range, superblock_t *superblock, auto_drainer_t::lock_t,
        signal_t *interruptor, release_superblock_t release_superblock,
        const value_deleter_t *deleter) THROWS_NOTHING {

    rdb_value_sizer_t sizer(superblock->cache()->max_block_size());

    sindex_key_range_tester_t tester(key_range);

    try {
        btree_erase_range_generic(&sizer, &tester,
                                  deleter, NULL, NULL,
                                  superblock, interruptor,
                                  release_superblock);
    } catch (const interrupted_exc_t &) {
        // We were interrupted. That's fine nothing to be done about it.
    }
}

/* Spawns a coro to carry out the erase range for each sindex. */
void spawn_sindex_erase_ranges(
        const store_t::sindex_access_vector_t *sindex_access,
        const key_range_t &key_range,
        auto_drainer_t *drainer,
        auto_drainer_t::lock_t,
        release_superblock_t release_superblock,
        signal_t *interruptor,
        const value_deleter_t *deleter) {
    for (auto it = sindex_access->begin(); it != sindex_access->end(); ++it) {
        coro_t::spawn_sometime(std::bind(
                    &sindex_erase_range,
                    key_range, (*it)->super_block.get(),
                    auto_drainer_t::lock_t(drainer), interruptor,
                    release_superblock, deleter));
    }
}

/* Helper function for rdb_erase_*_range() */
void rdb_erase_range_convert_keys(const key_range_t &key_range,
                                  bool *left_key_supplied_out,
                                  bool *right_key_supplied_out,
                                  store_key_t *left_key_exclusive_out,
                                  store_key_t *right_key_inclusive_out) {
    /* This is guaranteed because the way the keys are calculated below would
     * lead to a single key being deleted even if the range was empty. */
    guarantee(!key_range.is_empty());

    rassert(left_key_supplied_out != NULL);
    rassert(right_key_supplied_out != NULL);
    rassert(left_key_exclusive_out != NULL);
    rassert(right_key_inclusive_out != NULL);

    /* Twiddle some keys to get the in the form we want. Notice these are keys
     * which will be made  exclusive and inclusive as their names suggest
     * below. At the point of construction they aren't. */
    *left_key_exclusive_out = store_key_t(key_range.left);
    *right_key_inclusive_out = store_key_t(key_range.right.key);

    *left_key_supplied_out = left_key_exclusive_out->decrement();
    *right_key_supplied_out = !key_range.right.unbounded;
    if (*right_key_supplied_out) {
        right_key_inclusive_out->decrement();
    }

    /* Now left_key_exclusive and right_key_inclusive accurately reflect their
     * names. */
}

void rdb_erase_small_range(key_tester_t *tester,
                           const key_range_t &key_range,
                           superblock_t *superblock,
                           const deletion_context_t *deletion_context,
                           signal_t *interruptor,
                           std::vector<rdb_modification_report_t> *mod_reports_out) {
    rassert(mod_reports_out != NULL);
    mod_reports_out->clear();

    bool left_key_supplied, right_key_supplied;
    store_key_t left_key_exclusive, right_key_inclusive;
    rdb_erase_range_convert_keys(key_range, &left_key_supplied, &right_key_supplied,
             &left_key_exclusive, &right_key_inclusive);

    /* We need these structures to perform the erase range. */
    rdb_value_sizer_t sizer(superblock->cache()->max_block_size());

    struct on_erase_cb_t {
        static void on_erase(
                const store_key_t &key,
                const char *data,
                const buf_parent_t &parent,
                std::vector<rdb_modification_report_t> *_mod_reports_out) {
            const rdb_value_t *value = reinterpret_cast<const rdb_value_t *>(data);

            // The mod_report we generate is a simple delete. While there is generally
            // a difference between an erase and a delete (deletes get backfilled,
            // while an erase is as if the value had never existed), that
            // difference is irrelevant in the case of secondary indexes.
            rdb_modification_report_t mod_report;
            mod_report.primary_key = key;
            // Get the full data
            mod_report.info.deleted.first = get_data(value, parent);
            // Get the inline value
            max_block_size_t block_size = parent.cache()->max_block_size();
            mod_report.info.deleted.second.assign(value->value_ref(),
                value->value_ref() + value->inline_size(block_size));

            _mod_reports_out->push_back(mod_report);
        }
    };

    btree_erase_range_generic(&sizer, tester, deletion_context->in_tree_deleter(),
        left_key_supplied ? left_key_exclusive.btree_key() : NULL,
        right_key_supplied ? right_key_inclusive.btree_key() : NULL,
        superblock, interruptor, release_superblock_t::RELEASE,
        std::bind(&on_erase_cb_t::on_erase,
                  ph::_1, ph::_2, ph::_3, mod_reports_out));
}


typedef ql::transform_variant_t transform_variant_t;
typedef ql::terminal_variant_t terminal_variant_t;

class rget_sindex_data_t {
public:
    rget_sindex_data_t(const key_range_t &_pkey_range, const datum_range_t &_range,
                       reql_version_t wire_func_reql_version,
                       ql::map_wire_func_t wire_func, sindex_multi_bool_t _multi)
        : pkey_range(_pkey_range), range(_range),
          func_reql_version(wire_func_reql_version),
          func(wire_func.compile_wire_func()), multi(_multi) { }
private:
    friend class rget_cb_t;
    const key_range_t pkey_range;
    const datum_range_t range;
    const reql_version_t func_reql_version;
    const counted_t<const ql::func_t> func;
    const sindex_multi_bool_t multi;
};

class job_data_t {
public:
    job_data_t(ql::env_t *_env, const ql::batchspec_t &batchspec,
               const std::vector<transform_variant_t> &_transforms,
               const boost::optional<terminal_variant_t> &_terminal,
               sorting_t _sorting)
        : env(_env),
          batcher(batchspec.to_batcher()),
          sorting(_sorting),
          accumulator(_terminal
                      ? ql::make_terminal(*_terminal)
                      : ql::make_append(sorting, &batcher)) {
        for (size_t i = 0; i < _transforms.size(); ++i) {
            transformers.push_back(ql::make_op(_transforms[i]));
        }
        guarantee(transformers.size() == _transforms.size());
    }
    job_data_t(job_data_t &&jd)
        : env(jd.env),
          batcher(std::move(jd.batcher)),
          transformers(std::move(jd.transformers)),
          sorting(jd.sorting),
          accumulator(jd.accumulator.release()) {
    }
private:
    friend class rget_cb_t;
    ql::env_t *const env;
    ql::batcher_t batcher;
    std::vector<scoped_ptr_t<ql::op_t> > transformers;
    sorting_t sorting;
    scoped_ptr_t<ql::accumulator_t> accumulator;
};

class rget_io_data_t {
public:
    rget_io_data_t(rget_read_response_t *_response, btree_slice_t *_slice)
        : response(_response), slice(_slice) { }
private:
    friend class rget_cb_t;
    rget_read_response_t *const response;
    btree_slice_t *const slice;
};

class rget_cb_t : public concurrent_traversal_callback_t {
public:
    rget_cb_t(rget_io_data_t &&_io,
              job_data_t &&_job,
              boost::optional<rget_sindex_data_t> &&_sindex,
              const key_range_t &range);

    virtual done_traversing_t handle_pair(scoped_key_value_t &&keyvalue,
                               concurrent_traversal_fifo_enforcer_signal_t waiter)
    THROWS_ONLY(interrupted_exc_t);
    void finish() THROWS_ONLY(interrupted_exc_t);
private:
    const rget_io_data_t io; // How do get data in/out.
    job_data_t job; // What to do next (stateful).
    const boost::optional<rget_sindex_data_t> sindex; // Optional sindex information.

    // State for internal bookkeeping.
    bool bad_init;
    scoped_ptr_t<profile::disabler_t> disabler;
    scoped_ptr_t<profile::sampler_t> sampler;
};

rget_cb_t::rget_cb_t(rget_io_data_t &&_io,
                     job_data_t &&_job,
                     boost::optional<rget_sindex_data_t> &&_sindex,
                     const key_range_t &range)
    : io(std::move(_io)),
      job(std::move(_job)),
      sindex(std::move(_sindex)),
      bad_init(false) {
    io.response->last_key = !reversed(job.sorting)
        ? range.left
        : (!range.right.unbounded ? range.right.key : store_key_t::max());
    disabler.init(new profile::disabler_t(job.env->trace));
    sampler.init(new profile::sampler_t("Range traversal doc evaluation.",
                                        job.env->trace));
}

void rget_cb_t::finish() THROWS_ONLY(interrupted_exc_t) {
    job.accumulator->finish(&io.response->result);
    if (job.accumulator->should_send_batch()) {
        io.response->truncated = true;
    }
}

// Handle a keyvalue pair.  Returns whether or not we're done early.
done_traversing_t rget_cb_t::handle_pair(scoped_key_value_t &&keyvalue,
                              concurrent_traversal_fifo_enforcer_signal_t waiter)
THROWS_ONLY(interrupted_exc_t) {
    sampler->new_sample();

    if (bad_init || boost::get<ql::exc_t>(&io.response->result) != NULL) {
        return done_traversing_t::YES;
    }

    // Load the key and value.
    store_key_t key(keyvalue.key());
    if (sindex && !sindex->pkey_range.contains_key(ql::datum_t::extract_primary(key))) {
        return done_traversing_t::NO;
    }

    lazy_json_t row(static_cast<const rdb_value_t *>(keyvalue.value()),
                    keyvalue.expose_buf());
    ql::datum_t val;
    // We only load the value if we actually use it (`count` does not).
    if (job.accumulator->uses_val() || job.transformers.size() != 0 || sindex) {
        val = row.get();
        io.slice->stats.pm_keys_read.record();
        io.slice->stats.pm_total_keys_read += 1;
    } else {
        row.reset();
    }
    guarantee(!row.references_parent());
    keyvalue.reset();
    waiter.wait_interruptible();

    try {
        // Update the last considered key.
        if ((io.response->last_key < key && !reversed(job.sorting)) ||
            (io.response->last_key > key && reversed(job.sorting))) {
            io.response->last_key = key;
        }

        // Check whether we're out of sindex range.
        ql::datum_t sindex_val; // NULL if no sindex.
        if (sindex) {
            // Secondary index functions are deterministic (so no need for an
            // rdb_context_t) and evaluated in a pristine environment (without global
            // optargs).
            ql::env_t sindex_env(job.env->interruptor, sindex->func_reql_version);
            sindex_val = sindex->func->call(&sindex_env, val)->as_datum();
            if (sindex->multi == sindex_multi_bool_t::MULTI
                && sindex_val.get_type() == ql::datum_t::R_ARRAY) {
                boost::optional<uint64_t> tag = *ql::datum_t::extract_tag(key);
                guarantee(tag);
                sindex_val = sindex_val.get(*tag, ql::NOTHROW);
                guarantee(sindex_val.has());
            }
            if (!sindex->range.contains(sindex->func_reql_version, sindex_val)) {
                return done_traversing_t::NO;
            }
        }

        ql::groups_t data(optional_datum_less_t(job.env->reql_version()));
        data = {{ql::datum_t(), ql::datums_t{val}}};

        for (auto it = job.transformers.begin(); it != job.transformers.end(); ++it) {
            (**it)(job.env, &data, sindex_val);
            //                     ^^^^^^^^^^ NULL if no sindex
        }
        // We need lots of extra data for the accumulation because we might be
        // accumulating `rget_item_t`s for a batch.
        return (*job.accumulator)(job.env,
                                  &data,
                                  std::move(key),
                                  std::move(sindex_val)); // NULL if no sindex
    } catch (const ql::exc_t &e) {
        io.response->result = e;
        return done_traversing_t::YES;
    } catch (const ql::datum_exc_t &e) {
#ifndef NDEBUG
        unreachable();
#else
        io.response->result = ql::exc_t(e, NULL);
        return done_traversing_t::YES;
#endif // NDEBUG
    }
}

// TODO: Having two functions which are 99% the same sucks.
void rdb_rget_slice(
        btree_slice_t *slice,
        const key_range_t &range,
        superblock_t *superblock,
        ql::env_t *ql_env,
        const ql::batchspec_t &batchspec,
        const std::vector<transform_variant_t> &transforms,
        const boost::optional<terminal_variant_t> &terminal,
        sorting_t sorting,
        rget_read_response_t *response) {

    r_sanity_check(boost::get<ql::exc_t>(&response->result) == NULL);
    profile::starter_t starter("Do range scan on primary index.", ql_env->trace);
    rget_cb_t callback(
        rget_io_data_t(response, slice),
        job_data_t(ql_env, batchspec, transforms, terminal, sorting),
        boost::optional<rget_sindex_data_t>(),
        range);
    btree_concurrent_traversal(superblock, range, &callback,
                               (!reversed(sorting) ? FORWARD : BACKWARD));
    callback.finish();
}

void rdb_rget_secondary_slice(
        btree_slice_t *slice,
        const datum_range_t &sindex_range,
        const region_t &sindex_region,
        superblock_t *superblock,
        ql::env_t *ql_env,
        const ql::batchspec_t &batchspec,
        const std::vector<transform_variant_t> &transforms,
        const boost::optional<terminal_variant_t> &terminal,
        const key_range_t &pk_range,
        sorting_t sorting,
        const sindex_disk_info_t &sindex_info,
        rget_read_response_t *response) {

    r_sanity_check(boost::get<ql::exc_t>(&response->result) == NULL);
    guarantee(sindex_info.geo == sindex_geo_bool_t::REGULAR);
    profile::starter_t starter("Do range scan on secondary index.", ql_env->trace);

    const reql_version_t sindex_func_reql_version =
        sindex_info.mapping_version_info.latest_compatible_reql_version;
    rget_cb_t callback(
        rget_io_data_t(response, slice),
        job_data_t(ql_env, batchspec, transforms, terminal, sorting),
        rget_sindex_data_t(pk_range, sindex_range, sindex_func_reql_version,
                           sindex_info.mapping, sindex_info.multi),
        sindex_region.inner);
    btree_concurrent_traversal(
        superblock, sindex_region.inner, &callback,
        (!reversed(sorting) ? FORWARD : BACKWARD));
    callback.finish();
}

void rdb_get_intersecting_slice(
        btree_slice_t *slice,
        const ql::datum_t &query_geometry,
        const region_t &sindex_region,
        superblock_t *superblock,
        ql::env_t *ql_env,
        const ql::batchspec_t &batchspec,
        const std::vector<ql::transform_variant_t> &transforms,
        const boost::optional<ql::terminal_variant_t> &terminal,
        const key_range_t &pk_range,
        const sindex_disk_info_t &sindex_info,
        rget_read_response_t *response) {
    guarantee(query_geometry.has());

    guarantee(sindex_info.geo == sindex_geo_bool_t::GEO);
    profile::starter_t starter("Do intersection scan on geospatial index.", ql_env->trace);

    const reql_version_t sindex_func_reql_version =
        sindex_info.mapping_version_info.latest_compatible_reql_version;
    collect_all_geo_intersecting_cb_t callback(
        slice,
        geo_job_data_t(ql_env, batchspec, transforms, terminal),
        geo_sindex_data_t(pk_range, sindex_info.mapping, sindex_func_reql_version,
                          sindex_info.multi),
        query_geometry,
        sindex_region.inner,
        response);
    btree_concurrent_traversal(
        superblock, sindex_region.inner, &callback,
        direction_t::FORWARD,
        release_superblock_t::RELEASE);
    callback.finish();
}

void rdb_get_nearest_slice(
    btree_slice_t *slice,
    const lon_lat_point_t &center,
    double max_dist,
    uint64_t max_results,
    const ellipsoid_spec_t &geo_system,
    superblock_t *superblock,
    ql::env_t *ql_env,
    const key_range_t &pk_range,
    const sindex_disk_info_t &sindex_info,
    nearest_geo_read_response_t *response) {

    guarantee(sindex_info.geo == sindex_geo_bool_t::GEO);
    profile::starter_t starter("Do nearest traversal on geospatial index.", ql_env->trace);

    const reql_version_t sindex_func_reql_version =
        sindex_info.mapping_version_info.latest_compatible_reql_version;

    // TODO (daniel): Instead of calling this multiple times until we are done,
    //   results should be streamed lazily. Also, even if we don't do that,
    //   the copying of the result we do here is bad.
    nearest_traversal_state_t state(center, max_results, max_dist, geo_system);
    response->results_or_error = nearest_geo_read_response_t::result_t();
    do {
        nearest_geo_read_response_t partial_response;
        try {
            nearest_traversal_cb_t callback(
                slice,
                geo_sindex_data_t(pk_range, sindex_info.mapping,
                                  sindex_func_reql_version, sindex_info.multi),
                ql_env,
                &state);
            btree_concurrent_traversal(
                superblock, key_range_t::universe(), &callback,
                direction_t::FORWARD,
                release_superblock_t::KEEP);
            callback.finish(&partial_response);
        } catch (const geo_exception_t &e) {
            partial_response.results_or_error =
                ql::exc_t(ql::base_exc_t::GENERIC, e.what(), NULL);
        }
        if (boost::get<ql::exc_t>(&partial_response.results_or_error)) {
            response->results_or_error = partial_response.results_or_error;
            return;
        } else {
            auto partial_res = boost::get<nearest_geo_read_response_t::result_t>(
                &partial_response.results_or_error);
            guarantee(partial_res != NULL);
            auto full_res = boost::get<nearest_geo_read_response_t::result_t>(
                &response->results_or_error);
            std::move(partial_res->begin(), partial_res->end(),
                      std::back_inserter(*full_res));
        }
    } while (state.proceed_to_next_batch() == done_traversing_t::NO);
}

void rdb_distribution_get(int max_depth,
                          const store_key_t &left_key,
                          superblock_t *superblock,
                          distribution_read_response_t *response) {
    int64_t key_count_out;
    std::vector<store_key_t> key_splits;
    get_btree_key_distribution(superblock, max_depth,
                               &key_count_out, &key_splits);

    int64_t keys_per_bucket;
    if (key_splits.size() == 0) {
        keys_per_bucket = key_count_out;
    } else  {
        keys_per_bucket = std::max<int64_t>(key_count_out / key_splits.size(), 1);
    }
    response->key_counts[left_key] = keys_per_bucket;

    for (std::vector<store_key_t>::iterator it  = key_splits.begin();
                                            it != key_splits.end();
                                            ++it) {
        response->key_counts[*it] = keys_per_bucket;
    }
}

static const int8_t HAS_VALUE = 0;
static const int8_t HAS_NO_VALUE = 1;

template <cluster_version_t W>
void serialize(write_message_t *wm, const rdb_modification_info_t &info) {
    if (!info.deleted.first.has()) {
        guarantee(info.deleted.second.empty());
        serialize<W>(wm, HAS_NO_VALUE);
    } else {
        serialize<W>(wm, HAS_VALUE);
        serialize<W>(wm, info.deleted);
    }

    if (!info.added.first.has()) {
        guarantee(info.added.second.empty());
        serialize<W>(wm, HAS_NO_VALUE);
    } else {
        serialize<W>(wm, HAS_VALUE);
        serialize<W>(wm, info.added);
    }
}

template <cluster_version_t W>
archive_result_t deserialize(read_stream_t *s, rdb_modification_info_t *info) {
    int8_t has_value;
    archive_result_t res = deserialize<W>(s, &has_value);
    if (bad(res)) { return res; }

    if (has_value == HAS_VALUE) {
        res = deserialize<W>(s, &info->deleted);
        if (bad(res)) { return res; }
    }

    res = deserialize<W>(s, &has_value);
    if (bad(res)) { return res; }

    if (has_value == HAS_VALUE) {
        res = deserialize<W>(s, &info->added);
        if (bad(res)) { return res; }
    }

    return archive_result_t::SUCCESS;
}

INSTANTIATE_SERIALIZABLE_SINCE_v1_13(rdb_modification_info_t);

RDB_IMPL_SERIALIZABLE_2_SINCE_v1_13(rdb_modification_report_t, primary_key, info);

rdb_modification_report_cb_t::rdb_modification_report_cb_t(
        store_t *store,
        buf_lock_t *sindex_block,
        auto_drainer_t::lock_t lock)
    : lock_(lock), store_(store),
      sindex_block_(sindex_block) {
    store_->acquire_post_constructed_sindex_superblocks_for_write(
            sindex_block_, &sindexes_);
}

rdb_modification_report_cb_t::~rdb_modification_report_cb_t() { }

void rdb_modification_report_cb_t::on_mod_report(
    const rdb_modification_report_t &mod_report) {
    // debugf("%" PRIu64 "\n", timestamp.longtime);
    if (mod_report.info.deleted.first.has() || mod_report.info.added.first.has()) {
        // We spawn the sindex update in its own coroutine because we don't want to
        // hold the sindex update for the changefeed update or vice-versa.
        cond_t sindexes_updated_cond;
        coro_t::spawn_now_dangerously(
            std::bind(&rdb_modification_report_cb_t::on_mod_report_sub,
                      this,
                      mod_report,
                      &sindexes_updated_cond));
        if (store_->changefeed_server.has()) {
            store_->changefeed_server->send_all(
                ql::changefeed::msg_t(
                    ql::changefeed::msg_t::change_t(
                        mod_report.info.deleted.first,
                        mod_report.info.added.first)),
                mod_report.primary_key);
        }

        sindexes_updated_cond.wait_lazily_unordered();
    }
}

void rdb_modification_report_cb_t::on_mod_report_sub(
    const rdb_modification_report_t &mod_report,
    cond_t *cond) {
    scoped_ptr_t<new_mutex_in_line_t> acq =
        store_->get_in_line_for_sindex_queue(sindex_block_);

    store_->sindex_queue_push(mod_report, acq.get());

    rdb_live_deletion_context_t deletion_context;
    rdb_update_sindexes(sindexes_, &mod_report, sindex_block_->txn(),
                        &deletion_context);
    cond->pulse();
}

std::vector<std::string> expand_geo_key(
        UNUSED reql_version_t reql_version,
        const ql::datum_t &key,
        const store_key_t &primary_key,
        boost::optional<uint64_t> tag_num) {
    // Ignore non-geometry objects in geo indexes.
    // TODO (daniel): This needs to be changed once compound geo index
    // support gets added.
    if (!key.is_ptype(ql::pseudo::geometry_string)) {
        return std::vector<std::string>();
    }

    try {
        std::vector<std::string> grid_keys =
            compute_index_grid_keys(key, GEO_INDEX_GOAL_GRID_CELLS);

        std::vector<std::string> result;
        result.reserve(grid_keys.size());
        for (size_t i = 0; i < grid_keys.size(); ++i) {
            // TODO (daniel): Something else that needs change for compound index
            //   support: We must be able to truncate geo keys and handle such
            //   truncated keys.
            rassert(grid_keys[i].length() <= ql::datum_t::trunc_size(
                key_to_unescaped_str(primary_key).length()));

            result.push_back(
                ql::datum_t::compose_secondary(grid_keys[i], primary_key, tag_num));
        }

        return result;
    } catch (const geo_exception_t &e) {
        // As things are now, this exception is actually ignored in
        // `compute_keys()`. That's ok, though it would be nice if we could
        // pass on some kind of warning to the user.
        logWRN("Failed to compute grid keys for an index: %s", e.what());
        rfail_target(&key, ql::base_exc_t::GENERIC,
                "Failed to compute grid keys: %s", e.what());
    }
}

void compute_keys(const store_key_t &primary_key, ql::datum_t doc,
                  const sindex_disk_info_t &index_info,
                  std::vector<store_key_t> *keys_out) {
    guarantee(keys_out->empty());

    const reql_version_t reql_version =
        index_info.mapping_version_info.latest_compatible_reql_version;

    // Secondary index functions are deterministic (so no need for an rdb_context_t)
    // and evaluated in a pristine environment (without global optargs).
    cond_t non_interruptor;
    ql::env_t sindex_env(&non_interruptor, reql_version);

    ql::datum_t index =
        index_info.mapping.compile_wire_func()->call(&sindex_env, doc)->as_datum();

    if (index_info.multi == sindex_multi_bool_t::MULTI
        && index.get_type() == ql::datum_t::R_ARRAY) {
        for (uint64_t i = 0; i < index.arr_size(); ++i) {
            const ql::datum_t &skey = index.get(i, ql::THROW);
            if (index_info.geo == sindex_geo_bool_t::GEO) {
                std::vector<std::string> geo_keys = expand_geo_key(reql_version,
                                                                   skey,
                                                                   primary_key,
                                                                   i);
                for (auto it = geo_keys.begin(); it != geo_keys.end(); ++it) {
                    keys_out->push_back(store_key_t(*it));
                }
            } else {
                keys_out->push_back(store_key_t(skey.print_secondary(reql_version,
                                                                     primary_key,
                                                                     i)));
            }
        }
    } else {
        if (index_info.geo == sindex_geo_bool_t::GEO) {
            std::vector<std::string> geo_keys = expand_geo_key(reql_version,
                                                               index,
                                                               primary_key,
                                                               boost::none);
            for (auto it = geo_keys.begin(); it != geo_keys.end(); ++it) {
                keys_out->push_back(store_key_t(*it));
            }
        } else {
            keys_out->push_back(store_key_t(index.print_secondary(reql_version,
                                                                  primary_key,
                                                                  boost::none)));
        }
    }
}

ARCHIVE_PRIM_MAKE_RANGED_SERIALIZABLE(
        reql_version_t, int8_t,
        reql_version_t::v1_13, reql_version_t::v1_16_is_latest);

void serialize_sindex_info(write_message_t *wm,
                           const sindex_disk_info_t &info) {
    serialize_cluster_version(wm, cluster_version_t::LATEST_DISK);
    serialize<cluster_version_t::LATEST_DISK>(
        wm, info.mapping_version_info.original_reql_version);
    serialize<cluster_version_t::LATEST_DISK>(
        wm, info.mapping_version_info.latest_compatible_reql_version);
    serialize<cluster_version_t::LATEST_DISK>(
        wm, info.mapping_version_info.latest_checked_reql_version);

    serialize<cluster_version_t::LATEST_DISK>(wm, info.mapping);
    serialize<cluster_version_t::LATEST_DISK>(wm, info.multi);
    serialize<cluster_version_t::LATEST_DISK>(wm, info.geo);
}

void deserialize_sindex_info(const std::vector<char> &data,
                             sindex_disk_info_t *info_out)
    THROWS_ONLY(archive_exc_t) {
    buffer_read_stream_t read_stream(data.data(), data.size());
    // This cluster version field is _not_ a ReQL evaluation version field, which is
    // in secondary_index_t -- it only says how the value was serialized.
    cluster_version_t cluster_version;
    archive_result_t success
        = deserialize_cluster_version(&read_stream, &cluster_version);
    throw_if_bad_deserialization(success, "sindex description");

    switch (cluster_version) {
    case cluster_version_t::v1_13:
    case cluster_version_t::v1_13_2:
        info_out->mapping_version_info.original_reql_version =
            reql_version_t::v1_13;
        info_out->mapping_version_info.latest_compatible_reql_version =
            reql_version_t::v1_13;
        info_out->mapping_version_info.latest_checked_reql_version =
            reql_version_t::v1_13;
        break;
    case cluster_version_t::v1_14:
    case cluster_version_t::v1_15_is_latest:
        success = deserialize_for_version(
                cluster_version,
                &read_stream,
                &info_out->mapping_version_info.original_reql_version);
        throw_if_bad_deserialization(success, "original_reql_version");
        success = deserialize_for_version(
                cluster_version,
                &read_stream,
                &info_out->mapping_version_info.latest_compatible_reql_version);
        throw_if_bad_deserialization(success, "latest_compatible_reql_version");
        success = deserialize_for_version(
                cluster_version,
                &read_stream,
                &info_out->mapping_version_info.latest_checked_reql_version);
        throw_if_bad_deserialization(success, "latest_checked_reql_version");
        break;
    default:
        unreachable();
    }

    success = deserialize_for_version(cluster_version, &read_stream, &info_out->mapping);
    throw_if_bad_deserialization(success, "sindex description");

    success = deserialize_for_version(cluster_version, &read_stream, &info_out->multi);
    throw_if_bad_deserialization(success, "sindex description");
    if (cluster_version == cluster_version_t::v1_13
        || cluster_version == cluster_version_t::v1_13_2
        || cluster_version == cluster_version_t::v1_14) {
        info_out->geo = sindex_geo_bool_t::REGULAR;
    } else {
        success = deserialize_for_version(cluster_version, &read_stream, &info_out->geo);
        throw_if_bad_deserialization(success, "sindex description");
    }

    guarantee(static_cast<size_t>(read_stream.tell()) == data.size(),
              "An sindex description was incompletely deserialized.");
}

/* Used below by rdb_update_sindexes. */
void rdb_update_single_sindex(
        const store_t::sindex_access_t *sindex,
        const deletion_context_t *deletion_context,
        const rdb_modification_report_t *modification,
        auto_drainer_t::lock_t) {
    // Note if you get this error it's likely that you've passed in a default
    // constructed mod_report. Don't do that.  Mod reports should always be passed
    // to a function as an output parameter before they're passed to this
    // function.
    guarantee(modification->primary_key.size() != 0);

    sindex_disk_info_t sindex_info;
    try {
        deserialize_sindex_info(sindex->sindex.opaque_definition, &sindex_info);
    } catch (const archive_exc_t &e) {
        crash("%s", e.what());
    }
    // TODO(2014-08): Actually get real profiling information for
    // secondary index updates.
    profile::trace_t *const trace = nullptr;

    superblock_t *super_block = sindex->super_block.get();

    if (modification->info.deleted.first.has()) {
        guarantee(!modification->info.deleted.second.empty());
        try {
            ql::datum_t deleted = modification->info.deleted.first;

            std::vector<store_key_t> keys;

            compute_keys(modification->primary_key, deleted, sindex_info, &keys);

            for (auto it = keys.begin(); it != keys.end(); ++it) {
                promise_t<superblock_t *> return_superblock_local;
                {
                    keyvalue_location_t kv_location;
                    rdb_value_sizer_t sizer(super_block->cache()->max_block_size());


                    find_keyvalue_location_for_write(&sizer,
                                                     super_block,
                                                     it->btree_key(),
                                                     deletion_context->balancing_detacher(),
                                                     &kv_location,
                                                     &sindex->btree->stats,
                                                     trace,
                                                     &return_superblock_local);

                    if (kv_location.value.has()) {
                        kv_location_delete(&kv_location, *it,
                            repli_timestamp_t::distant_past, deletion_context, NULL);
                    }
                    // The keyvalue location gets destroyed here.
                }
                super_block = return_superblock_local.wait();
            }
        } catch (const ql::base_exc_t &) {
            // Do nothing (it wasn't actually in the index).
        }
    }

    // If the secondary index is being deleted, we don't add any new values to
    // the sindex tree.
    // This is so we don't race against any sindex erase about who is faster
    // (we with inserting new entries, or the erase with removing them).
    const bool sindex_is_being_deleted = sindex->sindex.being_deleted;
    if (!sindex_is_being_deleted && modification->info.added.first.has()) {
        try {
            ql::datum_t added = modification->info.added.first;

            std::vector<store_key_t> keys;

            compute_keys(modification->primary_key, added, sindex_info, &keys);

            for (auto it = keys.begin(); it != keys.end(); ++it) {
                promise_t<superblock_t *> return_superblock_local;
                {
                    keyvalue_location_t kv_location;

                    rdb_value_sizer_t sizer(super_block->cache()->max_block_size());
                    find_keyvalue_location_for_write(&sizer,
                                                     super_block,
                                                     it->btree_key(),
                                                     deletion_context->balancing_detacher(),
                                                     &kv_location,
                                                     &sindex->btree->stats,
                                                     trace,
                                                     &return_superblock_local);

                    ql::serialization_result_t res =
                        kv_location_set(&kv_location, *it,
                                        modification->info.added.second,
                                        repli_timestamp_t::distant_past,
                                        deletion_context);
                    // this particular context cannot fail AT THE MOMENT.
                    guarantee(!bad(res));
                    // The keyvalue location gets destroyed here.
                }
                super_block = return_superblock_local.wait();
            }
        } catch (const ql::base_exc_t &) {
            // Do nothing (we just drop the row from the index).
        }
    }
}

void rdb_update_sindexes(const store_t::sindex_access_vector_t &sindexes,
                         const rdb_modification_report_t *modification,
                         txn_t *txn, const deletion_context_t *deletion_context) {
    {
        auto_drainer_t drainer;

        for (auto it = sindexes.begin(); it != sindexes.end(); ++it) {
            coro_t::spawn_sometime(std::bind(
                        &rdb_update_single_sindex, it->get(), deletion_context,
                        modification, auto_drainer_t::lock_t(&drainer)));
        }
    }

    /* All of the sindex have been updated now it's time to actually clear the
     * deleted blob if it exists. */
    if (modification->info.deleted.first.has()) {
        deletion_context->post_deleter()->delete_value(buf_parent_t(txn),
                modification->info.deleted.second.data());
    }
}

class post_construct_traversal_helper_t : public btree_traversal_helper_t {
public:
    post_construct_traversal_helper_t(
            store_t *store,
            const std::set<uuid_u> &sindexes_to_post_construct,
            cond_t *interrupt_myself,
            signal_t *interruptor
            )
        : store_(store),
          sindexes_to_post_construct_(sindexes_to_post_construct),
          interrupt_myself_(interrupt_myself), interruptor_(interruptor)
    { }

    void process_a_leaf(buf_lock_t *leaf_node_buf,
                        const btree_key_t *, const btree_key_t *,
                        signal_t *, int *) THROWS_ONLY(interrupted_exc_t) {

        // KSI: FML
        scoped_ptr_t<txn_t> wtxn;
        store_t::sindex_access_vector_t sindexes;

        buf_read_t leaf_read(leaf_node_buf);
        const leaf_node_t *leaf_node
            = static_cast<const leaf_node_t *>(leaf_read.get_data_read());

        // Number of key/value pairs we process before yielding
        const int MAX_CHUNK_SIZE = 10;
        int current_chunk_size = 0;
        const rdb_post_construction_deletion_context_t deletion_context;
        for (auto it = leaf::begin(*leaf_node); it != leaf::end(*leaf_node); ++it) {
            if (current_chunk_size == 0) {
                // Start a write transaction and acquire the secondary index
                // at the beginning of each chunk. We reset the transaction
                // after each chunk because large write transactions can cause
                // the cache to go into throttling, and that would interfere
                // with other transactions on this table.
                try {
                    write_token_pair_t token_pair;
                    store_->new_write_token_pair(&token_pair);

                    scoped_ptr_t<real_superblock_t> superblock;

                    // We use HARD durability because we want post construction
                    // to be throttled if we insert data faster than it can
                    // be written to disk. Otherwise we might exhaust the cache's
                    // dirty page limit and bring down the whole table.
                    // Other than that, the hard durability guarantee is not actually
                    // needed here.
                    store_->acquire_superblock_for_write(
                            repli_timestamp_t::distant_past,
                            2 + MAX_CHUNK_SIZE,
                            write_durability_t::HARD,
                            &token_pair,
                            &wtxn,
                            &superblock,
                            interruptor_);

                    // Acquire the sindex block.
                    const block_id_t sindex_block_id = superblock->get_sindex_block_id();

                    buf_lock_t sindex_block
                        = store_->acquire_sindex_block_for_write(superblock->expose_buf(),
                                                                 sindex_block_id);

                    superblock.reset();

                    store_->acquire_sindex_superblocks_for_write(
                            sindexes_to_post_construct_,
                            &sindex_block,
                            &sindexes);

                    if (sindexes.empty()) {
                        interrupt_myself_->pulse_if_not_already_pulsed();
                        return;
                    }
                } catch (const interrupted_exc_t &e) {
                    return;
                }
            }

            store_->btree->stats.pm_keys_read.record();
            store_->btree->stats.pm_total_keys_read += 1;

            /* Grab relevant values from the leaf node. */
            const btree_key_t *key = (*it).first;
            const void *value = (*it).second;
            guarantee(key);

            const store_key_t pk(key);
            rdb_modification_report_t mod_report(pk);
            const rdb_value_t *rdb_value = static_cast<const rdb_value_t *>(value);
            const max_block_size_t block_size = leaf_node_buf->cache()->max_block_size();
            mod_report.info.added
                = std::make_pair(
                    get_data(rdb_value, buf_parent_t(leaf_node_buf)),
                    std::vector<char>(rdb_value->value_ref(),
                        rdb_value->value_ref() + rdb_value->inline_size(block_size)));

            rdb_update_sindexes(sindexes, &mod_report, wtxn.get(), &deletion_context);
            store_->btree->stats.pm_keys_set.record();
            store_->btree->stats.pm_total_keys_set += 1;

            ++current_chunk_size;
            if (current_chunk_size >= MAX_CHUNK_SIZE) {
                current_chunk_size = 0;
                // Release the write transaction and yield.
                // We continue later where we have left off.
                sindexes.clear();
                wtxn.reset();
                coro_t::yield();
            }
        }
    }

    void postprocess_internal_node(buf_lock_t *) { }

    void filter_interesting_children(buf_parent_t,
                                     ranged_block_ids_t *ids_source,
                                     interesting_children_callback_t *cb) {
        for (int i = 0, e = ids_source->num_block_ids(); i < e; ++i) {
            cb->receive_interesting_child(i);
        }
        cb->no_more_interesting_children();
    }

    access_t btree_superblock_mode() { return access_t::read; }
    access_t btree_node_mode() { return access_t::read; }

    store_t *store_;
    const std::set<uuid_u> &sindexes_to_post_construct_;
    cond_t *interrupt_myself_;
    signal_t *interruptor_;
};

void post_construct_secondary_indexes(
        store_t *store,
        const std::set<uuid_u> &sindexes_to_post_construct,
        signal_t *interruptor)
    THROWS_ONLY(interrupted_exc_t) {
    cond_t local_interruptor;

    wait_any_t wait_any(&local_interruptor, interruptor);

    post_construct_traversal_helper_t helper(store,
            sindexes_to_post_construct, &local_interruptor, interruptor);
    /* Notice the ordering of progress_tracker and insertion_sentries matters.
     * insertion_sentries puts pointers in the progress tracker map. Once
     * insertion_sentries is destructed nothing has a reference to
     * progress_tracker so we know it's safe to destruct it. */
    parallel_traversal_progress_t progress_tracker;
    helper.progress = &progress_tracker;

    std::vector<map_insertion_sentry_t<uuid_u, const parallel_traversal_progress_t *> >
        insertion_sentries(sindexes_to_post_construct.size());
    auto sentry = insertion_sentries.begin();
    for (auto it = sindexes_to_post_construct.begin();
         it != sindexes_to_post_construct.end(); ++it) {
        store->add_progress_tracker(&*sentry, *it, &progress_tracker);
    }

    object_buffer_t<fifo_enforcer_sink_t::exit_read_t> read_token;
    store->new_read_token(&read_token);

    // Mind the destructor ordering.
    // The superblock must be released before txn (`btree_parallel_traversal`
    // usually already takes care of that).
    // The txn must be destructed before the cache_account.
    cache_account_t cache_account;
    scoped_ptr_t<txn_t> txn;
    scoped_ptr_t<real_superblock_t> superblock;

    store->acquire_superblock_for_read(
        &read_token,
        &txn,
        &superblock,
        interruptor,
        true /* USE_SNAPSHOT */);

    cache_account
        = txn->cache()->create_cache_account(SINDEX_POST_CONSTRUCTION_CACHE_PRIORITY);
    txn->set_account(&cache_account);

    btree_parallel_traversal(superblock.get(), &helper, &wait_any);
}
