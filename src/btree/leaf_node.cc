#include "btree/leaf_node.hpp"

#include "btree/node.hpp"
#include "serializer/buf_ptr.hpp"
#include "repli_timestamp.hpp"

namespace leaf {

leaf::iterator begin(sized_ptr_t<const leaf_node_t> leaf_node) {
    // RSI: Generify
    return old_leaf::begin(leaf_node.buf);
}
leaf::iterator end(sized_ptr_t<const leaf_node_t> leaf_node) {
    // RSI: Generify
    return old_leaf::begin(leaf_node.buf);
}

leaf::reverse_iterator rbegin(sized_ptr_t<const leaf_node_t> leaf_node) {
    // RSI: Generify
    return old_leaf::rbegin(leaf_node.buf);
}
leaf::reverse_iterator rend(sized_ptr_t<const leaf_node_t> leaf_node) {
    // RSI: Generify
    return old_leaf::rend(leaf_node.buf);
}

leaf::iterator inclusive_lower_bound(const btree_key_t *key,
                                     sized_ptr_t<const leaf_node_t> leaf_node) {
    // RSI: Generify
    return old_leaf::inclusive_lower_bound(key, leaf_node.buf);
}
leaf::reverse_iterator inclusive_upper_bound(
        const btree_key_t *key,
        sized_ptr_t<const leaf_node_t> leaf_node) {
    // RSI: Generify.
    return old_leaf::inclusive_upper_bound(key, leaf_node.buf);
}

buf_ptr_t init(value_sizer_t *sizer) {
    // RSI: Generify?  Or not quite.
    buf_ptr_t buf = buf_ptr_t::alloc_zeroed(sizer->default_block_size());
    old_leaf::init(sizer, static_cast<leaf_node_t *>(buf.cache_data()));
    return buf;
}

bool is_empty(sized_ptr_t<const leaf_node_t> node) {
    // RSI: Generify.
    return old_leaf::is_empty(node.buf);
}

bool is_full(value_sizer_t *sizer, sized_ptr_t<const leaf_node_t> node,
             const btree_key_t *key, const void *value) {
    // RSI: Generify.
    return old_leaf::is_full(sizer, node.buf, key, value);
}

bool is_underfull(value_sizer_t *sizer, sized_ptr_t<const leaf_node_t> node) {
    // RSI: Generify.
    return old_leaf::is_underfull(sizer, node.buf);
}

// RSI: This signature will change.
void split(value_sizer_t *sizer, leaf_node_t *node, leaf_node_t *sibling,
           store_key_t *median_out) {
    // RSI: Generify.
    return old_leaf::split(sizer, node, sibling, median_out);
}

// RSI: This signature will change.
void merge(value_sizer_t *sizer, leaf_node_t *left, leaf_node_t *right) {
    // RSI: Generify.
    return old_leaf::merge(sizer, left, right);
}

// RSI: This signature will need to be changed... likewise.
bool level(value_sizer_t *sizer, int nodecmp_node_with_sib, leaf_node_t *node,
           leaf_node_t *sibling, btree_key_t *replacement_key_out,
           std::vector<const void *> *moved_values_out) {
    // RSI: Generify.
    return old_leaf::level(sizer, nodecmp_node_with_sib, node, sibling, replacement_key_out, moved_values_out);
}

bool is_mergable(value_sizer_t *sizer, sized_ptr_t<const leaf_node_t> node, sized_ptr_t<const leaf_node_t> sibling) {
    // RSI: Generify
    return old_leaf::is_mergable(sizer, node.buf, sibling.buf);
}

bool find_key(const leaf_node_t *node, const btree_key_t *key, int *index_out) {
    // RSI: Generify
    return old_leaf::find_key(node, key, index_out);
}

bool lookup(value_sizer_t *sizer, sized_ptr_t<const leaf_node_t> node, const btree_key_t *key, void *value_out) {
    // RSI: Generify
    return old_leaf::lookup(sizer, node.buf, key, value_out);
}

void insert(value_sizer_t *sizer, leaf_node_t *node, const btree_key_t *key, const void *value, repli_timestamp_t tstamp) {
    // RSI: Generify
    return old_leaf::insert(sizer, node, key, value, tstamp);
}

void remove(value_sizer_t *sizer, leaf_node_t *node, const btree_key_t *key, repli_timestamp_t tstamp) {
    // RSI: Generify
    return old_leaf::remove(sizer, node, key, tstamp);
}

void erase_presence(value_sizer_t *sizer, leaf_node_t *node, const btree_key_t *key) {
    // RSI: Generify
    return old_leaf::erase_presence(sizer, node, key);
}

void validate(value_sizer_t *sizer, sized_ptr_t<const leaf_node_t> node) {
#ifndef NDEBUG
    // RSI: Generify
    rassert(sizer->default_block_size().value() == node.block_size);
    old_leaf::validate(sizer, node.buf);
#endif
}

void dump_entries_since_time(value_sizer_t *sizer, sized_ptr_t<const leaf_node_t> node, repli_timestamp_t minimum_tstamp, repli_timestamp_t maximum_possible_timestamp, entry_reception_callback_t *cb) {
    // RSI: Generify
    return old_leaf::dump_entries_since_time(sizer, node.buf, minimum_tstamp, maximum_possible_timestamp, cb);
}


}  // namespace leaf
