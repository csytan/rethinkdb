// Copyright 2010-2014 RethinkDB, all rights reserved.
#ifndef BTREE_LEAF_NODE_HPP_
#define BTREE_LEAF_NODE_HPP_

#include <vector>

#include "btree/old_leaf_node.hpp"
#include "buffer_cache/sized_ptr.hpp"
#include "errors.hpp"

struct leaf_node_t;
struct btree_key_t;
struct store_key_t;
class buf_ptr_t;
class repli_timestamp_t;
class value_sizer_t;

namespace leaf {
using old_leaf::iterator;
using old_leaf::reverse_iterator;

leaf::iterator begin(sized_ptr_t<const leaf_node_t> leaf_node);
leaf::iterator end(sized_ptr_t<const leaf_node_t> leaf_node);

leaf::reverse_iterator rbegin(sized_ptr_t<const leaf_node_t> leaf_node);
leaf::reverse_iterator rend(sized_ptr_t<const leaf_node_t> leaf_node);

leaf::iterator inclusive_lower_bound(const btree_key_t *key,
                                         sized_ptr_t<const leaf_node_t> leaf_node);
leaf::reverse_iterator inclusive_upper_bound(
        const btree_key_t *key,
        sized_ptr_t<const leaf_node_t> leaf_node);

buf_ptr_t init(value_sizer_t *sizer);

bool is_empty(sized_ptr_t<const leaf_node_t> node);

bool is_full(value_sizer_t *sizer, sized_ptr_t<const leaf_node_t> node,
             const btree_key_t *key, const void *value);

bool is_underfull(value_sizer_t *sizer, sized_ptr_t<const leaf_node_t> node);

// RSI: This signature will change.
void split(value_sizer_t *sizer, leaf_node_t *node, leaf_node_t *sibling,
           store_key_t *median_out);

// RSI: This signature will change.
void merge(value_sizer_t *sizer, leaf_node_t *left, leaf_node_t *right);

// RSI: This signature will need to be changed... likewise.
bool level(value_sizer_t *sizer, int nodecmp_node_with_sib, leaf_node_t *node,
           leaf_node_t *sibling, btree_key_t *replacement_key_out,
           std::vector<const void *> *moved_values_out);
bool is_mergable(value_sizer_t *sizer,
                 sized_ptr_t<const leaf_node_t> node,
                 sized_ptr_t<const leaf_node_t> sibling);

bool lookup(value_sizer_t *sizer, sized_ptr_t<const leaf_node_t> node,
            const btree_key_t *key, void *value_out);

// RSI: This'll change.
void insert(value_sizer_t *sizer, leaf_node_t *node, const btree_key_t *key, const void *value, repli_timestamp_t tstamp);

// RSI: This'll change.
void remove(value_sizer_t *sizer, leaf_node_t *node, const btree_key_t *key, repli_timestamp_t tstamp);

// RSI: This signature will need to be changed -- we need to be able to resize an
// acquired loaded buf.
void erase_presence(value_sizer_t *sizer, leaf_node_t *node, const btree_key_t *key);

void validate(value_sizer_t *sizer, sized_ptr_t<const leaf_node_t> node);

using old_leaf::entry_reception_callback_t;

void dump_entries_since_time(value_sizer_t *sizer,
                             sized_ptr_t<const leaf_node_t> node,
                             repli_timestamp_t minimum_tstamp,
                             repli_timestamp_t maximum_possible_timestamp,
                             entry_reception_callback_t *cb);

}  // namespace leaf

#endif  // BTREE_LEAF_NODE_HPP_
