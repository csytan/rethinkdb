// Copyright 2010-2014 RethinkDB, all rights reserved.
#ifndef BTREE_LEAF_STRUCTURE_HPP_
#define BTREE_LEAF_STRUCTURE_HPP_

#include <stdint.h>

#include "buffer_cache/types.hpp"

// RSI: Drop all use and reference to this flag.
#define NEW_LEAF_LOGIC 1

// RSI: Rename leaf_node_t to old_leaf_node_t and make leaf_node_t just have magic.

// The leaf node begins with the following struct layout.
struct leaf_node_t {
    // The magic.
    block_magic_t magic;

    // The size of pair_offsets.
    uint16_t num_pairs;

    // The total size (in bytes) of the live entries and their 2-byte
    // pair offsets in pair_offsets.  (Does not include the size of
    // the live entries' timestamps.)
    uint16_t live_size;

    // The frontmost offset.
    uint16_t frontmost;

    // The first offset whose entry is not accompanied by a timestamp.
    uint16_t tstamp_cutpoint;

    // The pair offsets.
    uint16_t pair_offsets[];

} __attribute__ ((__packed__));

#if NEW_LEAF_LOGIC
// The modern leaf node in the main b-tree.
struct main_leaf_node_t {
    block_magic_t magic;

    // The size of pair_offsets.
    uint16_t num_pairs;

    // The total size (in bytes) of the live entries and their 2-byte
    // pair offsets in pair_offsets.  (Does not include the size of
    // the live entries' timestamps.)
    uint16_t live_size;

    // The frontmost offset.
    uint16_t frontmost;

    // The offset after which there's no record of deletion entries.
    uint16_t tstamp_cutpoint;

    // The offset after which live entries don't have timestamps.  (As
    // more and more operations are done, this value eventually
    // becomes the block size.)
    uint16_t live_tstamp_cutpoint;

    // The pair offsets.
    uint16_t pair_offsets[];

} __attribute__ ((__packed__));
#endif  // NEW_LEAF_LOGIC



#endif  // BTREE_LEAF_STRUCTURE_HPP_
