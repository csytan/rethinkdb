// Copyright 2010-2014 RethinkDB, all rights reserved.
#ifndef BTREE_LEAF_STRUCTURE_HPP_
#define BTREE_LEAF_STRUCTURE_HPP_

#include <stdint.h>

#include "buffer_cache/types.hpp"

// RSI: Drop all use and reference to this flag.
#define NEW_LEAF_LOGIC 1

// RSI: Rename leaf_node_t to old_leaf_node_t and make leaf_node_t just have magic.

// The original leaf node layout, that old leaf nodes have.  Not all live entries have timestamps.
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
// The modern secondary index leaf node.  We don't have tstamp_cutpoint or live_tstamp_cutpoint,
// because there are no timestamps.  We can migrate directly from the old format to this one,
// because there's no way that migration could cause a triple-split or any split -- old secondary
// indexes aren't post-constructing, which means we don't need to store any of the post-contruction
// metadata, so converting to the modern format strictly frees up space (because we delete
// timestamps and remove deletion entries).
struct sindex_leaf_node_t {
    // This magic can be one of two values.  It tells us whether the node is in post-construction
    // state or not.  If it's in "post-construction" state, there can be deletion entries, old
    // entries, and live entries are tagged to indicate whether a deletion entry should be left
    // behind.  Otherwise, there are just values.
    block_magic_t magic;

    // The size of pair_offsets.
    uint16_t num_pairs;

    // The frontmost offset.
    uint16_t frontmost;

    // The pair offsets.
    uint16_t pair_offsets[];

} __attribute__ ((__packed__));

// The modern leaf node in the main b-tree.  It needs to be possible to gradually migrate from the
// original leaf node format to main_leaf_node_t -- in particular, to do so without causing a
// triple-split, where one node gets broken into three.
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
