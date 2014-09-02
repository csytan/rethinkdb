// Copyright 2010-2014 RethinkDB, all rights reserved.
#ifndef BTREE_LEAF_STRUCTURE_HPP_
#define BTREE_LEAF_STRUCTURE_HPP_

#include <stdint.h>

#include "buffer_cache/types.hpp"

// The leaf node begins with the following struct layout.
struct leaf_node_t {
    // The value-type-specific magic value.  It's a bit of a hack, but
    // it's possible to construct a value_sizer_t based on this value.
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



#endif  // BTREE_LEAF_STRUCTURE_HPP_
