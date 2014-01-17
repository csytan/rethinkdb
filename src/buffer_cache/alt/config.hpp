#ifndef BUFFER_CACHE_ALT_CONFIG_HPP_
#define BUFFER_CACHE_ALT_CONFIG_HPP_

#include "rpc/serialize_macros.hpp"

// RSI: Maybe this config struct can just go away completely.  For now we have it to
// conform to some aspects of the interface of the mirrored cache, putting off until
// later whether certain configuration options may be removed.

namespace alt {

// RSI: Maybe get rid of this file, just put these in page.hpp and alt.hpp,
// respectively.

class page_cache_config_t {
public:
    page_cache_config_t()
        : io_priority_reads(CACHE_READS_IO_PRIORITY),
          io_priority_writes(CACHE_WRITES_IO_PRIORITY) { }

    int32_t io_priority_reads;
    int32_t io_priority_writes;

    RDB_MAKE_ME_SERIALIZABLE_2(io_priority_reads, io_priority_writes);
};

class alt_cache_config_t {
public:
    page_cache_config_t page_config;
    RDB_MAKE_ME_SERIALIZABLE_1(page_config);
};

}  // namespace alt

#endif  // BUFFER_CACHE_ALT_CONFIG_HPP_
