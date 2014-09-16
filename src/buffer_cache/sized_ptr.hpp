// Copyright 2010-2014 RethinkDB, all rights reserved.
#ifndef BUFFER_CACHE_SIZED_PTR_HPP_
#define BUFFER_CACHE_SIZED_PTR_HPP_

#include <stdint.h>

// RSI: Move to the cache?  Idk.

template <class T>
class sized_ptr_t {
public:
    sized_ptr_t(T *_buf, uint32_t _block_size)
        : buf(_buf), block_size(_block_size) { }
    template <class U>
    sized_ptr_t(const sized_ptr_t<U> &copyee)  // NOLINT(runtime/explicit)
        : buf(copyee.buf), block_size(copyee.block_size) { }

    T *buf;
    uint32_t block_size;
};

template <class T, class U>
sized_ptr_t<T> sized_ptr_reinterpret_cast(sized_ptr_t<U> p) {
    return sized_ptr_t<T>(reinterpret_cast<T *>(p.buf), p.block_size);
}

#endif  // BUFFER_CACHE_SIZED_PTR_HPP_
