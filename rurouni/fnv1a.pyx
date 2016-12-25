# coding: utf-8

from libc.stdint cimport uint8_t, uint32_t, int32_t


cdef int32_t FNV32a(void* key, int size) nogil:
    cdef uint8_t* p = <uint8_t*>key
    cdef uint32_t h = 2166136261UL

    for i in range(size):
        h ^= p[i]
        h *= 16777619;

    return <int32_t>h


def get_int32_hash(bytes b not None):
    """Return signed 32-bit fnv1a hash.

    NOTE: It's a historical reason that we (Douban) use signed 32-bit
    hash, you can change it if you want.
    """
    return FNV32a(<uint8_t*>b, len(b))
