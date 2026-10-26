/* Fixed-point helpers shared by the GCRA limiter and the keyed table. */
#ifndef PACER_GCRA_MATH_H
#define PACER_GCRA_MATH_H

#include "pacer.h"

/* 128-bit intermediates keep rate conversions exact; both supported
 * compilers provide the type. */
__extension__ typedef unsigned __int128 pacer_u128;

static inline uint64_t pacer_ticks(uint64_t now_ns)
{
    if (now_ns >= PACER_MAX_NOW_NS) {
        now_ns = PACER_MAX_NOW_NS - 1;
    }
    return now_ns << PACER_TICK_SHIFT;
}

static inline uint64_t pacer_ticks_to_ns_ceil(uint64_t ticks)
{
    return (ticks >> PACER_TICK_SHIFT) + ((ticks & ((1u << PACER_TICK_SHIFT) - 1)) != 0);
}

/* Whole tokens available when the bucket is `debt` ticks behind. */
static inline uint64_t pacer_avail(uint64_t debt, uint64_t tol, uint64_t iv)
{
    return debt >= tol ? 0 : (tol - debt) / iv;
}

#endif
