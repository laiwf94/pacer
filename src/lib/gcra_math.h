/*
 * GCRA decision steps as pure functions of the arrival time. The lock-free
 * limiter runs them inside a CAS loop; the keyed table runs them under its
 * shard lock. All times are in ticks.
 */
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

/*
 * All-or-nothing take. The caller has checked cost <= tol / iv, so
 * cost * iv cannot overflow. Returns 1 and sets *next when granted.
 */
static inline int pacer_step_take(uint64_t tat, uint64_t now, uint64_t cost, uint64_t iv,
                                  uint64_t tol, uint64_t *next, pacer_decision *d)
{
    uint64_t base = tat > now ? tat : now;
    uint64_t want = base + cost * iv;
    if (want - now > tol) {
        d->granted = 0;
        d->remaining = pacer_avail(base - now, tol, iv);
        d->retry_after_ns = pacer_ticks_to_ns_ceil(want - now - tol);
        return 0;
    }
    *next = want;
    d->granted = cost;
    d->remaining = pacer_avail(want - now, tol, iv);
    d->retry_after_ns = 0;
    return 1;
}

/* Take whatever is available up to max. Returns the grant; *next is set
 * only when the grant is non-zero. */
static inline uint64_t pacer_step_upto(uint64_t tat, uint64_t now, uint64_t max, uint64_t iv,
                                       uint64_t tol, uint64_t *next, pacer_decision *d)
{
    uint64_t base = tat > now ? tat : now;
    uint64_t avail = pacer_avail(base - now, tol, iv);
    uint64_t grant = avail < max ? avail : max;
    d->granted = grant;
    d->remaining = avail - grant;
    d->retry_after_ns = 0;
    if (grant == 0) {
        /* One token fits once base - now drops to tol - iv. When max is 0
         * nothing was asked for, so there is nothing to wait for. */
        if (max != 0) {
            d->retry_after_ns = pacer_ticks_to_ns_ceil(base - now + iv - tol);
        }
        return 0;
    }
    *next = base + grant * iv;
    return grant;
}

/* Arrival time after giving back `tokens`. Never moves before now, so a
 * refund cannot create more than a full bucket. */
static inline uint64_t pacer_step_refund(uint64_t tat, uint64_t now, uint64_t tokens, uint64_t iv,
                                         uint64_t tol)
{
    if (tat <= now) {
        return tat;
    }
    uint64_t back = tokens > tol / iv ? tol : tokens * iv;
    return tat - now > back ? tat - back : now;
}

#endif
