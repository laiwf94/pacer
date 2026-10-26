/*
 * libpacer: rate limiting primitives.
 *
 * Ownership: every object is allocated by the caller or by a *_create
 * function and released by the matching *_destroy. The library never keeps a
 * pointer to caller memory after a call returns.
 *
 * Time: every decision takes `now_ns`, a monotonic timestamp in nanoseconds
 * chosen by the caller. pacer_now_ns() returns one relative to the first call
 * in the process. Passing time explicitly keeps the library free of hidden
 * clock reads and makes tests exact.
 */
#ifndef PACER_H
#define PACER_H

#include <stdbool.h>
#include <stdint.h>

#define PACER_VERSION "0.1.0"

/* Library version string, for logs and `pacerd -v`. */
const char *pacer_version(void);

/*
 * Monotonic nanoseconds since the first call in this process. Thread-safe.
 * Starting near zero keeps the fixed-point limiter math far from overflow.
 */
uint64_t pacer_now_ns(void);

/* Error codes. Functions that can fail return 0 or one of these. */
enum {
    PACER_OK = 0,
    PACER_EINVAL = -1, /* argument or configuration out of range */
    PACER_ERANGE = -2, /* cost larger than the burst: can never be granted */
};

/* Short description of an error code, never NULL. */
const char *pacer_strerror(int err);

/*
 * A rate: `tokens` per `period_ns`, with a bucket `burst` tokens deep. A
 * fresh limiter grants `burst` tokens at once, then refills at the rate.
 * Example: 10 per second with burst 20 is {10, 1000000000, 20}.
 */
typedef struct pacer_rate {
    uint64_t tokens;
    uint64_t period_ns;
    uint64_t burst;
} pacer_rate;

/*
 * Internally time is kept in ticks of 1/64 ns so that high byte rates keep
 * their precision. The emission interval is rounded up, so the limiter never
 * grants faster than configured; the relative error is below 1/interval,
 * under 0.2% for rates up to 10^8 per second. Faster rates are capped at
 * one token per tick. Timestamps past PACER_MAX_NOW_NS (about 4.5 years of
 * process uptime) are clamped.
 */
#define PACER_TICK_SHIFT    6
#define PACER_MAX_NOW_NS    ((uint64_t)1 << 57)
#define PACER_MAX_TOL_TICKS ((uint64_t)1 << 62)

/* The outcome of a take. */
typedef struct pacer_decision {
    uint64_t granted;        /* tokens granted; 0 when denied */
    uint64_t remaining;      /* tokens still available after this call */
    uint64_t retry_after_ns; /* when nothing was granted: wait this long */
} pacer_decision;

/*
 * Generic cell rate algorithm limiter. The whole state is one 64-bit
 * theoretical arrival time updated with compare-and-swap, so takes from any
 * number of threads are lock-free. The struct may be embedded anywhere;
 * there is nothing to destroy.
 */
typedef struct pacer_gcra {
    _Atomic uint64_t tat;       /* theoretical arrival time, ticks */
    _Atomic uint64_t interval;  /* ticks per token */
    _Atomic uint64_t tolerance; /* burst * interval, ticks */
} pacer_gcra;

/* Validate a rate and compute its interval and tolerance in ticks. */
int pacer_rate_ticks(const pacer_rate *r, uint64_t *interval, uint64_t *tolerance);

/* Initialise a limiter with a full bucket. Not thread-safe. */
int pacer_gcra_init(pacer_gcra *g, const pacer_rate *r);

/*
 * Take `cost` tokens at `now_ns`, all or nothing. Returns 1 when granted, 0
 * when denied (state unchanged, d->retry_after_ns says when the same cost
 * would fit), or PACER_ERANGE when cost exceeds the burst. A cost of 0 is
 * always granted and only reports `remaining`. `d` may be NULL.
 */
int pacer_gcra_take(pacer_gcra *g, uint64_t now_ns, uint64_t cost, pacer_decision *d);

/* Tokens available at `now_ns` without taking any. */
uint64_t pacer_gcra_remaining(pacer_gcra *g, uint64_t now_ns);

/*
 * Take as many tokens as are available, up to `max`. Returns the number
 * granted. When it is 0, d->retry_after_ns says when one token will be
 * ready. Used for byte shaping, where a read is sized by the grant and the
 * unused part handed back with pacer_gcra_refund. `d` may be NULL.
 */
uint64_t pacer_gcra_take_upto(pacer_gcra *g, uint64_t now_ns, uint64_t max, pacer_decision *d);

/*
 * Give back `tokens` previously granted and not used. The state never moves
 * before `now_ns`, so a refund cannot create more than a full bucket.
 */
void pacer_gcra_refund(pacer_gcra *g, uint64_t now_ns, uint64_t tokens);

/*
 * Change the rate while other threads keep taking. Debt is kept as time:
 * a client that had to wait 2 s still waits 2 s. A take running at the same
 * moment may combine the old interval with the new tolerance; the result
 * is bounded by the larger of the two configurations.
 */
int pacer_gcra_reconfigure(pacer_gcra *g, const pacer_rate *r);

#endif
