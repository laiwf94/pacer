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

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define PACER_VERSION "0.1.0"

/* Library version string, for logs and `pacerd -v`. */
const char *pacer_version(void);

/*
 * Monotonic nanoseconds since the first call in this process. Thread-safe.
 * Starting near zero keeps the fixed-point limiter math far from overflow.
 */
uint64_t pacer_now_ns(void);

#ifdef __cplusplus
}
#endif

#endif
