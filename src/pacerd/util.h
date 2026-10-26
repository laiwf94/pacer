/* Small helpers shared by the daemon. */
#ifndef PACERD_UTIL_H
#define PACERD_UTIL_H

#include <stddef.h>

/* Thread-safe strerror: writes the message for `err` into buf and returns
 * buf (or a static string the C library chose). */
const char *pd_strerror(int err, char *buf, size_t len);

#endif
