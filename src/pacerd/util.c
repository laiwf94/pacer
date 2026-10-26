#include "pacerd/util.h"

#include <stdio.h>
#include <string.h>

const char *pd_strerror(int err, char *buf, size_t len)
{
#if defined(__GLIBC__) && defined(_GNU_SOURCE)
    return strerror_r(err, buf, len);
#else
    if (strerror_r(err, buf, len) != 0) {
        (void)snprintf(buf, len, "error %d", err);
    }
    return buf;
#endif
}
