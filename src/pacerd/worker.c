#include "pacerd/daemon.h"
#include "pacerd/log.h"
#include "pacerd/util.h"

#include <errno.h>
#include <sys/epoll.h>
#include <unistd.h>

#define MAX_EVENTS 64

static void worker_fatal(struct pd_worker *w, const char *what, int err)
{
    char eb[128];
    struct pd_log_line l;
    pd_log_begin(&l, PD_LOG_ERROR, "worker failed");
    pd_log_u64(&l, "worker", w->id);
    pd_log_str(&l, "what", what);
    pd_log_str(&l, "error", pd_strerror(err, eb, sizeof eb));
    pd_log_end(&l);
    atomic_store(&w->d->failed, 1);
}

/* Drain requests and config hand-offs arrive through the wake eventfd. */
static void on_wake(struct pd_worker *w)
{
    uint64_t v = 0;
    (void)read(w->wake.fd, &v, sizeof v);
    struct pd_cfg *next = atomic_exchange(&w->pending, NULL);
    if (next) {
        pd_cfg_unref(w->cfg);
        w->cfg = next;
    }
    if (atomic_load(&w->d->draining) && !w->draining) {
        w->draining = 1;
        atomic_fetch_add(&w->d->workers_off_listeners, 1);
        pd_wake(w->d->main_wake);
    }
}

static int setup(struct pd_worker *w)
{
    if (pd_heap_init(&w->timers, 1024) != 0) {
        worker_fatal(w, "timer heap", ENOMEM);
        return -1;
    }
    struct epoll_event ev = {.events = EPOLLIN, .data.ptr = &w->wake};
    if (epoll_ctl(w->epfd, EPOLL_CTL_ADD, w->wake.fd, &ev) != 0) {
        worker_fatal(w, "epoll_ctl wake", errno);
        return -1;
    }
    return 0;
}

void *pd_worker_main(void *arg)
{
    struct pd_worker *w = arg;
    if (setup(w) == 0) {
        /* A drain may have been requested before this thread started. */
        on_wake(w);
        while (!w->draining && !atomic_load(&w->d->failed)) {
            struct epoll_event events[MAX_EVENTS];
            int n = epoll_wait(w->epfd, events, MAX_EVENTS, -1);
            if (n < 0 && errno != EINTR) {
                worker_fatal(w, "epoll_wait", errno);
                break;
            }
            for (int i = 0; i < n; i++) {
                struct pd_handle *h = events[i].data.ptr;
                if (h->kind == PD_H_WAKE) {
                    on_wake(w);
                }
            }
        }
    }
    if (!w->draining) {
        atomic_fetch_add(&w->d->workers_off_listeners, 1);
    }
    pd_heap_free(&w->timers);
    atomic_fetch_sub(&w->d->workers_running, 1);
    pd_wake(w->d->main_wake);
    return NULL;
}
