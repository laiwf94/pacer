#include "pacerd/daemon.h"
#include "pacerd/log.h"
#include "pacerd/util.h"

#include <errno.h>
#include <fcntl.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#define MAX_EVENTS      64
#define ACCEPT_PER_WAKE 32
#define ACCEPT_PAUSE_NS 100000000ull

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

static void listeners_off(struct pd_worker *w)
{
    for (uint32_t i = 0; i < w->d->nlisteners; i++) {
        /* The listener stays open until every worker has let go of it, so
         * this cannot race with the main thread closing it. */
        (void)epoll_ctl(w->epfd, EPOLL_CTL_DEL, w->d->listeners[i].h.fd, NULL);
    }
}

static int listeners_on(struct pd_worker *w)
{
    for (uint32_t i = 0; i < w->d->nlisteners; i++) {
        /* EPOLLEXCLUSIVE wakes one worker per new connection instead of
         * all of them. */
        struct epoll_event ev = {.events = EPOLLIN | EPOLLEXCLUSIVE,
                                 .data.ptr = &w->d->listeners[i].h};
        if (epoll_ctl(w->epfd, EPOLL_CTL_ADD, w->d->listeners[i].h.fd, &ev) != 0 &&
            errno != EEXIST) {
            return -1;
        }
    }
    return 0;
}

static void stop_accepting(struct pd_worker *w)
{
    if (!w->accept_paused_to) {
        listeners_off(w);
    }
    w->accept_paused_to = 0;
    w->draining = 1;
    atomic_fetch_add(&w->d->workers_off_listeners, 1);
    pd_wake(w->d->main_wake);
}

static void close_all(struct pd_worker *w, const char *reason)
{
    while (w->conns) {
        pd_conn_close(w->conns, reason);
    }
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
        stop_accepting(w);
        close_all(w, "shutdown");
    }
}

/*
 * Out of file descriptors. The pending connection stays queued, so the
 * listener stays readable and a level-triggered loop would spin. Give up
 * the spare fd, accept the connection and close it at once (the client
 * sees a clean close instead of a hang), then take the spare back. If the
 * spare cannot be had, stop accepting for a short while instead.
 */
static void shed_connection(struct pd_worker *w, struct pd_listener *l, uint64_t now)
{
    const char *name = w->cfg->c.listeners[l->index].name;
    if (w->reserve_fd >= 0) {
        (void)close(w->reserve_fd);
        int fd = accept4(l->h.fd, NULL, NULL, SOCK_CLOEXEC);
        if (fd >= 0) {
            (void)close(fd);
            pd_log_reject(l, name, "-", "fd_exhausted", now);
        }
        w->reserve_fd = open("/dev/null", O_RDONLY | O_CLOEXEC);
    }
    if (w->reserve_fd < 0) {
        listeners_off(w);
        w->accept_paused_to = now + ACCEPT_PAUSE_NS;
        pd_log_reject(l, name, "-", "fd_exhausted_paused", now);
    }
}

static void on_listener(struct pd_worker *w, struct pd_listener *l, uint64_t now)
{
    for (int i = 0; i < ACCEPT_PER_WAKE && !w->accept_paused_to; i++) {
        struct sockaddr_storage peer;
        socklen_t len = sizeof peer;
        int fd = accept4(l->h.fd, (struct sockaddr *)&peer, &len, SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (fd >= 0) {
            pd_conn_accept(w, l, fd, &peer);
            continue;
        }
        if (errno == EINTR || errno == ECONNABORTED) {
            continue;
        }
        if (errno == EMFILE || errno == ENFILE) {
            shed_connection(w, l, now);
        }
        /* EAGAIN: the queue is empty or another worker got there first. */
        return;
    }
}

static void maybe_resume_accepting(struct pd_worker *w, uint64_t now)
{
    if (!w->accept_paused_to || now < w->accept_paused_to) {
        return;
    }
    w->accept_paused_to = 0;
    if (w->reserve_fd < 0) {
        w->reserve_fd = open("/dev/null", O_RDONLY | O_CLOEXEC);
    }
    if (listeners_on(w) != 0) {
        worker_fatal(w, "epoll_ctl listener", errno);
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
    w->reserve_fd = open("/dev/null", O_RDONLY | O_CLOEXEC);
    if (listeners_on(w) != 0) {
        worker_fatal(w, "epoll_ctl listener", errno);
        return -1;
    }
    return 0;
}

static int wait_ms(struct pd_worker *w, uint64_t now)
{
    struct pd_timer *t = pd_heap_peek(&w->timers);
    uint64_t next = t ? t->deadline : UINT64_MAX;
    if (w->accept_paused_to && w->accept_paused_to < next) {
        next = w->accept_paused_to;
    }
    if (next == UINT64_MAX) {
        return -1;
    }
    if (next <= now) {
        return 0;
    }
    /* Round up so the loop never wakes just before a deadline. */
    uint64_t ms = (next - now + 999999) / 1000000;
    return ms > 60000 ? 60000 : (int)ms;
}

static void run_timers(struct pd_worker *w, uint64_t now)
{
    struct pd_timer *t = NULL;
    while ((t = pd_heap_pop_due(&w->timers, now)) != NULL) {
        pd_conn_timer(pd_conn_of_timer(t), now);
    }
}

void *pd_worker_main(void *arg)
{
    struct pd_worker *w = arg;
    if (setup(w) == 0) {
        /* A drain may have been requested before this thread started. */
        on_wake(w);
        while (!(w->draining && w->nconns == 0) && !atomic_load(&w->d->failed)) {
            struct epoll_event events[MAX_EVENTS];
            int n = epoll_wait(w->epfd, events, MAX_EVENTS, wait_ms(w, pacer_now_ns()));
            if (n < 0 && errno != EINTR) {
                worker_fatal(w, "epoll_wait", errno);
                break;
            }
            uint64_t now = pacer_now_ns();
            for (int i = 0; i < n; i++) {
                struct pd_handle *h = events[i].data.ptr;
                switch (h->kind) {
                case PD_H_WAKE:
                    on_wake(w);
                    break;
                case PD_H_LISTENER:
                    if (!w->draining) {
                        on_listener(w, (struct pd_listener *)h, now);
                    }
                    break;
                default:
                    pd_conn_event(pd_conn_of_handle(h), h, events[i].events, now);
                    break;
                }
            }
            run_timers(w, now);
            maybe_resume_accepting(w, now);
            pd_conn_reap(w);
        }
    }
    close_all(w, "shutdown");
    pd_conn_reap(w);
    if (w->reserve_fd >= 0) {
        (void)close(w->reserve_fd);
    }
    if (!w->draining) {
        atomic_fetch_add(&w->d->workers_off_listeners, 1);
    }
    pd_heap_free(&w->timers);
    atomic_fetch_sub(&w->d->workers_running, 1);
    pd_wake(w->d->main_wake);
    return NULL;
}
