#include "pacerd/daemon.h"
#include "pacerd/log.h"
#include "pacerd/util.h"

#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/signalfd.h>
#include <sys/socket.h>
#include <unistd.h>

#define LISTEN_BACKLOG 1024

struct pd_cfg *pd_cfg_load(const char *path, char *err, size_t errlen)
{
    struct pd_cfg *cfg = malloc(sizeof *cfg);
    if (!cfg) {
        (void)snprintf(err, errlen, "out of memory");
        return NULL;
    }
    if (pd_config_load(path, &cfg->c, err, errlen) != 0) {
        free(cfg);
        return NULL;
    }
    atomic_init(&cfg->refs, 1);
    return cfg;
}

void pd_cfg_ref(struct pd_cfg *cfg)
{
    atomic_fetch_add_explicit(&cfg->refs, 1, memory_order_relaxed);
}

void pd_cfg_unref(struct pd_cfg *cfg)
{
    /* acq_rel: the thread that frees must see every other thread's last use. */
    if (cfg && atomic_fetch_sub_explicit(&cfg->refs, 1, memory_order_acq_rel) == 1) {
        free(cfg);
    }
}

void pd_wake(int eventfd)
{
    uint64_t one = 1;
    /* A full counter already means "wake up"; nothing else can fail here. */
    (void)write(eventfd, &one, sizeof one);
}

static void log_errno(const char *msg, const char *what, int err)
{
    char eb[128];
    struct pd_log_line l;
    pd_log_begin(&l, PD_LOG_ERROR, msg);
    if (what) {
        pd_log_str(&l, "what", what);
    }
    pd_log_str(&l, "error", pd_strerror(err, eb, sizeof eb));
    pd_log_end(&l);
}

static int open_listener(const struct pd_listener_conf *lc, struct pd_listener *l)
{
    char addr[64];
    pd_addr_format(&lc->listen, addr, sizeof addr);
    int fd = socket(lc->listen.ss.ss_family, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (fd < 0) {
        log_errno("cannot create listening socket", addr, errno);
        return -1;
    }
    int one = 1;
    /* SO_REUSEADDR lets a restart bind while old connections sit in
     * TIME_WAIT. V6ONLY keeps [::]:port from also claiming IPv4. */
    (void)setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    if (lc->listen.ss.ss_family == AF_INET6) {
        (void)setsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, &one, sizeof one);
    }
    if (bind(fd, (const struct sockaddr *)&lc->listen.ss, lc->listen.len) != 0 ||
        listen(fd, LISTEN_BACKLOG) != 0) {
        log_errno("cannot listen", addr, errno);
        (void)close(fd);
        return -1;
    }
    l->h.kind = PD_H_LISTENER;
    l->h.fd = fd;
    return 0;
}

static void close_listeners(struct pd_daemon *d)
{
    for (uint32_t i = 0; i < d->nlisteners; i++) {
        if (d->listeners[i].h.fd >= 0) {
            (void)close(d->listeners[i].h.fd);
            d->listeners[i].h.fd = -1;
        }
    }
}

static int setup_listeners(struct pd_daemon *d)
{
    const struct pd_config *c = &d->cfg->c;
    d->nlisteners = c->nlisteners;
    for (uint32_t i = 0; i < c->nlisteners; i++) {
        d->listeners[i].h.fd = -1;
        d->listeners[i].index = i;
    }
    for (uint32_t i = 0; i < c->nlisteners; i++) {
        if (open_listener(&c->listeners[i], &d->listeners[i]) != 0) {
            return -1;
        }
    }
    return 0;
}

static int start_workers(struct pd_daemon *d)
{
    d->nworkers = d->cfg->c.workers;
    d->workers = calloc(d->nworkers, sizeof *d->workers);
    if (!d->workers) {
        pd_log_msg(PD_LOG_ERROR, "out of memory");
        return -1;
    }
    for (uint32_t i = 0; i < d->nworkers; i++) {
        struct pd_worker *w = &d->workers[i];
        w->d = d;
        w->id = i;
        w->wake.kind = PD_H_WAKE;
        pd_cfg_ref(d->cfg);
        w->cfg = d->cfg;
        atomic_init(&w->pending, NULL);
        /* Created here, not in the thread, so a drain request sent while a
         * worker is still starting is never lost. */
        w->epfd = epoll_create1(EPOLL_CLOEXEC);
        w->wake.fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        if (w->epfd < 0 || w->wake.fd < 0) {
            log_errno("cannot create worker event fds", NULL, errno);
            d->nworkers = i + 1;
            return -1;
        }
    }
    for (uint32_t i = 0; i < d->nworkers; i++) {
        struct pd_worker *w = &d->workers[i];
        atomic_fetch_add(&d->workers_running, 1);
        int rc = pthread_create(&w->thread, NULL, pd_worker_main, w);
        if (rc == 0) {
            d->started = i + 1;
        }
        if (rc != 0) {
            atomic_fetch_sub(&d->workers_running, 1);
            log_errno("cannot start worker", NULL, rc);
            return -1;
        }
    }
    return 0;
}

static void begin_drain(struct pd_daemon *d, const char *why)
{
    if (atomic_exchange(&d->draining, 1)) {
        return;
    }
    struct pd_log_line l;
    pd_log_begin(&l, PD_LOG_INFO, "draining");
    pd_log_str(&l, "reason", why);
    pd_log_u64(&l, "timeout_ms", d->cfg->c.drain_timeout_ns / 1000000);
    pd_log_end(&l);
    for (uint32_t i = 0; i < d->started; i++) {
        pd_wake(d->workers[i].wake.fd);
    }
}

static void join_workers(struct pd_daemon *d)
{
    for (uint32_t i = 0; i < d->started; i++) {
        (void)pthread_join(d->workers[i].thread, NULL);
    }
}

static void free_workers(struct pd_daemon *d)
{
    for (uint32_t i = 0; i < d->nworkers; i++) {
        struct pd_worker *w = &d->workers[i];
        if (w->epfd >= 0) {
            (void)close(w->epfd);
        }
        if (w->wake.fd >= 0) {
            (void)close(w->wake.fd);
        }
        pd_cfg_unref(w->cfg);
    }
    free(d->workers);
}

static int on_signal(struct pd_daemon *d, int sfd)
{
    struct signalfd_siginfo si;
    while (read(sfd, &si, sizeof si) == (ssize_t)sizeof si) {
        if (si.ssi_signo == SIGTERM || si.ssi_signo == SIGINT) {
            begin_drain(d, si.ssi_signo == SIGTERM ? "SIGTERM" : "SIGINT");
        }
    }
    return 0;
}

/* Main thread loop: signals and worker notifications until drained. */
static int main_loop(struct pd_daemon *d, int sfd)
{
    int ep = epoll_create1(EPOLL_CLOEXEC);
    if (ep < 0) {
        log_errno("epoll_create1 failed", NULL, errno);
        begin_drain(d, "startup failure");
        return 1;
    }
    struct epoll_event ev = {.events = EPOLLIN, .data.fd = sfd};
    (void)epoll_ctl(ep, EPOLL_CTL_ADD, sfd, &ev);
    ev.data.fd = d->main_wake;
    (void)epoll_ctl(ep, EPOLL_CTL_ADD, d->main_wake, &ev);

    int listeners_closed = 0;
    while (atomic_load(&d->workers_running) > 0) {
        struct epoll_event events[8];
        int n = epoll_wait(ep, events, 8, -1);
        for (int i = 0; i < n; i++) {
            if (events[i].data.fd == sfd) {
                on_signal(d, sfd);
            } else {
                uint64_t v = 0;
                (void)read(d->main_wake, &v, sizeof v);
            }
        }
        if (atomic_load(&d->failed)) {
            begin_drain(d, "worker failure");
        }
        /* Once no worker can call accept on them, the listening sockets
         * are closed so new connections are refused at once. */
        if (!listeners_closed && atomic_load(&d->workers_off_listeners) == d->started &&
            atomic_load(&d->draining)) {
            close_listeners(d);
            listeners_closed = 1;
        }
    }
    (void)close(ep);
    return atomic_load(&d->failed) ? 1 : 0;
}

int pd_daemon_run(const char *config_path)
{
    char err[PD_ERR_MAX];
    struct pd_daemon d;
    memset(&d, 0, sizeof d);
    d.config_path = config_path;
    d.main_wake = -1;
    d.cfg = pd_cfg_load(config_path, err, sizeof err);
    if (!d.cfg) {
        struct pd_log_line l;
        pd_log_begin(&l, PD_LOG_ERROR, "bad config");
        pd_log_str(&l, "error", err);
        pd_log_end(&l);
        return 2;
    }
    pd_log_set_level(d.cfg->c.log_level);

    /* Block the signals before any thread exists so every thread inherits
     * the mask and only the signalfd sees them. */
    sigset_t mask;
    sigemptyset(&mask);
    sigaddset(&mask, SIGTERM);
    sigaddset(&mask, SIGINT);
    sigaddset(&mask, SIGHUP);
    (void)pthread_sigmask(SIG_BLOCK, &mask, NULL);
    (void)signal(SIGPIPE, SIG_IGN);

    int rc = 1;
    int sfd = signalfd(-1, &mask, SFD_NONBLOCK | SFD_CLOEXEC);
    d.main_wake = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (sfd < 0 || d.main_wake < 0) {
        log_errno("cannot create signal or event fd", NULL, errno);
    } else if (setup_listeners(&d) == 0 && start_workers(&d) == 0) {
        struct pd_log_line l;
        pd_log_begin(&l, PD_LOG_INFO, "ready");
        pd_log_str(&l, "version", pacer_version());
        pd_log_u64(&l, "workers", d.nworkers);
        pd_log_u64(&l, "listeners", d.nlisteners);
        pd_log_end(&l);
        rc = main_loop(&d, sfd);
    } else {
        atomic_store(&d.failed, 1);
        begin_drain(&d, "startup failure");
    }
    join_workers(&d);
    close_listeners(&d);
    if (rc == 0) {
        pd_log_msg(PD_LOG_INFO, "drained");
    }
    if (d.workers) {
        free_workers(&d);
    }
    pd_cfg_unref(d.cfg);
    if (sfd >= 0) {
        (void)close(sfd);
    }
    if (d.main_wake >= 0) {
        (void)close(d.main_wake);
    }
    return rc;
}
