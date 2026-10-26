/*
 * pacerd internals: the shared daemon state, worker threads and connections.
 *
 * Threads: the main thread owns signals (through signalfd), config reloads
 * and the admin socket. Each worker thread runs its own epoll loop over the
 * shared listening sockets (EPOLLEXCLUSIVE) and the connections it accepted.
 * A connection never moves between workers. Shared state between threads is
 * limited to the per-listener limiters (lock-free or sharded), atomic
 * counters, and config objects handed over by reference count.
 */
#ifndef PACERD_DAEMON_H
#define PACERD_DAEMON_H

#ifndef __linux__
#error "pacerd needs Linux (epoll, signalfd, eventfd). libpacer and its tests build anywhere."
#endif

#include "pacer.h"
#include "pacerd/config.h"
#include "pacerd/heap.h"

#include <pthread.h>
#include <stdatomic.h>

/* An immutable parsed config shared by reference count. */
struct pd_cfg {
    atomic_int refs;
    struct pd_config c;
};

struct pd_cfg *pd_cfg_load(const char *path, char *err, size_t errlen);
void pd_cfg_ref(struct pd_cfg *cfg);
void pd_cfg_unref(struct pd_cfg *cfg);

/* What an epoll event points at. */
enum pd_handle_kind { PD_H_LISTENER, PD_H_WAKE, PD_H_CLIENT, PD_H_UPSTREAM };

struct pd_handle {
    int kind;
    int fd;
};

struct pd_listener {
    struct pd_handle h;
    uint32_t index;
};

struct pd_worker {
    struct pd_daemon *d;
    uint32_t id;
    pthread_t thread;
    int epfd;
    struct pd_handle wake; /* eventfd: drain, reload */
    _Atomic(struct pd_cfg *) pending;
    struct pd_cfg *cfg;
    struct pd_heap timers;
    int draining;
};

struct pd_daemon {
    const char *config_path;
    struct pd_cfg *cfg;
    uint32_t nlisteners;
    struct pd_listener listeners[PD_MAX_LISTENERS];
    uint32_t nworkers;
    uint32_t started; /* worker threads running or joinable */
    struct pd_worker *workers;
    atomic_int draining;
    int main_wake;                     /* eventfd the workers poke */
    atomic_uint workers_off_listeners; /* workers that stopped accepting */
    atomic_uint workers_running;
    atomic_int failed; /* a worker hit an unrecoverable error */
};

/* Run the daemon until it has drained. Returns the process exit code. */
int pd_daemon_run(const char *config_path);

/* Worker thread body and wake-up. */
void *pd_worker_main(void *arg);
void pd_wake(int eventfd);

#endif
