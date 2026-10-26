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
#include "pacerd/log.h"
#include "pacerd/util.h"

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

/* A listening socket and the limits shared by every worker that serves it.
 * Lives for the whole run; reloads change the limits in place. */
struct pd_listener {
    struct pd_handle h;
    uint32_t index;
    _Atomic uint32_t conns; /* open connections across all workers */
    pacer_table *ip_conns;  /* per-client holds and new-connection rate */
    pacer_table *ip_bytes;  /* per-client bytes per second, both directions */
    pacer_gcra bytes;       /* the listener's bytes per second */
    struct pd_log_limit reject_log;
};

/* One direction's buffer: bytes [start, end) are waiting to be written. */
struct pd_buf {
    char *data;
    uint32_t start;
    uint32_t end;
    uint32_t cap;
};

/* Directions: UP is client to upstream, DOWN is upstream to client. */
enum { PD_UP = 0, PD_DOWN = 1 };

struct pd_conn {
    struct pd_handle client;
    struct pd_handle upstream;
    struct pd_worker *w;
    struct pd_conn *prev;
    struct pd_conn *next;
    uint64_t id;
    uint32_t li; /* listener index */
    int connecting;
    int closed; /* fds closed; freed after the current event batch */
    int held;   /* holds a slot in the listener's per-client table */
    uint8_t key[PD_CLIENT_KEY_MAX];
    uint8_t keylen;
    uint32_t mask[2];     /* epoll interest: [0] client fd, [1] upstream fd */
    struct pd_buf buf[2]; /* indexed by direction */
    int eof[2];           /* the direction's source sent FIN */
    int shut[2];          /* the direction's destination was shut for writing */
    uint64_t bytes[2];
    uint64_t start_ns;
    uint64_t connect_deadline;
    uint64_t throttled_to[2]; /* reading the direction's source waits until then */
    struct pd_timer timer;
    char peer[64];
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
    struct pd_conn *conns; /* every open connection, for drain */
    struct pd_conn *dead;  /* closed this batch; events may still point here */
    uint32_t nconns;
    int draining;
    uint64_t now;              /* clock read once per loop iteration */
    int reserve_fd;            /* spare fd, released to shed a connection at EMFILE */
    uint64_t accept_paused_to; /* accepting resumes at this time; 0 when running */
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

/* Connections (proxy.c). accept takes ownership of the client fd. */
void pd_conn_accept(struct pd_worker *w, struct pd_listener *l, int fd,
                    const struct sockaddr_storage *peer);

/* Log a refused connection, rate limited per listener. */
void pd_log_reject(struct pd_listener *l, const char *listener, const char *peer,
                   const char *reason, uint64_t now);
void pd_conn_event(struct pd_conn *c, struct pd_handle *h, uint32_t events, uint64_t now);
void pd_conn_timer(struct pd_conn *c, uint64_t now);
void pd_conn_close(struct pd_conn *c, const char *reason);
void pd_conn_reap(struct pd_worker *w);

/* The connection that embeds a handle or a timer. */
struct pd_conn *pd_conn_of_handle(struct pd_handle *h);
struct pd_conn *pd_conn_of_timer(struct pd_timer *t);

#endif
