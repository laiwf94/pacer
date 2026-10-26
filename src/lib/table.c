#include "lib/gcra_math.h"
#include "lib/siphash.h"
#include "pacer.h"

#include <pthread.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

#define CACHE_LINE     64
#define MAX_SHARDS     65536u
#define MAX_ENTRIES    (1u << 28)
#define DEFAULT_SHARDS 64u
#define MIN_PER_SHARD  16u

/* One key's state. 64 bytes, so an entry never straddles two cache lines. */
struct entry {
    uint64_t hash;
    uint64_t tat;
    uint32_t pad[3];
    uint8_t keylen; /* 0 marks an empty slot */
    uint8_t key[PACER_KEY_MAX];
};

_Static_assert(sizeof(struct entry) == 64, "entry should fill one cache line");

struct shard {
    pthread_mutex_t mu;
    struct entry *slots;
    uint32_t mask;  /* slot count - 1; the count is a power of two */
    uint32_t limit; /* most keys this shard may hold */
    uint32_t count;
    uint64_t inserts;
    uint64_t removals;
    uint64_t rejections;
} __attribute__((aligned(CACHE_LINE)));

struct pacer_table {
    struct shard *shards;
    struct entry *slots;
    uint32_t nshards;
    uint32_t shard_shift; /* the top bits of a hash pick the shard */
    uint32_t capacity;
    size_t memory;
    _Atomic uint64_t interval;
    _Atomic uint64_t tolerance;
    uint8_t hash_key[PACER_SIPHASH_KEY_LEN];
};

static uint32_t next_pow2(uint32_t v)
{
    uint32_t p = 1;
    while (p < v) {
        p <<= 1;
    }
    return p;
}

static uint32_t log2_u32(uint32_t v)
{
    uint32_t r = 0;
    while (v > 1) {
        v >>= 1;
        r++;
    }
    return r;
}

static uint32_t pick_shards(uint32_t max_entries)
{
    uint32_t n = DEFAULT_SHARDS;
    while (n > 1 && max_entries / n < MIN_PER_SHARD) {
        n >>= 1;
    }
    return n;
}

int pacer_table_create(const pacer_table_config *cfg, pacer_table **out)
{
    if (!cfg || !out || cfg->max_entries == 0 || cfg->max_entries > MAX_ENTRIES) {
        return PACER_EINVAL;
    }
    uint32_t nshards = cfg->shards ? cfg->shards : pick_shards(cfg->max_entries);
    if (nshards > MAX_SHARDS || (nshards & (nshards - 1)) != 0 || nshards > cfg->max_entries) {
        return PACER_EINVAL;
    }
    uint64_t iv = 0, tol = 0;
    int rc = pacer_rate_ticks(&cfg->rate, &iv, &tol);
    if (rc != PACER_OK) {
        return rc;
    }

    /* Spread max_entries over the shards exactly, then size each slot array
     * so it is at most three quarters full. */
    uint32_t base = cfg->max_entries / nshards, extra = cfg->max_entries % nshards;
    uint32_t slots_per_shard = next_pow2(((base + 1) * 4 + 2) / 3);
    size_t nslots = (size_t)slots_per_shard * nshards;

    pacer_table *t = calloc(1, sizeof *t);
    struct shard *shards = aligned_alloc(CACHE_LINE, sizeof(struct shard) * nshards);
    struct entry *slots = calloc(nslots, sizeof(struct entry));
    if (!t || !shards || !slots) {
        free(t);
        free(shards);
        free(slots);
        return PACER_ENOMEM;
    }
    memset(shards, 0, sizeof(struct shard) * nshards);
    for (uint32_t i = 0; i < nshards; i++) {
        struct shard *s = &shards[i];
        (void)pthread_mutex_init(&s->mu, NULL);
        s->slots = slots + (size_t)i * slots_per_shard;
        s->mask = slots_per_shard - 1;
        s->limit = base + (i < extra ? 1 : 0);
    }
    t->shards = shards;
    t->slots = slots;
    t->nshards = nshards;
    t->shard_shift = 64 - log2_u32(nshards);
    t->capacity = cfg->max_entries;
    t->memory = sizeof *t + sizeof(struct shard) * nshards + sizeof(struct entry) * nslots;
    atomic_init(&t->interval, iv);
    atomic_init(&t->tolerance, tol);
    if (cfg->hash_key) {
        memcpy(t->hash_key, cfg->hash_key, sizeof t->hash_key);
    } else if (pacer_random_bytes(t->hash_key, sizeof t->hash_key) != 0) {
        pacer_table_destroy(t);
        return PACER_ENOMEM;
    }
    *out = t;
    return PACER_OK;
}

void pacer_table_destroy(pacer_table *t)
{
    if (!t) {
        return;
    }
    for (uint32_t i = 0; i < t->nshards; i++) {
        (void)pthread_mutex_destroy(&t->shards[i].mu);
    }
    free(t->slots);
    free(t->shards);
    free(t);
}

static struct shard *shard_for(pacer_table *t, uint64_t hash)
{
    /* shard_shift is 64 for a single shard, and shifting by 64 is undefined. */
    return t->nshards == 1 ? &t->shards[0] : &t->shards[hash >> t->shard_shift];
}

static int key_ok(const void *key, size_t len)
{
    return key && len > 0 && len <= PACER_KEY_MAX;
}

/* Index of the key's entry, or the empty slot that ends its probe chain. */
static uint32_t probe(const struct shard *s, uint64_t hash, const void *key, size_t len)
{
    uint32_t i = (uint32_t)hash & s->mask;
    for (;;) {
        const struct entry *e = &s->slots[i];
        if (e->keylen == 0 ||
            (e->hash == hash && e->keylen == len && memcmp(e->key, key, len) == 0)) {
            return i;
        }
        i = (i + 1) & s->mask;
    }
}

/* Find or add the key. Returns NULL when the shard is full. */
static struct entry *find_or_insert(struct shard *s, uint64_t hash, const void *key, size_t len)
{
    struct entry *e = &s->slots[probe(s, hash, key, len)];
    if (e->keylen != 0) {
        return e;
    }
    if (s->count == s->limit) {
        s->rejections++;
        return NULL;
    }
    e->hash = hash;
    e->tat = 0;
    e->keylen = (uint8_t)len;
    memcpy(e->key, key, len);
    s->count++;
    s->inserts++;
    return e;
}

/*
 * Delete by shifting later members of the probe chain back into the hole,
 * so lookups never need tombstones. An entry at j may fill hole i only if
 * its home slot is not in the cyclic range (i, j].
 */
static void delete_at(struct shard *s, uint32_t i)
{
    uint32_t j = i;
    for (;;) {
        j = (j + 1) & s->mask;
        struct entry *e = &s->slots[j];
        if (e->keylen == 0) {
            break;
        }
        uint32_t home = (uint32_t)e->hash & s->mask;
        int stays = i <= j ? (i < home && home <= j) : (i < home || home <= j);
        if (stays) {
            continue;
        }
        s->slots[i] = *e;
        i = j;
    }
    memset(&s->slots[i], 0, sizeof s->slots[i]);
    s->count--;
    s->removals++;
}

static void load_rate(pacer_table *t, uint64_t *iv, uint64_t *tol)
{
    *iv = atomic_load_explicit(&t->interval, memory_order_relaxed);
    *tol = atomic_load_explicit(&t->tolerance, memory_order_relaxed);
}

int pacer_table_take(pacer_table *t, const void *key, size_t len, uint64_t now_ns, uint64_t cost,
                     pacer_decision *d)
{
    pacer_decision local;
    if (!d) {
        d = &local;
    }
    *d = (pacer_decision){0, 0, 0};
    if (!key_ok(key, len)) {
        return PACER_EINVAL;
    }
    uint64_t iv = 0, tol = 0;
    load_rate(t, &iv, &tol);
    if (cost > tol / iv) {
        return PACER_ERANGE;
    }
    uint64_t now = pacer_ticks(now_ns);
    uint64_t hash = pacer_siphash24(t->hash_key, key, len);
    struct shard *s = shard_for(t, hash);

    (void)pthread_mutex_lock(&s->mu);
    struct entry *e = find_or_insert(s, hash, key, len);
    int rc = PACER_EFULL;
    if (e) {
        uint64_t next = 0;
        rc = pacer_step_take(e->tat, now, cost, iv, tol, &next, d);
        if (rc == 1) {
            e->tat = next;
        }
    }
    (void)pthread_mutex_unlock(&s->mu);
    return rc;
}

int pacer_table_take_upto(pacer_table *t, const void *key, size_t len, uint64_t now_ns,
                          uint64_t max, pacer_decision *d)
{
    *d = (pacer_decision){0, 0, 0};
    if (!key_ok(key, len)) {
        return PACER_EINVAL;
    }
    uint64_t iv = 0, tol = 0;
    load_rate(t, &iv, &tol);
    uint64_t now = pacer_ticks(now_ns);
    uint64_t hash = pacer_siphash24(t->hash_key, key, len);
    struct shard *s = shard_for(t, hash);

    (void)pthread_mutex_lock(&s->mu);
    struct entry *e = find_or_insert(s, hash, key, len);
    int rc = PACER_EFULL;
    if (e) {
        uint64_t next = 0;
        if (pacer_step_upto(e->tat, now, max, iv, tol, &next, d) != 0) {
            e->tat = next;
        }
        rc = PACER_OK;
    }
    (void)pthread_mutex_unlock(&s->mu);
    return rc;
}

void pacer_table_refund(pacer_table *t, const void *key, size_t len, uint64_t now_ns,
                        uint64_t tokens)
{
    if (!key_ok(key, len)) {
        return;
    }
    uint64_t iv = 0, tol = 0;
    load_rate(t, &iv, &tol);
    uint64_t now = pacer_ticks(now_ns);
    uint64_t hash = pacer_siphash24(t->hash_key, key, len);
    struct shard *s = shard_for(t, hash);

    (void)pthread_mutex_lock(&s->mu);
    struct entry *e = &s->slots[probe(s, hash, key, len)];
    if (e->keylen != 0) {
        e->tat = pacer_step_refund(e->tat, now, tokens, iv, tol);
    }
    (void)pthread_mutex_unlock(&s->mu);
}

int pacer_table_remove(pacer_table *t, const void *key, size_t len)
{
    if (!key_ok(key, len)) {
        return PACER_EINVAL;
    }
    uint64_t hash = pacer_siphash24(t->hash_key, key, len);
    struct shard *s = shard_for(t, hash);

    (void)pthread_mutex_lock(&s->mu);
    uint32_t i = probe(s, hash, key, len);
    int rc = PACER_ENOENT;
    if (s->slots[i].keylen != 0) {
        delete_at(s, i);
        rc = PACER_OK;
    }
    (void)pthread_mutex_unlock(&s->mu);
    return rc;
}

int pacer_table_reconfigure(pacer_table *t, const pacer_rate *r)
{
    uint64_t iv = 0, tol = 0;
    int rc = pacer_rate_ticks(r, &iv, &tol);
    if (rc != PACER_OK) {
        return rc;
    }
    atomic_store_explicit(&t->interval, iv, memory_order_relaxed);
    atomic_store_explicit(&t->tolerance, tol, memory_order_relaxed);
    return PACER_OK;
}

void pacer_table_get_stats(pacer_table *t, pacer_table_stats *out)
{
    memset(out, 0, sizeof *out);
    out->capacity = t->capacity;
    for (uint32_t i = 0; i < t->nshards; i++) {
        struct shard *s = &t->shards[i];
        (void)pthread_mutex_lock(&s->mu);
        out->entries += s->count;
        out->inserts += s->inserts;
        out->removals += s->removals;
        out->rejections += s->rejections;
        (void)pthread_mutex_unlock(&s->mu);
    }
}

size_t pacer_table_memory(const pacer_table *t)
{
    return t->memory;
}
