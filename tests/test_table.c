#include "pacer.h"
#include "test.h"

#include <stdio.h>
#include <string.h>

#define SEC 1000000000ull

static const uint8_t fixed_key[16] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};

static pacer_table *make_table(uint32_t max_entries, uint32_t shards, uint64_t burst)
{
    pacer_table_config cfg = {max_entries, shards, {10, SEC, burst}, fixed_key};
    pacer_table *t = NULL;
    CHECK_EQ_INT(pacer_table_create(&cfg, &t), PACER_OK);
    return t;
}

static size_t key_name(char *buf, size_t cap, unsigned i)
{
    return (size_t)snprintf(buf, cap, "10.0.%u.%u", i / 256, i % 256);
}

TEST(table_rejects_bad_config)
{
    pacer_table *t = NULL;
    pacer_table_config cfg = {0, 0, {10, SEC, 1}, NULL};
    CHECK_EQ_INT(pacer_table_create(&cfg, &t), PACER_EINVAL);
    cfg.max_entries = 100;
    cfg.shards = 3;
    CHECK_EQ_INT(pacer_table_create(&cfg, &t), PACER_EINVAL);
    cfg.shards = 128;
    CHECK_EQ_INT(pacer_table_create(&cfg, &t), PACER_EINVAL);
    cfg.shards = 0;
    cfg.rate.tokens = 0;
    CHECK_EQ_INT(pacer_table_create(&cfg, &t), PACER_EINVAL);
    cfg.rate.tokens = 1;
    cfg.max_entries = (1u << 28) + 1;
    CHECK_EQ_INT(pacer_table_create(&cfg, &t), PACER_EINVAL);
    CHECK_EQ_INT(pacer_table_create(NULL, &t), PACER_EINVAL);
    CHECK_EQ_INT(pacer_table_create(&cfg, NULL), PACER_EINVAL);
    CHECK(t == NULL);
    pacer_table_destroy(NULL);
}

TEST(table_keys_are_independent)
{
    pacer_table *t = make_table(1000, 0, 2);
    CHECK_EQ_INT(pacer_table_take(t, "a", 1, SEC, 2, NULL), 1);
    CHECK_EQ_INT(pacer_table_take(t, "a", 1, SEC, 1, NULL), 0);
    CHECK_EQ_INT(pacer_table_take(t, "b", 1, SEC, 1, NULL), 1);
    CHECK_EQ_INT(pacer_table_take(t, "ab", 2, SEC, 2, NULL), 1);
    pacer_decision d;
    CHECK_EQ_INT(pacer_table_take(t, "a", 1, SEC, 1, &d), 0);
    CHECK_EQ_U64(d.retry_after_ns, SEC / 10);
    CHECK_EQ_INT(pacer_table_take(t, "a", 1, SEC + SEC / 10, 1, NULL), 1);
    pacer_table_stats st;
    pacer_table_get_stats(t, &st);
    CHECK_EQ_U64(st.entries, 3);
    CHECK_EQ_U64(st.inserts, 3);
    CHECK_EQ_U64(st.capacity, 1000);
    pacer_table_destroy(t);
}

TEST(table_rejects_bad_keys_and_costs)
{
    pacer_table *t = make_table(100, 0, 2);
    char long_key[PACER_KEY_MAX + 1];
    memset(long_key, 'k', sizeof long_key);
    pacer_decision d;
    CHECK_EQ_INT(pacer_table_take(t, "", 0, SEC, 1, NULL), PACER_EINVAL);
    CHECK_EQ_INT(pacer_table_take(t, NULL, 4, SEC, 1, NULL), PACER_EINVAL);
    CHECK_EQ_INT(pacer_table_take(t, long_key, sizeof long_key, SEC, 1, NULL), PACER_EINVAL);
    CHECK_EQ_INT(pacer_table_take(t, long_key, PACER_KEY_MAX, SEC, 1, NULL), 1);
    CHECK_EQ_INT(pacer_table_take(t, "a", 1, SEC, 3, NULL), PACER_ERANGE);
    CHECK_EQ_INT(pacer_table_take_upto(t, "", 0, SEC, 1, &d), PACER_EINVAL);
    CHECK_EQ_INT(pacer_table_remove(t, "", 0), PACER_EINVAL);
    pacer_table_refund(t, "", 0, SEC, 1);
    pacer_table_destroy(t);
}

TEST(table_full_refuses_new_keys)
{
    pacer_table *t = make_table(12, 1, 1);
    char k[32];
    for (unsigned i = 0; i < 12; i++) {
        size_t n = key_name(k, sizeof k, i);
        CHECK_EQ_INT(pacer_table_take(t, k, n, SEC, 1, NULL), 1);
    }
    size_t n = key_name(k, sizeof k, 99);
    CHECK_EQ_INT(pacer_table_take(t, k, n, SEC, 1, NULL), PACER_EFULL);
    pacer_decision d;
    CHECK_EQ_INT(pacer_table_take_upto(t, k, n, SEC, 1, &d), PACER_EFULL);
    pacer_table_stats st;
    pacer_table_get_stats(t, &st);
    CHECK_EQ_U64(st.entries, 12);
    CHECK_EQ_U64(st.rejections, 2);
    /* Existing keys still work when the table is full. */
    n = key_name(k, sizeof k, 3);
    CHECK_EQ_INT(pacer_table_take(t, k, n, 2 * SEC, 1, NULL), 1);
    pacer_table_destroy(t);
}

/* Each key takes its whole burst, so a later take tells whether the entry
 * is still there (denied) or was lost (admitted fresh). */
static int key_present(pacer_table *t, unsigned i)
{
    char k[32];
    size_t n = key_name(k, sizeof k, i);
    return pacer_table_take(t, k, n, SEC, 1, NULL) == 0;
}

TEST(table_delete_keeps_probe_chains_intact)
{
    /* 48 keys in 64 slots of one shard produce long probe chains. Remove keys
     * in a scrambled order and check every survivor after each removal. */
    const unsigned n = 48;
    pacer_table *t = make_table(n, 1, 1);
    char k[32];
    for (unsigned i = 0; i < n; i++) {
        size_t len = key_name(k, sizeof k, i);
        CHECK_EQ_INT(pacer_table_take(t, k, len, SEC, 1, NULL), 1);
    }
    unsigned removed[48] = {0};
    for (unsigned step = 0; step < n; step++) {
        unsigned victim = (step * 29 + 7) % n;
        size_t len = key_name(k, sizeof k, victim);
        CHECK_EQ_INT(pacer_table_remove(t, k, len), PACER_OK);
        CHECK_EQ_INT(pacer_table_remove(t, k, len), PACER_ENOENT);
        removed[victim] = 1;
        unsigned lost = 0;
        for (unsigned i = 0; i < n; i++) {
            lost += !removed[i] && !key_present(t, i);
        }
        CHECK_EQ_U64(lost, 0);
    }
    pacer_table_stats st;
    pacer_table_get_stats(t, &st);
    CHECK_EQ_U64(st.entries, 0);
    CHECK_EQ_U64(st.removals, n);
    pacer_table_destroy(t);
}

TEST(table_take_upto_and_refund)
{
    pacer_table *t = make_table(100, 0, 100);
    pacer_decision d;
    CHECK_EQ_INT(pacer_table_take_upto(t, "k", 1, SEC, 70, &d), PACER_OK);
    CHECK_EQ_U64(d.granted, 70);
    CHECK_EQ_INT(pacer_table_take_upto(t, "k", 1, SEC, 70, &d), PACER_OK);
    CHECK_EQ_U64(d.granted, 30);
    CHECK_EQ_INT(pacer_table_take_upto(t, "k", 1, SEC, 70, &d), PACER_OK);
    CHECK_EQ_U64(d.granted, 0);
    CHECK_EQ_U64(d.retry_after_ns, SEC / 10);
    pacer_table_refund(t, "k", 1, SEC, 20);
    CHECK_EQ_INT(pacer_table_take_upto(t, "k", 1, SEC, 70, &d), PACER_OK);
    CHECK_EQ_U64(d.granted, 20);
    /* Refunding an unknown key is harmless. */
    pacer_table_refund(t, "zz", 2, SEC, 20);
    pacer_table_stats st;
    pacer_table_get_stats(t, &st);
    CHECK_EQ_U64(st.entries, 1);
    pacer_table_destroy(t);
}

TEST(table_reconfigure_applies_to_every_key)
{
    pacer_table *t = make_table(100, 0, 1);
    CHECK_EQ_INT(pacer_table_take(t, "a", 1, SEC, 1, NULL), 1);
    pacer_rate r = {10, SEC, 5};
    CHECK_EQ_INT(pacer_table_reconfigure(t, &r), PACER_OK);
    CHECK_EQ_INT(pacer_table_take(t, "b", 1, SEC, 5, NULL), 1);
    CHECK_EQ_INT(pacer_table_take(t, "a", 1, SEC, 4, NULL), 1);
    pacer_rate bad = {0, SEC, 5};
    CHECK_EQ_INT(pacer_table_reconfigure(t, &bad), PACER_EINVAL);
    pacer_table_destroy(t);
}

TEST(table_memory_is_fixed_at_create)
{
    pacer_table *t = make_table(65536, 0, 1);
    size_t before = pacer_table_memory(t);
    /* 65536 keys at 75% load need 2048 slots in each of 64 shards. */
    CHECK(before >= (size_t)64 * 2048 * 64);
    CHECK(before < (size_t)64 * 2048 * 64 + (size_t)64 * 1024);
    char k[32];
    for (unsigned i = 0; i < 70000; i++) {
        size_t n = key_name(k, sizeof k, i);
        (void)pacer_table_take(t, k, n, SEC, 1, NULL);
    }
    CHECK_EQ_U64(pacer_table_memory(t), before);
    pacer_table_stats st;
    pacer_table_get_stats(t, &st);
    CHECK(st.entries <= 65536);
    CHECK_EQ_U64(st.entries + st.rejections, 70000);
    pacer_table_destroy(t);
}

TEST(table_random_hash_key)
{
    pacer_table_config cfg = {10, 0, {10, SEC, 1}, NULL};
    pacer_table *t = NULL;
    CHECK_EQ_INT(pacer_table_create(&cfg, &t), PACER_OK);
    CHECK_EQ_INT(pacer_table_take(t, "a", 1, SEC, 1, NULL), 1);
    CHECK_EQ_INT(pacer_table_take(t, "a", 1, SEC, 1, NULL), 0);
    pacer_table_destroy(t);
}
