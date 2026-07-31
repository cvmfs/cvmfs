#include <atomic>
#include <benchmark/benchmark.h>
#include "util/atomic.h"  // Your in-house atomic header

// In-house atomics
static void BM_InHouse_AtomicInc32(benchmark::State& state) {
    atomic_int32 a;
    atomic_init32(&a);
    for (auto _ : state) {
        atomic_inc32(&a);
    }
}
BENCHMARK(BM_InHouse_AtomicInc32);

static void BM_InHouse_AtomicXadd32(benchmark::State& state) {
    atomic_int32 a;
    atomic_init32(&a);
    for (auto _ : state) {
        atomic_xadd32(&a, 1);
    }
}
BENCHMARK(BM_InHouse_AtomicXadd32);

// std::atomic
static void BM_Std_AtomicInc32(benchmark::State& state) {
    std::atomic<int32_t> a(0);
    for (auto _ : state) {
        a++;
    }
}
BENCHMARK(BM_Std_AtomicInc32);

static void BM_Std_AtomicFetchAdd32(benchmark::State& state) {
    std::atomic<int32_t> a(0);
    for (auto _ : state) {
        a.fetch_add(1);
    }
}
BENCHMARK(BM_Std_AtomicFetchAdd32);

// 64-bit versions
static void BM_InHouse_AtomicInc64(benchmark::State& state) {
    atomic_int64 a;
    atomic_init64(&a);
    for (auto _ : state) {
        atomic_inc64(&a);
    }
}
BENCHMARK(BM_InHouse_AtomicInc64);

static void BM_Std_AtomicInc64(benchmark::State& state) {
    std::atomic<int64_t> a(0);
    for (auto _ : state) {
        a++;
    }
}
BENCHMARK(BM_Std_AtomicInc64);

// CAS operations
static void BM_InHouse_AtomicCas32(benchmark::State& state) {
    atomic_int32 a;
    atomic_init32(&a);
    for (auto _ : state) {
        atomic_cas32(&a, 0, 1);
    }
}
BENCHMARK(BM_InHouse_AtomicCas32);

static void BM_Std_AtomicCas32(benchmark::State& state) {
    std::atomic<int32_t> a(42);
    for (auto _ : state) {
        int32_t expected = a.load();
        a.compare_exchange_strong(expected, 1);
    }
}
BENCHMARK(BM_Std_AtomicCas32);
