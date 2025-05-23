/**
 * This file is part of the CernVM File System.
 */
#ifndef TEST_MICRO_BENCHMARKS_BM_UTIL_H_
#define TEST_MICRO_BENCHMARKS_BM_UTIL_H_

/**
 * Probably the same as benchmark::DoNotOptimize
 */
inline static void Escape(void *p) { asm volatile("" : : "g"(p) : "memory"); }

/**
 * Tell the optimizer that after this command, everything in the memory could
 * have changed.
 */
inline static void ClobberMemory() { asm volatile("" : : : "memory"); }

#endif  // TEST_MICRO_BENCHMARKS_BM_UTIL_H_
