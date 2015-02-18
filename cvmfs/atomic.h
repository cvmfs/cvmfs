/**
 * This file is part of the CernVM File System.
 *
 * Defines wrapper functions for atomic integer operations.  Atomic operations
 * are handled by GCC.
 */

#ifndef CVMFS_ATOMIC_H_
#define CVMFS_ATOMIC_H_

#include <stdint.h>

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

typedef int32_t atomic_int32;
typedef int64_t atomic_int64;


static void inline __attribute__((used)) atomic_init32(atomic_int32 *a) {
  *a = 0;
}


static void inline __attribute__((used)) atomic_init64(atomic_int64 *a) {
  *a = 0;
}


static int32_t inline __attribute__((used)) atomic_read32(atomic_int32 *a) {
  return __sync_fetch_and_add(a, 0);
}


static int64_t inline __attribute__((used)) atomic_read64(atomic_int64 *a) {
  return __sync_fetch_and_add(a, 0);
}


static void inline __attribute__((used)) atomic_write32(
  atomic_int32  *a,
  int32_t        value)
{
  while (!__sync_bool_compare_and_swap(a, atomic_read32(a), value)) { }
}


static void inline __attribute__((used)) atomic_write64(
  atomic_int64  *a,
  int64_t       value)
{
  while (!__sync_bool_compare_and_swap(a, atomic_read64(a), value)) { }
}


static void inline __attribute__((used)) atomic_inc32(atomic_int32 *a) {
  (void) __sync_fetch_and_add(a, 1);
}


static void inline __attribute__((used)) atomic_inc64(atomic_int64 *a) {
  (void) __sync_fetch_and_add(a, 1);
}


static void inline __attribute__((used)) atomic_dec32(atomic_int32 *a) {
  (void) __sync_fetch_and_sub(a, 1);
}

static void inline __attribute__((used)) atomic_dec64(atomic_int64 *a) {
  (void) __sync_fetch_and_sub(a, 1);
}

static int32_t inline __attribute__((used)) atomic_xadd32(
  atomic_int32 *a,
  int32_t offset)
{
  if (offset < 0)
    return __sync_fetch_and_sub(a, -offset);
  return __sync_fetch_and_add(a, offset);
}


static int64_t inline __attribute__((used)) atomic_xadd64(
  atomic_int64 *a,
  int64_t offset)
{
  if (offset < 0)
    return __sync_fetch_and_sub(a, -offset);
  return __sync_fetch_and_add(a, offset);
}


static int32_t inline __attribute__((used)) atomic_cas32(
  atomic_int32 *a,
  int32_t cmp,
  int32_t newval)
{
  return __sync_bool_compare_and_swap(a, cmp, newval);
}

#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif

#endif  // CVMFS_ATOMIC_H_
