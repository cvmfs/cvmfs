#ifndef ATOMIC_H
#define ATOMIC_H 1

#include <stdint.h>

typedef int32_t atomic_int;
typedef int64_t atomic_int64;

static void inline __attribute__((used)) atomic_init(atomic_int *a) {
   *a = 0;
}

static void inline __attribute__((used)) atomic_init64(atomic_int64 *a) {
   *a = 0;
}


static int32_t __attribute__((used)) atomic_read(atomic_int *a) {
   /*int32_t result;
    __asm__ __volatile__("mfence; movl %1, %%eax; mfence"
    :"=a" (result)
    :"m" (*a));
    return result;*/
   return __sync_fetch_and_add(a, 0);
}

static int64_t __attribute__((used)) atomic_read64(atomic_int64 *a) {
   return __sync_fetch_and_add(a, 0);
}


static void inline __attribute__((used)) atomic_inc(atomic_int *a) {
   /*__asm__ __volatile__("mfence; lock; incl %0; mfence"
    :"=m" (*a)
    :"m" (*a));*/
   (void) __sync_fetch_and_add(a, 1);
}

static void inline __attribute__((used)) atomic_inc64(atomic_int64 *a) {
   (void) __sync_fetch_and_add(a, 1);
}

static void inline __attribute__((used)) atomic_dec(atomic_int *a) {
   /*__asm__ __volatile__("mfence; lock; decl %0; mfence"
    :"=m" (*a)
    :"m" (*a));*/
  (void) __sync_fetch_and_add(a, -1);
}

static int32_t inline __attribute__((used)) atomic_xadd(atomic_int *a, int32_t offset) {
   /*int32_t oldval;
    __asm__ __volatile__("mfence; lock; xadd %%eax, %2; mfence"
    :"=a" (oldval)
    :"a" (offset), "m" (*a));
    return oldval;*/
   return __sync_fetch_and_add(a, offset);
}

static int64_t inline __attribute__((used)) atomic_xadd64(atomic_int64 *a, int64_t offset) {
   return __sync_fetch_and_add(a, offset);
}

static int32_t inline __attribute__((used)) atomic_cas(atomic_int *a, int32_t cmp, int32_t newval) {
   /*int32_t result;
    __asm__ __volatile__("mfence; lock; cmpxchg %%edx, %3; mfence"
    :"=a" (result)
    :"a" (cmp), "d" (newval), "m" (*a));
    return result == cmp;*/
   return __sync_bool_compare_and_swap(a, cmp, newval);
}

#endif
