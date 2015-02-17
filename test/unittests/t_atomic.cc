#include <gtest/gtest.h>

#include <pthread.h>

#include "../../cvmfs/atomic.h"

class T_Atomic : public ::testing::Test {
 protected:
  virtual void SetUp() {
    atomic_init32(&atomic32_);
    atomic_init64(&atomic64_);

    ASSERT_EQ (0, atomic32_);
    ASSERT_EQ (0, atomic64_);
  }


  static void *concurrent_writer32(void *atomic) {
    const int cycles = T_Atomic::concurrent_writer_cycles;

    atomic_int32 *atomic32 = static_cast<atomic_int32*>(atomic);
    for (int i = 0; i < cycles; ++i) {
      if (i % 5 == 0) {
        atomic_dec32(atomic32);
      } else {
        atomic_inc32(atomic32);
      }
    }

    return atomic;
  }


  static void *concurrent_writer64(void *atomic) {
    const int cycles = T_Atomic::concurrent_writer_cycles;

    atomic_int64 *atomic32 = static_cast<atomic_int64*>(atomic);
    for (int i = 0; i < cycles; ++i) {
      if (i % 5 == 0) {
        atomic_dec64(atomic32);
      } else {
        atomic_inc64(atomic32);
      }
    }

    return atomic;
  }


  static void *concurrent_assigner32(void *atomic) {
    const int cycles = T_Atomic::concurrent_writer_cycles;

    atomic_int32 *atomic32 = static_cast<atomic_int32*>(atomic);
    for (int i = 0; i < cycles; ++i) {
      atomic_write32(atomic32, static_cast<int32_t>(i));
    }

    return atomic;
  }


  static void *concurrent_assigner64(void *atomic) {
    const int cycles = T_Atomic::concurrent_writer_cycles;

    atomic_int64 *atomic64 = static_cast<atomic_int64*>(atomic);
    for (int i = 0; i < cycles; ++i) {
      atomic_write64(atomic64, static_cast<int64_t>(i));
    }

    return atomic;
  }


 protected:
  atomic_int32 atomic32_;
  atomic_int64 atomic64_;

  static const int concurrent_writer_cycles = 1000000;
  static const int concurrent_writer_result = 600000;
};


TEST_F(T_Atomic, InitialReadAtomicInts) {
  const int32_t i32 = atomic_read32(&atomic32_);
  const int64_t i64 = atomic_read64(&atomic64_);

  EXPECT_EQ (0, i32);
  EXPECT_EQ (0, i64);
}


TEST_F(T_Atomic, IncrementAtomicInts) {
  const int cycles = 100;

  int32_t i32;
  int64_t i64;

  for (int i = 1; i < cycles; ++i) {
    atomic_inc32(&atomic32_);
    atomic_inc64(&atomic64_);

    i32 = atomic_read32(&atomic32_);
    i64 = atomic_read64(&atomic64_);

    EXPECT_EQ (i, i32);
    EXPECT_EQ (i, i64);
  }
}


TEST_F(T_Atomic, AddToAtomicInts) {
  const int32_t off1 = 1337;
  const int32_t off2 = 42;

  atomic_xadd32(&atomic32_, off1);
  atomic_xadd64(&atomic64_, off2);

  const int32_t i32 = atomic_read32(&atomic32_);
  const int64_t i64 = atomic_read64(&atomic64_);

  EXPECT_EQ (off1, i32);
  EXPECT_EQ (off2, i64);
}


TEST_F(T_Atomic, SubtractFromAtomicInts) {
  const int32_t off1 = 1337;
  const int32_t off2 = 42;

  atomic_xadd32(&atomic32_, off1);
  atomic_xadd64(&atomic64_, off2);

  atomic_xadd32(&atomic32_, -off2);
  atomic_xadd64(&atomic64_, -off1);

  const int32_t i32 = atomic_read32(&atomic32_);
  const int64_t i64 = atomic_read64(&atomic64_);

  EXPECT_EQ (off1 - off2, i32);
  EXPECT_EQ (off2 - off1, i64);
}


TEST_F(T_Atomic, DecrementAtomicInts) {
  const int cycles = 100;

  atomic_xadd32(&atomic32_, cycles + 1);
  atomic_xadd64(&atomic64_, cycles + 1);

  int32_t i32;
  int64_t i64;

  for (int i = cycles; i > 0; --i) {
    atomic_dec32(&atomic32_);
    atomic_dec64(&atomic64_);

    i32 = atomic_read32(&atomic32_);
    i64 = atomic_read64(&atomic64_);

    EXPECT_EQ (i, i32);
    EXPECT_EQ (i, i64);
  }
}


TEST_F(T_Atomic, CompareAndSetAtomicInts) {
  const int32_t off1 = 31415;
  const int32_t off2 = 2;
  const int32_t off3 = 217;

  atomic_xadd32(&atomic32_, off1);

  const int32_t res1   = atomic_cas32(&atomic32_, off2, off3);
  const int32_t value1 = atomic_read32(&atomic32_);
  EXPECT_FALSE (res1);
  EXPECT_EQ (off1, value1);

  const int32_t res2   = atomic_cas32(&atomic32_, off1, off3);
  const int32_t value2 = atomic_read32(&atomic32_);
  EXPECT_TRUE (res2);
  EXPECT_EQ (off3, value2);
}


TEST_F(T_Atomic, TransactionalAssignment) {
  const int32_t value1 = 1337;
  const int32_t value2 = 42;
  const int32_t value3 = 128;

  const int64_t value4 = 1247623;
  const int64_t value5 = 53847432;
  const int64_t value6 = 0xFFFFFFFF;

  atomic_write32(&atomic32_, value1);
  EXPECT_EQ (value1, atomic_read32(&atomic32_));

  atomic_write32(&atomic32_, value2);
  EXPECT_EQ (value2, atomic_read32(&atomic32_));

  atomic_write32(&atomic32_, value3);
  EXPECT_EQ (value3, atomic_read32(&atomic32_));

  atomic_write64(&atomic64_, value4);
  EXPECT_EQ (value4, atomic_read64(&atomic64_));

  atomic_write64(&atomic64_, value5);
  EXPECT_EQ (value5, atomic_read64(&atomic64_));

  atomic_write64(&atomic64_, value6);
  EXPECT_EQ (value6, atomic_read64(&atomic64_));
}


TEST_F(T_Atomic, ConcurrentTransactionalAssignmentsSlow) {
  const int pthreads = 20;

  pthread_t threads32[pthreads];
  pthread_t threads64[pthreads];

  int pthread_result;

  for (int i = 0; i < pthreads; ++i) {
    pthread_result = pthread_create(&threads32[i],
                                     NULL,
                                    &T_Atomic::concurrent_assigner32,
                                     static_cast<void*>(&atomic32_));
    ASSERT_EQ(0, pthread_result);
  }

  for (int i = 0; i < pthreads; ++i) {
    pthread_join(threads32[i], NULL);
  }

  const int32_t result32 = atomic_read32(&atomic32_);
  EXPECT_EQ (T_Atomic::concurrent_writer_cycles - 1, result32);

  // ----

  for (int i = 0; i < pthreads; ++i) {
    pthread_result = pthread_create(&threads64[i],
                                     NULL,
                                    &T_Atomic::concurrent_assigner64,
                                     static_cast<void*>(&atomic64_));
    ASSERT_EQ(0, pthread_result);
  }

  for (int i = 0; i < pthreads; ++i) {
    pthread_join(threads64[i], NULL);
  }

  const int64_t result64 = atomic_read64(&atomic64_);
  EXPECT_EQ (T_Atomic::concurrent_writer_cycles - 1, result64);
}


TEST_F(T_Atomic, ConcurrentWriteOfAtomicIntsSlow) {
  const int pthreads = 100;

  pthread_t threads32[pthreads];
  pthread_t threads64[pthreads];

  int pthread_result;

  for (int i = 0; i < pthreads; ++i) {
    pthread_result = pthread_create(&threads32[i],
                                     NULL,
                                    &T_Atomic::concurrent_writer32,
                                     static_cast<void*>(&atomic32_));
    ASSERT_EQ(0, pthread_result);
  }

  for (int i = 0; i < pthreads; ++i) {
    pthread_join(threads32[i], NULL);
  }

  const int32_t result32 = atomic_read32(&atomic32_);
  EXPECT_EQ (T_Atomic::concurrent_writer_result * pthreads, result32);

  for (int i = 0; i < pthreads; ++i) {
    pthread_result = pthread_create(&threads64[i],
                                     NULL,
                                    &T_Atomic::concurrent_writer64,
                                     static_cast<void*>(&atomic64_));
    ASSERT_EQ(0, pthread_result);
  }

  for (int i = 0; i < pthreads; ++i) {
    pthread_join(threads64[i], NULL);
  }

  const int64_t result64 = atomic_read64(&atomic64_);
  EXPECT_EQ (T_Atomic::concurrent_writer_result * pthreads, result64);
}
