/**
 * This file is part of the CernVM File System.
 */

#include <pthread.h>

#include <cassert>
#include <cstdio>

#include "gtest/gtest.h"
#include "tracer.h"
#include "util/posix.h"
#include "util/string.h"

using namespace std;  // NOLINT

namespace tracer {

class T_Tracer : public ::testing::Test {
 protected:
  virtual void SetUp() {
    trace_file_ = CreateTempPath("./cvmfs_ut_tracer", 0600);
    EXPECT_NE("", trace_file_);
  }

  virtual void TearDown() { unlink(trace_file_.c_str()); }

  unsigned GetNol() {
    FILE *f = fopen(trace_file_.c_str(), "r");
    assert(f != NULL);
    unsigned nol = 0;
    string line;
    while (GetLineFile(f, &line))
      nol++;
    fclose(f);
    return nol;
  }

  static void *ThreadLog(void *data) {
    StartData *sd = reinterpret_cast<StartData *>(data);
    for (unsigned i = 0; i < sd->iterations; ++i) {
      sd->tracer->Trace(Tracer::kEventOpen,
                        PathString(StringifyInt(sd->thread_id)),
                        "Multi-Thread test string containing quote chars: \"");
      if ((sd->flush_every > 0) && ((i % sd->flush_every) == 0)) {
        sd->tracer->Flush();
      }
    }
    return NULL;
  }

  struct StartData {
    StartData() : tracer(NULL), iterations(0), flush_every(0), thread_id(0) { }

    Tracer *tracer;
    unsigned iterations;
    unsigned flush_every;
    unsigned thread_id;
  };

  Tracer *tracer_;
  string trace_file_;
  pthread_t pthreads_[10];
  StartData inits_[10];
};


TEST_F(T_Tracer, InitNull) {
  tracer_ = new Tracer();
  tracer_->Spawn();  // should be a noop because Activate() wasn't called
  for (unsigned i = 0; i < 100; ++i)
    tracer_->Trace(Tracer::kEventOpen, PathString("id"), "Null");
  delete tracer_;
}


TEST_F(T_Tracer, CreateDestroy) {
  tracer_ = new Tracer();
  tracer_->Activate(5, 2, trace_file_);
  tracer_->Spawn();
  delete tracer_;
  EXPECT_EQ(2U, GetNol());
}


TEST_F(T_Tracer, SingleThreaded) {
  tracer_ = new Tracer();
  tracer_->Activate(2, 0, trace_file_);
  tracer_->Spawn();
  for (int i = 0; i < 100; ++i)
    tracer_->Trace(Tracer::kEventOpen, PathString("id"), "test string");
  delete tracer_;
  EXPECT_EQ(102U, GetNol());
}


TEST_F(T_Tracer, SingleThreadedManySlow) {
  tracer_ = new Tracer();
  tracer_->Activate(2, 1, trace_file_);
  tracer_->Spawn();
  for (int i = 0; i < 10000; ++i)
    tracer_->Trace(Tracer::kEventOpen, PathString("id"), "test string");
  delete tracer_;
  EXPECT_EQ(10002U, GetNol());
}


TEST_F(T_Tracer, SingleThreadedMany) {
  tracer_ = new Tracer();
  tracer_->Activate(2048, 1024, trace_file_);
  tracer_->Spawn();
  for (int i = 0; i < 100000; ++i)
    tracer_->Trace(Tracer::kEventOpen, PathString("id"), "test string");
  delete tracer_;
  EXPECT_EQ(100002U, GetNol());
}


TEST_F(T_Tracer, MultiThreadedTwo) {
  tracer_ = new Tracer();
  tracer_->Activate(2, 0, trace_file_);
  tracer_->Spawn();
  for (unsigned i = 0; i < 2; ++i) {
    inits_[i].tracer = tracer_;
    inits_[i].iterations = 100;
    inits_[i].thread_id = i;
    int retval = pthread_create(&pthreads_[i], NULL, ThreadLog,
                                reinterpret_cast<void *>(&inits_[i]));
    EXPECT_EQ(0, retval);
  }
  for (int i = 0; i < 2; i++) {
    pthread_join(pthreads_[i], NULL);
  }
  delete tracer_;
  EXPECT_EQ(202U, GetNol());
}


TEST_F(T_Tracer, MultiThreadedThree) {
  tracer_ = new Tracer();
  tracer_->Activate(2, 1, trace_file_);
  tracer_->Spawn();
  for (unsigned i = 0; i < 3; ++i) {
    inits_[i].tracer = tracer_;
    inits_[i].iterations = 100;
    inits_[i].thread_id = i;
    int retval = pthread_create(&pthreads_[i], NULL, ThreadLog,
                                reinterpret_cast<void *>(&inits_[i]));
    EXPECT_EQ(0, retval);
  }
  for (int i = 0; i < 3; i++) {
    pthread_join(pthreads_[i], NULL);
  }
  delete tracer_;
  EXPECT_EQ(302U, GetNol());
}


TEST_F(T_Tracer, MultiThreadedTenSlow) {
  tracer_ = new Tracer();
  tracer_->Activate(8, 6, trace_file_);
  tracer_->Spawn();
  for (unsigned i = 0; i < 10; ++i) {
    inits_[i].tracer = tracer_;
    inits_[i].iterations = 10000;
    inits_[i].thread_id = i;
    int retval = pthread_create(&pthreads_[i], NULL, ThreadLog,
                                reinterpret_cast<void *>(&inits_[i]));
    EXPECT_EQ(0, retval);
  }
  for (int i = 0; i < 10; i++) {
    pthread_join(pthreads_[i], NULL);
  }
  delete tracer_;
  EXPECT_EQ(100002U, GetNol());
}


TEST_F(T_Tracer, MultiThreadedTen) {
  tracer_ = new Tracer();
  tracer_->Activate(2048, 1024, trace_file_);
  tracer_->Spawn();
  for (unsigned i = 0; i < 10; ++i) {
    inits_[i].tracer = tracer_;
    inits_[i].iterations = 10000;
    inits_[i].thread_id = i;
    int retval = pthread_create(&pthreads_[i], NULL, ThreadLog,
                                reinterpret_cast<void *>(&inits_[i]));
    EXPECT_EQ(0, retval);
  }
  for (int i = 0; i < 10; i++) {
    pthread_join(pthreads_[i], NULL);
  }
  delete tracer_;
  EXPECT_EQ(100002U, GetNol());
}


TEST_F(T_Tracer, ThrashingTwoSlow) {
  tracer_ = new Tracer();
  tracer_->Activate(2, 0, trace_file_);
  tracer_->Spawn();
  for (unsigned i = 0; i < 2; ++i) {
    inits_[i].tracer = tracer_;
    inits_[i].iterations = 100;
    inits_[i].thread_id = i;
  }
  for (unsigned j = 0; j < 100; ++j) {
    for (unsigned i = 0; i < 2; i++)
      pthread_create(&pthreads_[i], NULL, ThreadLog,
                     reinterpret_cast<void *>(&inits_[i]));
    for (int i = 0; i < 2; i++)
      pthread_join(pthreads_[i], NULL);
  }
  delete tracer_;
  EXPECT_EQ(20002U, GetNol());
}


TEST_F(T_Tracer, ThrashingThreeSlow) {
  tracer_ = new Tracer();
  tracer_->Activate(2, 1, trace_file_);
  tracer_->Spawn();
  for (unsigned i = 0; i < 3; ++i) {
    inits_[i].tracer = tracer_;
    inits_[i].iterations = 100;
    inits_[i].thread_id = i;
  }
  for (unsigned j = 0; j < 100; ++j) {
    for (unsigned i = 0; i < 3; i++)
      pthread_create(&pthreads_[i], NULL, ThreadLog,
                     reinterpret_cast<void *>(&inits_[i]));
    for (int i = 0; i < 3; i++)
      pthread_join(pthreads_[i], NULL);
  }
  delete tracer_;
  EXPECT_EQ(30002U, GetNol());
}


TEST_F(T_Tracer, Flush) {
  tracer_ = new Tracer();
  tracer_->Activate(5, 2, trace_file_);
  tracer_->Spawn();
  tracer_->Flush();
  delete tracer_;
  EXPECT_EQ(3U, GetNol());
}


TEST_F(T_Tracer, FlushTwo) {
  tracer_ = new Tracer();
  tracer_->Activate(2, 0, trace_file_);
  tracer_->Spawn();
  for (unsigned i = 0; i < 2; ++i) {
    inits_[i].tracer = tracer_;
    inits_[i].iterations = 100;
    inits_[i].flush_every = 1;
    inits_[i].thread_id = i;
    int retval = pthread_create(&pthreads_[i], NULL, ThreadLog,
                                reinterpret_cast<void *>(&inits_[i]));
    EXPECT_EQ(0, retval);
  }
  for (int i = 0; i < 2; i++) {
    pthread_join(pthreads_[i], NULL);
  }
  delete tracer_;
  EXPECT_EQ(402U, GetNol());
}


TEST_F(T_Tracer, FlushThree) {
  tracer_ = new Tracer();
  tracer_->Activate(2, 1, trace_file_);
  tracer_->Spawn();
  for (unsigned i = 0; i < 3; ++i) {
    inits_[i].tracer = tracer_;
    inits_[i].iterations = 100;
    inits_[i].flush_every = 1;
    inits_[i].thread_id = i;
    int retval = pthread_create(&pthreads_[i], NULL, ThreadLog,
                                reinterpret_cast<void *>(&inits_[i]));
    EXPECT_EQ(0, retval);
  }
  for (int i = 0; i < 3; i++) {
    pthread_join(pthreads_[i], NULL);
  }
  delete tracer_;
  EXPECT_EQ(602U, GetNol());
}


TEST_F(T_Tracer, FlushTen) {
  tracer_ = new Tracer();
  tracer_->Activate(64, 32, trace_file_);
  tracer_->Spawn();
  for (unsigned i = 0; i < 10; ++i) {
    inits_[i].tracer = tracer_;
    inits_[i].iterations = 1000;
    inits_[i].flush_every = 10;
    inits_[i].thread_id = i;
    int retval = pthread_create(&pthreads_[i], NULL, ThreadLog,
                                reinterpret_cast<void *>(&inits_[i]));
    EXPECT_EQ(0, retval);
  }
  for (int i = 0; i < 10; i++) {
    pthread_join(pthreads_[i], NULL);
  }
  delete tracer_;
  EXPECT_EQ(11002U, GetNol());
}

}  // namespace tracer
