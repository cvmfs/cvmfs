/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include "supervisor.h"

namespace {

const int kMaxRuns = 3;
const int kDelay = 2;

}  // namespace

class TestSupervisor : public Supervisor {
 public:
  TestSupervisor(int num_retries, uint64_t interval_sec)
      : Supervisor(num_retries, interval_sec)
      , delay_(false)
      , result_(true)
      , runs_(0) { }
  virtual ~TestSupervisor() { }

  virtual bool Task() {
    if (runs_ < kMaxRuns) {
      if (delay_) {
        sleep(kDelay);
      }
      runs_ += 1;
      return result_;
    }

    return true;
  }

  bool delay_;
  bool result_;
  int runs_;
  int max_runs_;
};

class T_Supervisor : public ::testing::Test { };

TEST_F(T_Supervisor, kNoRetryForSuccess) {
  TestSupervisor r(2, 1);
  r.Run();
  ASSERT_EQ(1, r.runs_);
}

TEST_F(T_Supervisor, kMaxRetriesGiveUp) {
  TestSupervisor r(2, 1);
  r.result_ = false;
  r.Run();
  ASSERT_EQ(3, r.runs_);
}

TEST_F(T_Supervisor, kCooldownSlow) {
  TestSupervisor r(2, 1);
  r.result_ = false;
  r.delay_ = true;
  r.Run();
  ASSERT_EQ(kMaxRuns, r.runs_);
}
