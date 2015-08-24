/**
 * This file is part of the CernVM File System.
 */

#ifndef TEST_UNITTESTS_ENV_H_
#define TEST_UNITTESTS_ENV_H_

#include <gtest/gtest.h>

/**
 * This class manages the unit test file system sandbox in /tmp. Unittests that
 * need to create or interact with files are supposed to do that in the current
 * working directory.
 *
 * Furthermore this class detects if it runs in a re-spawned death test process
 * and does not re-create a sandbox in that case. Obviously such death test
 * processes cannot clean up after themselves, leaving behind leaked temp files.
 */
class CvmfsEnvironment : public ::testing::Environment {
 private:
  static const size_t kMaxPathLength;

 public:
  CvmfsEnvironment(const int argc, char **argv);

  virtual void SetUp();
  virtual void TearDown();

 protected:
  /**
   * Detects if it is running inside a re-spawned death test process by looking
   * for gtest's internal command line flag --gtest_internal_run_death_test
   *
   * @return true   if it runs inside a death test process
   */
  static bool IsDeathTestExecution(const int argc, char **argv);

 private:
  const bool   owns_sandbox_;
  std::string  sandbox_;
};

#endif  /* TEST_UNITTESTS_ENV_H_ */
