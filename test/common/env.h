/**
 * This file is part of the CernVM File System.
 */

#ifndef TEST_UNITTESTS_ENV_H_
#define TEST_UNITTESTS_ENV_H_

#include <gtest/gtest.h>

#include <string>

/**
 * This class manages the unit test file system sandbox in /tmp. Unittests that
 * need to create or interact with files are supposed to do that in the current
 * working directory. It creates a pointer file to advertise the created sand-
 * box to child processes spawned to run death tests.
 *
 * Furthermore this class detects if it runs in a re-spawned death test process
 * and does not re-create a sandbox in that case. Obviously such death test
 * processes cannot clean up after themselves, leaving behind leaked temp files.
 * In such cases the pointer file of the parent process is read and its sandbox
 * directory is re-used.
 */
class CvmfsEnvironment : public ::testing::Environment {
 private:
  static const char *kSandboxEnvVariable;

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
  void ChangeDirectoryToSandbox() const;

  void CreateSandbox();
  void AdoptSandboxFromParent();

  void RemoveSandbox();

 private:
  const bool is_death_test_execution_;
  std::string sandbox_;
};

#endif /* TEST_UNITTESTS_ENV_H_ */
