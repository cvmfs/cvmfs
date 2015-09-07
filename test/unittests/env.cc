/**
 * This file is part of the CernVM File System.
 */

#include "env.h"

#include <cassert>
#include <cerrno>
#include <cstdlib>

#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

#include "../../cvmfs/util.h"


const char* CvmfsEnvironment::kSandboxEnvVariable = "CVMFS_UT_SANDBOX";


CvmfsEnvironment::CvmfsEnvironment(const int argc, char **argv)
  : is_death_test_execution_(CvmfsEnvironment::IsDeathTestExecution(argc, argv))
  { }


bool CvmfsEnvironment::IsDeathTestExecution(const int argc, char **argv) {
  const char* death_test_flag = "--gtest_internal_run_death_test";

  for (int i = 0; i < argc; ++i) {
    if (strncmp(argv[i], death_test_flag, strlen(death_test_flag)) == 0) {
      return true;
    }
  }

  return false;
}


void CvmfsEnvironment::SetUp() {
  assert(sandbox_.empty());

  if (!is_death_test_execution_) {
    CreateSandbox();
  } else {
    AdoptSandboxFromParent();
  }

  assert(!sandbox_.empty());
  ChangeDirectoryToSandbox();
}


void CvmfsEnvironment::TearDown() {
  if (!is_death_test_execution_) {
    RemoveSandbox();
  }
}


void CvmfsEnvironment::CreateSandbox() {
  assert(sandbox_.empty());

  // create sandbox directory
  const std::string sandbox = CreateTempDir("/tmp/cvmfs_ut_sandbox");
  if (sandbox.empty()) {
    std::cerr << "Unittest Setup: Failed to create sandbox directory in /tmp "
              << " (errno: " << errno << ")."
              << std::endl;
    abort();
  }
  sandbox_ = sandbox;

  // put sandbox path into the environment to be picked up by child processes
  unsetenv(kSandboxEnvVariable);
  if (setenv(kSandboxEnvVariable, sandbox_.c_str(), 1) != 0) {
    std::cerr << "Unittest Setup: Failed to append sandbox path to environment "
              << "'" << kSandboxEnvVariable << "' (errno: " << errno << ")."
              << std::endl;
    abort();
  }
}


void CvmfsEnvironment::AdoptSandboxFromParent() {
  assert(sandbox_.empty());

  const char *sandbox = getenv(kSandboxEnvVariable);
  if (NULL == sandbox) {
    std::cerr << "Unittest Setup: Failed to read sandbox path from environment "
              << "'" << kSandboxEnvVariable << "' (errno: " << errno << ")."
              << std::endl;
    abort();
  }

  if (!DirectoryExists(sandbox)) {
    std::cerr << "Unittest Setup: Failed to find sandbox directory "
              << "'" << sandbox << "' pointed to by "
              << "'" << kSandboxEnvVariable << "'"
              << std::endl;
    abort();
  }

  sandbox_ = sandbox;
}


void CvmfsEnvironment::RemoveSandbox() {
  assert(!sandbox_.empty());
  unsetenv(kSandboxEnvVariable);
  RemoveTree(sandbox_);
  sandbox_ = "";
}


void CvmfsEnvironment::ChangeDirectoryToSandbox() const {
  assert(!sandbox_.empty());

  if (chdir(sandbox_.c_str()) != 0) {
    std::cerr << "Unittest Setup: Failed to chdir() into sandbox directory "
              << "'" << sandbox_ << "' (errno: " << errno << ")."
              << std::endl;
    abort();
  }
}
