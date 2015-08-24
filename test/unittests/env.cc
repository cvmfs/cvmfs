/**
 * This file is part of the CernVM File System.
 */

#include "env.h"
#include "../../cvmfs/util.h"

#include <cerrno>
#include <string>
#include <iostream>


const size_t CvmfsEnvironment::kMaxPathLength = 255;


CvmfsEnvironment::CvmfsEnvironment(const int argc, char **argv) :
  owns_sandbox_(! CvmfsEnvironment::IsDeathTestExecution(argc, argv)) {}


bool CvmfsEnvironment::IsDeathTestExecution(const int argc, char **argv) {
  const char* death_test_flag="--gtest_internal_run_death_test";

  for (int i = 0; i < argc; ++i) {
    if (strncmp(argv[i], death_test_flag, strlen(death_test_flag)) == 0) {
      return true;
    }
  }

  return false;
}


void CvmfsEnvironment::SetUp() {
  if (owns_sandbox_) {
    sandbox_ = CreateTempDir("/tmp/cvmfs_ut_sandbox");
    if (sandbox_.empty()) {
      std::cerr << "Unittest Setup: Failed to create sandbox directory in /tmp "
                << " (errno: " << errno << ").";
      abort();
    }

    if (chdir(sandbox_.c_str()) != 0) {
      std::cerr << "Unittest Setup: Failed to chdir() into sandbox directory "
                << "'" << sandbox_ << "' (errno: " << errno << ").";
      abort();
    }
  }
}


void CvmfsEnvironment::TearDown() {
  if (owns_sandbox_) {
    RemoveTree(sandbox_);
  }
}
