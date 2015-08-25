/**
 * This file is part of the CernVM File System.
 */

#include "env.h"
#include "../../cvmfs/util.h"

#include <cassert>
#include <cerrno>
#include <cstdlib>

#include <fstream>
#include <string>
#include <sstream>
#include <iostream>


const size_t CvmfsEnvironment::kMaxPathLength = 255;


CvmfsEnvironment::CvmfsEnvironment(const int argc, char **argv) :
  is_death_test_execution_(CvmfsEnvironment::IsDeathTestExecution(argc, argv)) {}


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
  assert(sandbox_.empty());
  assert(sandbox_pointer_.empty());

  if (! is_death_test_execution_) {
    CreateSandbox();
  } else {
    AdoptSandboxFromParent();
  }

  assert(!sandbox_.empty());
  assert(!sandbox_pointer_.empty());

  ChangeDirectoryToSandbox();
  assert(GetCurrentWorkingDirectory() == sandbox_);
}


void CvmfsEnvironment::TearDown() {
  if (! is_death_test_execution_) {
    RemoveSandbox();
  }
}


std::string CvmfsEnvironment::GetSandboxPointerPath(const pid_t pid) const {
  std::stringstream ss;
  ss << "/tmp/cvmfs_ut_sandbox.pid." << pid;
  return ss.str();
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

  // create sandbox pointer file next to sandbox directory
  const std::string sandbox_pointer_path = GetSandboxPointerPath(getpid());
  std::ofstream sandbox_pointer;
  sandbox_pointer.open(sandbox_pointer_path.c_str(),
                       std::ios::out | std::ios::trunc);
  if (!sandbox_pointer.is_open()) {
    std::cerr << "Unittest Setup: Failed to create sandbox pointer file "
              << "'" << sandbox_pointer_path << "' (errno: " << errno << ")."
              << std::endl;
    abort();
  }

  sandbox_pointer << sandbox_ << std::endl;
  sandbox_pointer.close();
  sandbox_pointer_ = sandbox_pointer_path;
}


void CvmfsEnvironment::AdoptSandboxFromParent() {
  assert(sandbox_.empty());
  assert(sandbox_pointer_.empty());

  const std::string sandbox_pointer_path = GetSandboxPointerPath(getppid());
  if (! FileExists(sandbox_pointer_path)) {
    std::cerr << "Unittest Setup: Failed to find sandbox pointer file "
              << "'" << sandbox_pointer_path << "' of parent process."
              << std::endl;
    abort();
  }

  std::ifstream sandbox_pointer;
  sandbox_pointer.open(sandbox_pointer_path.c_str());
  if (!sandbox_pointer.is_open()) {
    std::cerr << "Unittest Setup: Failed to open sandbox pointer file "
              << "'" << sandbox_pointer_path << "' (errno: " << errno << ")."
              << std::endl;
    abort();
  }

  char path[256] = { '\0' };
  sandbox_pointer.getline(path, sizeof(path));
  if (sandbox_pointer.fail() || sandbox_pointer.bad() || strlen(path) == 0) {
    std::cerr << "Unittest Setup: Failed to read sandbox pointer file "
              << "'" << sandbox_pointer_path << "' (errno: " << errno << ")."
              << std::endl;
    abort();
  }

  if (!DirectoryExists(path)) {
    std::cerr << "Unittest Setup: Failed to find sandbox directory "
              << "'" << path << "' pointed to by "
              << "'" << sandbox_pointer_path << "'"
              << std::endl;
    abort();
  }

  sandbox_pointer_ = sandbox_pointer_path;
  sandbox_         = path;
}


void CvmfsEnvironment::RemoveSandbox() {
  assert (!sandbox_.empty());
  assert (!sandbox_pointer_.empty());

  unlink (sandbox_pointer_.c_str());
  RemoveTree(sandbox_);
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
