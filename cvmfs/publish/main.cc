/**
 * This file is part of the CernVM File System.
 */

#include "logging.h"

int main(int argc, char **argv) {
  LogCvmfs(kLogCvmfs, kLogStdout, "Hello, World!");
  return 0;
}
