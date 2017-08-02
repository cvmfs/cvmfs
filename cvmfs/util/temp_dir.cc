/**
 * This file is part of the CernVM File System.
 */

#include "temp_dir.h"

#include "posix.h"

TempDir* TempDir::Create(const std::string& prefix) {
  TempDir* tmp = new TempDir(prefix);
  if (tmp->dir() != "") {
    return tmp;
  } else {
    delete tmp;
    return NULL;
  }
}

TempDir::TempDir(const std::string& prefix) : dir_(CreateTempDir(prefix)) {}

TempDir::~TempDir() {
  RemoveTree(dir_);
}
