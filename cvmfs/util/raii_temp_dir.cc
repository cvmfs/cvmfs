/**
 * This file is part of the CernVM File System.
 */

#include "raii_temp_dir.h"
#include <string>
#include <cstddef>

#include "util/posix.h"

RaiiTempDir *RaiiTempDir::Create(const std::string &prefix) {
  RaiiTempDir *tmp = new RaiiTempDir(prefix);
  if (tmp->dir() != "") {
    return tmp;
  } else {
    delete tmp;
    return NULL;
  }
}

RaiiTempDir::RaiiTempDir(const std::string &prefix)
    : dir_(CreateTempDir(prefix)) { }

RaiiTempDir::~RaiiTempDir() { RemoveTree(dir_); }
