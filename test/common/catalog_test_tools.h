/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_CATALOG_TEST_TOOLS_H_
#define CVMFS_CATALOG_TEST_TOOLS_H_

#include <string>
#include <vector>

#include "directory_entry.h"
#include "statistics.h"
#include "testutil.h"
#include "upload.h"
#include "util/pointer.h"

struct DirSpecEntry {
  DirSpecEntry(shash::Any hash, size_t size, std::string parent,
               std::string name)
      : hash_(hash), size_(size), parent_(parent), name_(name) {}

  shash::Any hash_;
  size_t size_;
  std::string parent_;
  std::string name_;
};

typedef std::vector<DirSpecEntry> DirSpecEntryList;

struct DirSpec {
  DirSpec(const DirSpecEntryList& entries);
  ~DirSpec();

  DirSpecEntryList entries_;
};

class CatalogTestTool {
 public:
  CatalogTestTool(const DirSpec& spec);
  ~CatalogTestTool();

 private:
  DirSpec spec_;
  perf::Statistics statistics_;
  UniquePtr<catalog::MockCatalogManager> catalog_mgr_;
};

#endif  //  CVMFS_CATALOG_TEST_TOOLS_H_
