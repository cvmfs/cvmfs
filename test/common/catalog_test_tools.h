/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_CATALOG_TEST_TOOLS_H_
#define CVMFS_CATALOG_TEST_TOOLS_H_

#include <string>
#include <vector>

#include "catalog_mgr_rw.h"
#include "directory_entry.h"
#include "server_tool.h"
#include "statistics.h"
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

class CatalogTestTool : public ServerTool {
 public:
  CatalogTestTool(const std::string& name,
                  const DirSpec& spec);
  ~CatalogTestTool();

 private:
  const std::string name_;
  DirSpec spec_;

  perf::Statistics stats_;

  UniquePtr<upload::Spooler> spooler_;

  UniquePtr<catalog::WritableCatalogManager> catalog_mgr_;
};

#endif  //  CVMFS_CATALOG_TEST_TOOLS_H_
