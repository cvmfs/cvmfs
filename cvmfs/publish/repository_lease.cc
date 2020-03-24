/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "publish/repository.h"

#include <string>

#include "catalog_mgr_ro.h"
#include "directory_entry.h"
#include "publish/except.h"
#include "upload.h"

namespace publish {

void Publisher::AcquireLease(const std::string &path) {
  if (spooler()->GetDriverType() != upload::SpoolerDefinition::Gateway)
    return;

  catalog::SimpleCatalogManager *catalog_mgr = GetSimpleCatalogManager();
  catalog::DirectoryEntry dirent;
  bool retval = catalog_mgr->LookupPath(path, catalog::kLookupSole, &dirent);
  if (!retval) {
    throw EPublish("cannot open transaction on non-existing path " + path);
  }
  if (!dirent.IsDirectory()) {
    throw EPublish("cannot open transaction on " + path + ", which is not "
                   "a directory");
  }
}

void Publisher::DropLease() {
}

}  // namespace publish
