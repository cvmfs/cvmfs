/**
 * This file is part of the CernVM File System.
 */
 #include "cvmfs_config.h"
 #include "history_fs.h"

#include "catalog_mgr_rw.h"
#include "history.h"
#include "swissknife_sync.h"

using namespace std;  // NOLINT


TagFolderGenerator::TagFolderGenerator(
  manifest::Manifest *m,
  download::DownloadManager *d,
  catalog::WritableCatalogManager *c,
  SyncParameters *p)
  : catalog_mgr_(c)
  , assistant_(d, m, p->stratum0, p->dir_temp)
{ }


void TagFolderGenerator::Generate() {

}
