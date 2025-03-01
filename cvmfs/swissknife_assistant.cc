/**
 * This file is part of the CernVM File System
 */

#include "swissknife_assistant.h"

#include <unistd.h>

#include <cassert>
#include <cstdlib>

#include "catalog.h"
#include "catalog_rw.h"
#include "history.h"
#include "history_sqlite.h"
#include "manifest.h"
#include "network/download.h"
#include "util/logging.h"
#include "util/posix.h"

using namespace std;  // NOLINT

namespace swissknife {

catalog::Catalog *Assistant::GetCatalog(
  const shash::Any &catalog_hash,
  OpenMode open_mode)
{
  assert(shash::kSuffixCatalog == catalog_hash.suffix);
  string local_path = CreateTempPath(tmp_dir_ + "/catalog", 0600);
  assert(!local_path.empty());

  if (!FetchObject(catalog_hash, local_path)) {
    return NULL;
  }

  const std::string catalog_root_path = "";
  catalog::Catalog *catalog;
  switch (open_mode) {
    case kOpenReadWrite:
      catalog = catalog::WritableCatalog::AttachFreely(catalog_root_path,
                                                      local_path,
                                                      catalog_hash);
      break;
    case kOpenReadOnly:
      catalog = catalog::Catalog::AttachFreely(catalog_root_path,
                                               local_path,
                                               catalog_hash);
      break;
    default:
      abort();
  }
  assert(catalog != NULL);
  catalog->TakeDatabaseFileOwnership();
  return catalog;
}


history::History *Assistant::GetHistory(OpenMode open_mode) {
  const shash::Any history_hash = manifest_->history();
  history::History *history;

  string local_path = CreateTempPath(tmp_dir_ + "/history", 0600);
  assert(!local_path.empty());

  if (history_hash.IsNull()) {
    history = history::SqliteHistory::Create(local_path,
                                             manifest_->repository_name());
    if (NULL == history) {
      LogCvmfs(kLogCvmfs, kLogStderr, "failed to create history database");
      return NULL;
    }
    return history;
  }

  if (!FetchObject(history_hash, local_path))
    return NULL;

  switch (open_mode) {
    case kOpenReadWrite:
      history = history::SqliteHistory::OpenWritable(local_path);
      break;
    case kOpenReadOnly:
      history = history::SqliteHistory::Open(local_path);
      break;
    default:
      abort();
  }

  if (history == NULL) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to open history database (%s)",
             local_path.c_str());
    unlink(local_path.c_str());
    return NULL;
  }
  assert(history->fqrn() == manifest_->repository_name());
  history->TakeDatabaseFileOwnership();
  return history;
}


bool Assistant::FetchObject(const shash::Any &id, const string &local_path) {
  assert(!id.IsNull());

  download::Failures dl_retval;
  const std::string url = repository_url_ + "/data/" + id.MakePath();

  cvmfs::PathSink pathsink(local_path);
  download::JobInfo download_info(&url, true, false, &id, &pathsink);
  dl_retval = download_mgr_->Fetch(&download_info);

  if (dl_retval != download::kFailOk) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to download object '%s' (%d - %s)",
             id.ToStringWithSuffix().c_str(),
             dl_retval, download::Code2Ascii(dl_retval));
    return false;
  }
  return true;
}

}  // namespace swissknife
