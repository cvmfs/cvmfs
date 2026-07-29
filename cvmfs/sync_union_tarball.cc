/**
 * This file is part of the CernVM File System
 */

#include "sync_union_tarball.h"

#include <pthread.h>
#include <unistd.h>

#include <cassert>
#include <cstdio>
#include <list>
#include <set>
#include <string>
#include <vector>

#include "duplex_libarchive.h"
#include "sync_item.h"
#include "sync_item_dummy.h"
#include "sync_item_tar.h"
#include "sync_mediator.h"
#include "sync_union.h"
#include "util/concurrency.h"
#include "util/exception.h"
#include "util/fs_traversal.h"
#include "util/posix.h"
#include "util/smalloc.h"

namespace publish {

SyncUnionTarball::SyncUnionTarball(AbstractSyncMediator *mediator,
                                   const std::string &rdonly_path,
                                   const std::string &tarball_path,
                                   const std::string &base_directory,
                                   const uid_t uid,
                                   const gid_t gid,
                                   const std::string &to_delete,
                                   const bool create_catalog_on_root,
                                   const bool fast_delete,
                                   const std::string &path_delimiter,
                                   const bool tolerate_missing_hardlinks)
    : SyncUnion(mediator, rdonly_path, "", "")
    , src(NULL)
    , tarball_path_(tarball_path)
    , base_directory_(base_directory)
    , uid_(uid)
    , gid_(gid)
    , to_delete_(to_delete)
    , create_catalog_on_root_(create_catalog_on_root)
    , fast_delete_(fast_delete)
    , path_delimiter_(path_delimiter)
    , tolerate_missing_hardlinks_(tolerate_missing_hardlinks)
    , read_archive_signal_(new Signal) { }

SyncUnionTarball::~SyncUnionTarball() { delete read_archive_signal_; }

bool SyncUnionTarball::Initialize() {
  bool result;

  // We are just deleting entity from the repo
  if (tarball_path_ == "") {
    assert(NULL == src);
    return SyncUnion::Initialize();
  }

  src = archive_read_new();
  assert(ARCHIVE_OK == archive_read_support_format_tar(src));
  assert(ARCHIVE_OK == archive_read_support_format_empty(src));

  if (tarball_path_ == "-") {
    result = archive_read_open_filename(src, NULL, kBlockSize);
  } else {
    const std::string tarball_absolute_path = GetAbsolutePath(tarball_path_);
    result = archive_read_open_filename(src, tarball_absolute_path.c_str(),
                                        kBlockSize);
  }

  if (result != ARCHIVE_OK) {
    LogCvmfs(kLogUnionFs, kLogStderr, "Impossible to open the archive: %s",
             archive_error_string(src));
    return false;
  }

  return SyncUnion::Initialize();
}

/*
 * Libarchive is not thread aware, so we need to make sure that before
 * to read/"open" the next header in the archive the content of the
 *
 * present header is been consumed completely.
 * Different thread read/"open" the header from the one that consumes
 * it so we opted for a Signal that is backed by a conditional variable.
 * We wait for the signal just before to read the header.
 * Then when we have done with the header the Signal is fired.
 * The Signal can be fired inside the main loop if we don't need to read
 * data, or when the IngestionSource get closed, which means that we are
 * not reading data anymore from there.
 * This whole process is not necessary for directories since we don't
 * actually need to read data from them.
 *
 * It may be needed to add a catalog as a root of the archive.
 * A possible way to do it is by creating an virtual `.cvmfscatalog` file and
 * push it into the usual pipeline.
 * This operation must be done only once, and it seems like a good idea to do
 * it at the first iteration of the loop, hence this logic is managed by the
 * `first_iteration` boolean flag.
 */
void SyncUnionTarball::Traverse() {
  assert(this->IsInitialized());

  /*
   * As first step we eliminate the requested directories.
   */
  if (to_delete_ != "") {
    vector<std::string> to_eliminate_vec = SplitStringMultiChar(
        to_delete_, path_delimiter_);

    for (vector<string>::iterator s = to_eliminate_vec.begin();
         s != to_eliminate_vec.end();
         ++s) {
      std::string parent_path;
      std::string filename;
      SplitPath(*s, &parent_path, &filename);
      if (parent_path == ".")
        parent_path = "";
      const SharedPtr<SyncItem> sync_entry = CreateSyncItem(parent_path,
                                                            filename, kItemDir);
      mediator_->Remove(sync_entry, fast_delete_);
    }
  }

  // we are simply deleting entity from  the repo
  if (NULL == src)
    return;

  // Prime the signal so the first Wait() in the loop below can proceed.
  read_archive_signal_->Wakeup();

  struct archive_entry *entry = archive_entry_new();
  while (true) {
    // Get the lock, wait if lock is not available yet
    read_archive_signal_->Wait();

    const int result = archive_read_next_header2(src, entry);

    switch (result) {
      case ARCHIVE_FATAL: {
        PANIC(kLogStderr, "Fatal error in reading the archive.\n%s\n",
              archive_error_string(src));
        break;  // Only exit point with error
      }

      case ARCHIVE_RETRY: {
        LogCvmfs(kLogUnionFs, kLogStdout,
                 "Error in reading the header, retrying.\n%s\n",
                 archive_error_string(src));
        continue;
        break;
      }

      case ARCHIVE_EOF: {
        if (create_catalog_on_root_ && (base_directory_ != "/")) {
          CreateDirectories(base_directory_);  // necessary for empty archives
          const SharedPtr<SyncItem> catalog = SharedPtr<SyncItem>(
              new SyncItemDummyCatalog(base_directory_, this));
          ProcessFile(catalog);
          to_create_catalog_dirs_.insert(base_directory_);
        }
        for (set<string>::iterator dir = to_create_catalog_dirs_.begin();
             dir != to_create_catalog_dirs_.end();
             ++dir) {
          assert(dirs_.find(*dir) != dirs_.end());
          const SharedPtr<SyncItem> to_mark = dirs_[*dir];
          assert(to_mark->IsDirectory());
          to_mark->SetCatalogMarker();
          to_mark->MakePlaceholderDirectory();
          ProcessDirectory(to_mark);
        }
        return;  // Only successful exit point
        break;
      }

      case ARCHIVE_WARN: {
        LogCvmfs(kLogUnionFs, kLogStderr,
                 "Warning in uncompression reading, going on.\n %s",
                 archive_error_string(src));
        // We actually want this to enter the ARCHIVE_OK case
      }

      case ARCHIVE_OK: {
        ProcessArchiveEntry(entry);
        break;
      }

      default: {
        // We should never enter in this branch, but just for safeness we prefer
        // to abort in case we hit a case we don't how to manage.
        PANIC(kLogStderr, "Enter in unknown state. Aborting.\nError: %s\n",
              result, archive_error_string(src));
      }
    }
  }
}

void SyncUnionTarball::ProcessArchiveEntry(struct archive_entry *entry) {
  std::string archive_file_path(archive_entry_pathname(entry));
  archive_file_path = SanitizePath(archive_file_path);

  const std::string complete_path = base_directory_ != "/"
                                        ? MakeCanonicalPath(base_directory_
                                                            + "/"
                                                            + archive_file_path)
                                        : MakeCanonicalPath(archive_file_path);

  std::string parent_path;
  std::string filename;
  SplitPath(complete_path, &parent_path, &filename);
  if (parent_path == ".")
    parent_path.clear();

  CreateDirectories(parent_path);

  const SharedPtr<SyncItem> sync_entry = SharedPtr<SyncItem>(
      new SyncItemTar(parent_path, filename, src, entry, read_archive_signal_,
                      this, uid_, gid_));

  if (NULL != archive_entry_hardlink(entry)) {
    const std::string hardlink_name(
        SanitizePath(archive_entry_hardlink(entry)));
    const std::string hardlink = base_directory_ != "/"
                                     ? base_directory_ + "/" + hardlink_name
                                     : hardlink_name;

    // Capture the link's own ownership/permissions so that, if the target is
    // not part of this archive, an empty file can be materialized in its place
    // (see PostUpload and tolerate_missing_hardlinks_).  Read directly from the
    // tar header, applying the same uid/gid override as SyncItemTar.
    const struct stat *link_stat = archive_entry_stat(entry);
    const uid_t link_uid = (uid_ != -1u) ? uid_ : link_stat->st_uid;
    const gid_t link_gid = (gid_ != -1u) ? gid_ : link_stat->st_gid;
    const HardlinkDestination destination(complete_path, link_stat->st_mode,
                                          link_uid, link_gid,
                                          link_stat->st_mtime);
    if (hardlinks_.find(hardlink) != hardlinks_.end()) {
      hardlinks_.find(hardlink)->second.push_back(destination);
    } else {
      std::list<HardlinkDestination> to_hardlink;
      to_hardlink.push_back(destination);
      hardlinks_[hardlink] = to_hardlink;
    }
    if (filename == ".cvmfscatalog") {
      // the file is created in the PostUpload phase
      to_create_catalog_dirs_.insert(parent_path);
    }
    read_archive_signal_->Wakeup();
    return;
  }

  if (sync_entry->IsDirectory()) {
    if (know_directories_.find(complete_path) != know_directories_.end()) {
      sync_entry->MakePlaceholderDirectory();
    }
    ProcessUnmaterializedDirectory(sync_entry);
    dirs_[complete_path] = sync_entry;
    know_directories_.insert(complete_path);

    read_archive_signal_->Wakeup();  // We don't need to read data and we
                                     // can read the next header

  } else if (sync_entry->IsRegularFile()) {
    // inside the process pipeline we will wake up the signal
    ProcessFile(sync_entry);
    if (filename == ".cvmfscatalog") {
      to_create_catalog_dirs_.insert(parent_path);
    }

  } else if (sync_entry->IsSymlink() || sync_entry->IsFifo()
             || sync_entry->IsSocket() || sync_entry->IsCharacterDevice()
             || sync_entry->IsBlockDevice()) {
    // we avoid to add an entity called as a catalog marker if it is not a
    // regular file
    if (filename != ".cvmfscatalog") {
      ProcessFile(sync_entry);
    } else {
      PANIC(kLogStderr,
            "Found entity called as a catalog marker '%s' that however is "
            "not a regular file, abort",
            complete_path.c_str());
    }

    // here we don't need to read data from the tar file so we can wake up
    // immediately the signal
    read_archive_signal_->Wakeup();

  } else {
    PANIC(kLogStderr, "Fatal error found unexpected file: \n%s\n",
          filename.c_str());
    // if for any reason this code path change and we don't abort anymore,
    // remember to wakeup the signal, otherwise we will be stuck in a deadlock
    //
    // read_archive_signal_->Wakeup();
  }
}

std::string SyncUnionTarball::SanitizePath(const std::string &path) {
  if (path.length() >= 2) {
    if (path[0] == '.' && path[1] == '/') {
      return path.substr(2);
    }
  }
  if (path.length() >= 1) {
    if (path[0] == '/') {
      return path.substr(1);
    }
  }
  return path;
}

void SyncUnionTarball::PostUpload() {
  // When tolerating missing hardlink targets we ask Clone not to abort on a
  // missing source; it returns false instead and we materialize an empty file.
  const bool fail_if_target_missing = !tolerate_missing_hardlinks_;
  std::map<const std::string, std::list<HardlinkDestination> >::iterator
      hardlink;
  for (hardlink = hardlinks_.begin(); hardlink != hardlinks_.end();
       ++hardlink) {
    const std::string &target = hardlink->first;
    std::list<HardlinkDestination>::iterator entry;
    for (entry = hardlink->second.begin(); entry != hardlink->second.end();
         ++entry) {
      const bool cloned = mediator_->Clone(entry->path, target,
                                           fail_if_target_missing);
      if (cloned)
        continue;

      // The hardlink target is not part of this archive (e.g. a cross-layer
      // hardlink in an OCI image layer).  Materialize an empty regular file at
      // the link's path, preserving its ownership and permissions, and push it
      // through the normal ingestion pipeline so the empty object is stored.
      std::string parent_path;
      std::string filename;
      SplitPath(entry->path, &parent_path, &filename);
      if (parent_path == ".")
        parent_path.clear();
      const SharedPtr<SyncItem> empty_file = SharedPtr<SyncItem>(
          new SyncItemDummyFile(parent_path, filename, this, entry->mode,
                                entry->uid, entry->gid, entry->mtime));
      ProcessFile(empty_file);
      LogCvmfs(kLogUnionFs, kLogStderr | kLogSyslogWarn,
               "hardlink target '%s' is not present in the tarball; "
               "materialized an empty file at '%s'",
               target.c_str(), entry->path.c_str());
    }
  }
}

std::string SyncUnionTarball::UnwindWhiteoutFilename(
    SharedPtr<SyncItem> entry) const {
  return entry->filename();
}

bool SyncUnionTarball::IsOpaqueDirectory(SharedPtr<SyncItem> directory) const {
  return false;
}

bool SyncUnionTarball::IsWhiteoutEntry(SharedPtr<SyncItem> entry) const {
  return false;
}

/* Tar files are not necessarily traversed in order from root to leave.
 * So it may happens that we are expanding the file `/a/b/c.txt` without
 * having created yet the directory `/a/b/`.
 * In order to overcome this limitation the following function create dummy
 * directories that can be used as placeholder and that they will be overwritten
 * as soon as the real directory is found in the tarball
 */
void SyncUnionTarball::CreateDirectories(const std::string &target) {
  if (know_directories_.find(target) != know_directories_.end())
    return;
  if (target == ".")
    return;

  std::string dirname = "";
  std::string filename = "";
  SplitPath(target, &dirname, &filename);
  CreateDirectories(dirname);

  if (dirname == ".")
    dirname = "";
  const SharedPtr<SyncItem> dummy = SharedPtr<SyncItem>(
      new SyncItemDummyDir(dirname, filename, this, kItemDir, uid_, gid_));

  ProcessUnmaterializedDirectory(dummy);
  dirs_[target] = dummy;
  know_directories_.insert(target);
}

}  // namespace publish
