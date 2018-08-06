/**
 * This file is part of the CernVM File System
 */

#define __STDC_FORMAT_MACROS

#include "sync_union_tarball.h"

#include <pthread.h>

#include <cassert>
#include <cstdio>
#include <list>
#include <set>
#include <string>
#include <vector>

#include "duplex_libarchive.h"
#include "fs_traversal.h"
#include "smalloc.h"
#include "sync_item.h"
#include "sync_item_dummy.h"
#include "sync_item_tar.h"
#include "sync_mediator.h"
#include "sync_union.h"
#include "util/posix.h"
#include "util_concurrency.h"

namespace publish {

SyncUnionTarball::SyncUnionTarball(AbstractSyncMediator *mediator,
                                   const std::string &rdonly_path,
                                   const std::string &tarball_path,
                                   const std::string &base_directory,
                                   const std::string &to_delete)
    : SyncUnion(mediator, rdonly_path, "", ""),
      tarball_path_(tarball_path),
      base_directory_(base_directory),
      to_delete_(to_delete),
      read_archive_signal_(new Signal) {
}

SyncUnionTarball::~SyncUnionTarball() { delete read_archive_signal_; }

bool SyncUnionTarball::Initialize() {
  bool result;

  src = archive_read_new();
  assert(ARCHIVE_OK == archive_read_support_format_tar(src));
  assert(ARCHIVE_OK == archive_read_support_format_empty(src));

  if (tarball_path_ == "-") {
    result = archive_read_open_filename(src, NULL, kBlockSize);
  } else {
    std::string tarball_absolute_path = GetAbsolutePath(tarball_path_);
    result = archive_read_open_filename(src, tarball_absolute_path.c_str(),
                                        kBlockSize);
  }

  if (result != ARCHIVE_OK) {
    LogCvmfs(kLogUnionFs, kLogStderr, "Impossible to open the archive.");
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
 */
void SyncUnionTarball::Traverse() {
  read_archive_signal_->Wakeup();
  assert(this->IsInitialized());

  /*
   * As first step we eliminate the requested directories.
   */
  if (to_delete_ != "") {
    vector<std::string> to_eliminate_vec = SplitString(to_delete_, ':');

    for (vector<string>::iterator s = to_eliminate_vec.begin();
         s != to_eliminate_vec.end(); ++s) {
      std::string parent_path;
      std::string filename;
      SplitPath(*s, &parent_path, &filename);
      if (parent_path == ".") parent_path = "";
      SharedPtr<SyncItem> sync_entry =
          CreateSyncItem(parent_path, filename, kItemDir);
      mediator_->Remove(sync_entry);
    }
  }

  struct archive_entry *entry = archive_entry_new();
  while (true) {
    // Get the lock, wait if lock is not available yet
    read_archive_signal_->Wait();

    int result = archive_read_next_header2(src, entry);

    switch (result) {
      case ARCHIVE_FATAL: {
        LogCvmfs(kLogUnionFs, kLogStderr,
                 "Fatal error in reading the archive.\n%s\n",
                 archive_error_string(src));
        abort();
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
        for (set<string>::iterator dir = to_create_catalog_dirs_.begin();
             dir != to_create_catalog_dirs_.end(); ++dir) {
          assert(dirs_.find(*dir) != dirs_.end());
          SharedPtr<SyncItem> to_mark = dirs_[*dir];
          assert(to_mark->IsDirectory());
          to_mark->SetCatalogMarker();
          to_mark->IsPlaceholderDirectory();
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
        LogCvmfs(kLogUnionFs, kLogStderr,
                 "Enter in unknow state. Aborting.\nError: %s\n", result,
                 archive_error_string(src));

        abort();
      }
    }
  }
}

void SyncUnionTarball::ProcessArchiveEntry(struct archive_entry *entry) {
  std::string archive_file_path(archive_entry_pathname(entry));
  std::string complete_path =
      MakeCanonicalPath(base_directory_ + "/" + archive_file_path);

  std::string parent_path;
  std::string filename;
  SplitPath(complete_path, &parent_path, &filename);

  CreateDirectories(parent_path);

  SharedPtr<SyncItem> sync_entry = SharedPtr<SyncItem>(new SyncItemTar(
      parent_path, filename, src, entry, read_archive_signal_, this));

  if (NULL != archive_entry_hardlink(entry)) {
    const std::string hardlink =
        base_directory_ + "/" + std::string(archive_entry_hardlink(entry));

    if (hardlinks_.find(hardlink) != hardlinks_.end()) {
      hardlinks_.find(hardlink)->second.push_back(complete_path);
    } else {
      std::list<std::string> to_hardlink;
      to_hardlink.push_back(complete_path);
      hardlinks_[hardlink] = to_hardlink;
    }
    read_archive_signal_->Wakeup();
    return;
  }

  if (sync_entry->IsDirectory()) {
    if (know_directories_.find(complete_path) != know_directories_.end()) {
      sync_entry->IsPlaceholderDirectory();
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

  } else if (sync_entry->IsSymlink() || sync_entry->IsFifo() ||
             sync_entry->IsSocket() || sync_entry->IsCharacterDevice() ||
             sync_entry->IsBlockDevice()) {
    // we avoid to add an entity called as a catalog marker if it is not a
    // regular file
    if (filename != ".cvmfscatalog") {
      ProcessFile(sync_entry);
    } else {
      LogCvmfs(kLogUnionFs, kLogStderr,
               "Found entity called as a catalog marker '%s' that however is "
               "not a regular file, abort",
               complete_path.c_str());
      abort();
    }

    // here we don't need to read data from the tar file so we can wake up
    // immediately the signal
    read_archive_signal_->Wakeup();

  } else {
    LogCvmfs(kLogUnionFs, kLogStderr,
             "Fatal error found unexpected file: \n%s\n", filename.c_str());
    // if for any reason this code path change and we don't abort anymore,
    // remember to wakeup the signal, otherwise we will be stuck in a deadlock
    //
    // read_archive_signal_->Wakeup();
    abort();
  }
}

void SyncUnionTarball::PostUpload() {
  std::map<const std::string, std::list<std::string> >::iterator hardlink;
  for (hardlink = hardlinks_.begin(); hardlink != hardlinks_.end();
       ++hardlink) {
    std::list<std::string>::iterator entry;
    for (entry = hardlink->second.begin(); entry != hardlink->second.end();
         ++entry) {
      mediator_->Clone(*entry, hardlink->first);
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

/* Tar files are not necessarly traversed in order from root to leave.
 * So it may happens that we are expanding the file `/a/b/c.txt` without
 * having created yet the directory `/a/b/`.
 * In order to overcome this limitation the following function create dummy
 * directories that can be used as placeholder and that they will be overwritten
 * as soon as the real directory is found in the tarball
 */
void SyncUnionTarball::CreateDirectories(const std::string &target) {
  if (know_directories_.find(target) != know_directories_.end()) return;
  if (target == ".") return;

  std::string dirname = "";
  std::string filename = "";
  SplitPath(target, &dirname, &filename);
  CreateDirectories(dirname);

  if (dirname == ".") dirname = "";
  SharedPtr<SyncItem> dummy = SharedPtr<SyncItem>(
      new SyncItemDummyDir(dirname, filename, this, kItemDir));

  ProcessUnmaterializedDirectory(dummy);
  know_directories_.insert(target);
}

}  // namespace publish
