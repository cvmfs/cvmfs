/**
 * This file is part of the CernVM File System
 */

#define __STDC_FORMAT_MACROS

#include "sync_union_tarball.h"

#include <pthread.h>
#include <stdio.h>
#include <cassert>
#include <set>
#include <string>
#include <vector>

#include "archive.h"
#include "archive_entry.h"
#include "fs_traversal.h"
#include "smalloc.h"
#include "sync_item.h"
#include "sync_item_dummy.h"
#include "sync_item_tar.h"
#include "sync_mediator.h"
#include "sync_union.h"
#include "util/posix.h"

namespace publish {

SyncUnionTarball::SyncUnionTarball(AbstractSyncMediator *mediator,
                                   const std::string &rdonly_path,
                                   const std::string &union_path,
                                   const std::string &scratch_path,
                                   const std::string &tarball_path,
                                   const std::string &base_directory,
                                   const std::string &to_delete)
    : SyncUnion(mediator, rdonly_path, union_path, scratch_path),
      tarball_path_(tarball_path),
      base_directory_(base_directory),
      to_delete_(to_delete) {
  archive_lock_ =
      reinterpret_cast<pthread_mutex_t *>(smalloc(sizeof(pthread_mutex_t)));
  assert(0 == pthread_mutex_init(archive_lock_, NULL));

  read_archive_cond_ =
      reinterpret_cast<pthread_cond_t *>(smalloc(sizeof(pthread_cond_t)));
  assert(0 == pthread_cond_init(read_archive_cond_, NULL));

  can_read_archive_ = new bool;
  *can_read_archive_ = true;
}

SyncUnionTarball::~SyncUnionTarball() {
  pthread_mutex_lock(archive_lock_);
  pthread_mutex_unlock(archive_lock_);

  pthread_mutex_destroy(archive_lock_);
  pthread_cond_destroy(read_archive_cond_);

  delete can_read_archive_;
}

bool SyncUnionTarball::Initialize() {
  bool result;

  src = archive_read_new();
  assert(ARCHIVE_OK == archive_read_support_format_tar(src));
  assert(ARCHIVE_OK == archive_read_support_format_empty(src));

  if (tarball_path_ == "--") {
    result = archive_read_open_filename(src, NULL, 4096);
  } else {
    std::string tarball_absolute_path = GetAbsolutePath(tarball_path_);
    result =
        archive_read_open_filename(src, tarball_absolute_path.c_str(), 4096);
  }

  if (result != ARCHIVE_OK) {
    LogCvmfs(kLogUnionFs, kLogStderr, "Impossible to open the archive.");
    return false;
  }

  return (result == ARCHIVE_OK) && SyncUnion::Initialize();
}

void SyncUnionTarball::Traverse() {
  assert(this->IsInitialized());
  struct archive_entry *entry = archive_entry_new();
  bool retry_read_header = false;

  /*
   * As first step we eliminate the directories we are request.
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

  while (true) {
    do {
      retry_read_header = false;

      /* Get the lock, wait if lock is not available yet */
      pthread_mutex_lock(archive_lock_);
      while (!*can_read_archive_) {
        pthread_cond_wait(read_archive_cond_, archive_lock_);
      }
      pthread_mutex_unlock(archive_lock_);

      int result = archive_read_next_header2(src, entry);
      *can_read_archive_ = false;

      switch (result) {
        case ARCHIVE_FATAL: {
          LogCvmfs(kLogUnionFs, kLogStderr,
                   "Fatal error in reading the archive.");
          return;
          break; /* Only exit point with error */
        }

        case ARCHIVE_RETRY: {
          LogCvmfs(kLogUnionFs, kLogStderr,
                   "Error in reading the header, retrying. \n %s",
                   archive_error_string(src));
          retry_read_header = true;
          break;
        }

        case ARCHIVE_EOF: {
          for (set<string>::iterator dir = to_create_catalog_dirs_.begin();
               dir != to_create_catalog_dirs_.end(); ++dir) {
            assert(dirs_.find(*dir) != dirs_.end());
            SharedPtr<SyncItem> to_mark = dirs_[*dir];
            assert(to_mark->IsDirectory());
            to_mark->SetCatalogMarker();
            to_mark->AlreadyCreatedDir();
            ProcessDirectory(to_mark);
          }
          return; /* Only successful exit point */
          break;
        }

        case ARCHIVE_WARN: {
          LogCvmfs(kLogUnionFs, kLogStderr,
                   "Warning in uncompression reading, going on. \n %s",
                   archive_error_string(src));
          /* We actually want this to enter the ARCHIVE_OK case */
        }

        case ARCHIVE_OK: {
          std::string archive_file_path(archive_entry_pathname(entry));
          std::string complete_path(base_directory_ + "/" + archive_file_path);

          if (*complete_path.rbegin() == '/') {
            complete_path.erase(complete_path.size() - 1);
          }

          std::string parent_path;
          std::string filename;
          SplitPath(complete_path, &parent_path, &filename);

          CreateDirectories(parent_path);

          /*
           * Libarchive is not thread aware, so we need to make sure that before
           * to read/"open" the next header in the archive the content of the
           * present header is been consumed completely.
           * Different thread read/"open" the header from the one that consumes
           * it so we opted for a conditional variable.
           * The conditional variable is acquire when we read the header and as
           * long as it is not released we cannot read/"open" the next header.
           * At the end of the pipeline, when the IngestionSource (generated by
           * the SyncItem) is been 1. Opened, 2. Read, at the closing we
           * actually release the conditional variable.
           * This whole process is not necessary for directories since we don't
           * actually need to read data from them.
           */
          SharedPtr<SyncItem> sync_entry = SharedPtr<SyncItem>(
              new SyncItemTar(parent_path, filename, src, entry, archive_lock_,
                              read_archive_cond_, can_read_archive_, this));

          if (sync_entry->IsDirectory()) {
            if (know_directories_.find(complete_path) !=
                know_directories_.end()) {
              sync_entry->AlreadyCreatedDir();
            }
            ProcessDirectory(sync_entry);
            dirs_[complete_path] = sync_entry;
            know_directories_.insert(complete_path);

            *can_read_archive_ = true;

          } else if (sync_entry->IsRegularFile()) {
            ProcessFile(sync_entry);
            printf("Processing file stop on lock");
            if (filename == ".cvmfscatalog") {
              to_create_catalog_dirs_.insert(parent_path);
            }
          } else {
            *can_read_archive_ = true;
          }
        }
      }
    } while (retry_read_header);
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

  ProcessDirectory(dummy);
  know_directories_.insert(target);
}

}  // namespace publish
