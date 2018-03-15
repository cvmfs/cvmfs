/**
 * This file is part of the CernVM File System
 */

#define __STDC_FORMAT_MACROS

#include "sync_union_tarball.h"

#include <cassert>
#include <set>
#include <string>

#include "archive.h"
#include "archive_entry.h"
#include "fs_traversal.h"
#include "sync_item.h"
#include "sync_item_tar.h"
#include "sync_mediator.h"
#include "sync_union.h"
#include "util/posix.h"

#include <stdio.h>

namespace publish {

SyncUnionTarball::SyncUnionTarball(AbstractSyncMediator *mediator,
                                   const std::string &rdonly_path,
                                   const std::string &union_path,
                                   const std::string &scratch_path,
                                   const std::string &tarball_path,
                                   const std::string &base_directory)
    : SyncUnion(mediator, rdonly_path, union_path, scratch_path),
      tarball_path_(tarball_path),
      base_directory_(base_directory) {}

bool SyncUnionTarball::Initialize() {
  bool result;

  // Save the absolute path of the tarball
  std::string tarball_absolute_path = GetAbsolutePath(tarball_path_);

  src = archive_read_new();
  archive_read_support_format_tar(src);

  result = archive_read_open_filename(src, tarball_absolute_path.c_str(), 4096);

  if (result != ARCHIVE_OK) {
    LogCvmfs(kLogUnionFs, kLogStderr, "Impossible to open the archive.");
    return false;
  }

  // Actually untar the whole archive
  // result = UntarPath(working_dir_, tarball_absolute_path);
  // return result && SyncUnion::Initialize();
  return (result == ARCHIVE_OK) && SyncUnion::Initialize();
}

void SyncUnionTarball::Traverse() {
  printf("%s", union_path().c_str());
  struct archive_entry *entry;
  // when we find a directory we stack it up and call the EnterDirectory
  // as soon as we find a file that does not belong to the directory we call
  // LeaveDirectory
  std::stack<std::string> directories_stacked;
  std::string directory_traversing;

  std::string archive_file_path;
  std::string complete_path;
  std::string parent_path;
  std::string filename;
  int result;
  bool retry_read_header = false;

  assert(this->IsInitialized());
  while (true) {
    do {
      retry_read_header = false;
      result = archive_read_next_header(src, &entry);

      switch (result) {
        case ARCHIVE_FATAL: {
          LogCvmfs(kLogUnionFs, kLogStderr,
                   "Fatal error in reading the archive.");
          return;
          break;
        }

        case ARCHIVE_RETRY: {
          LogCvmfs(kLogUnionFs, kLogStderr,
                   "Error in reading the header, retrying. \n %s",
                   archive_error_string(src));
          retry_read_header = true;
          break;
        }

        case ARCHIVE_EOF: {
          return;
          break;
        }

        case ARCHIVE_WARN: {
          LogCvmfs(kLogUnionFs, kLogStderr,
                   "Warning in uncompression reading, going on. \n %s",
                   archive_error_string(src));
          break;
        }

        case ARCHIVE_OK: {
          archive_file_path.assign(archive_entry_pathname(entry));
          complete_path.assign(base_directory_ + "/" + archive_file_path);

          if (*complete_path.rbegin() == '/') {
            complete_path.erase(complete_path.size() - 1);
          }

          printf("\n\n");

          SplitPath(complete_path, &parent_path, &filename);
          printf("parent: %s\nfilename: %s\n", parent_path.c_str(),
                 filename.c_str());

          CreateDirectories(parent_path);

          SharedPtr<SyncItem> sync_entry = SharedPtr<SyncItem>(new SyncItem(
              parent_path, filename, src, entry, this, kItemUnknown, kTarball));

          printf(
              "complete_path: \t%s \ndirectory_traversing: "
              "\t%s\n",
              complete_path.c_str(), directory_traversing.c_str());
          int64_t inode = archive_entry_ino64(entry);
          int link_count = archive_entry_nlink(entry);
          printf("inode: %" PRIu64 "\n", inode);
          printf("link count %d\n", link_count);
          if (sync_entry->IsDirectory()) {
            directories_stacked.push(complete_path);
            directory_traversing.assign(complete_path);
            EnterDirectory(parent_path, filename);
          } else {
            if (complete_path.rfind(directory_traversing, 0) != 0) {
              directory_traversing.assign(directories_stacked.top());
              directories_stacked.pop();
              std::string leave_parent, leave_filename;
              SplitPath(directory_traversing, &leave_parent, &leave_filename);
              printf("Leaving dir: %s / %s \n", leave_parent.c_str(),
                     leave_filename.c_str());
              LeaveDirectory(leave_parent, leave_filename);
            }
          }

          printf("archive_file_path\t%s\n", archive_file_path.c_str());
          printf("complete_path\t\t%s\n", complete_path.c_str());
          printf("parent_path\t\t%s\n", parent_path.c_str());
          printf("filename\t\t%s\n", filename.c_str());
          printf("relative_parent_path:\t%s\n",
                 sync_entry->relative_parent_path().c_str());
          printf("WhiteOut:\t\t%d\n", sync_entry->IsWhiteout());
          printf("New:\t\t\t%d\n", sync_entry->IsNew());
          printf("RelativePath:\t\t%s\n",
                 sync_entry->GetRelativePath().c_str());
          printf("filename:\t\t%s\n", sync_entry->filename().c_str());
          printf("RdOnlyPath:\t\t%s\n", sync_entry->GetRdOnlyPath().c_str());
          printf("UnionPath:\t\t%s\n", sync_entry->GetUnionPath().c_str());
          printf("ScratchPath:\t\t%s\n", sync_entry->GetScratchPath().c_str());

          printf("\n\n");

          if (sync_entry->IsDirectory()) {
            if (know_directories_.find(complete_path) !=
                know_directories_.end()) {
              sync_entry->AlreadyCreatedDir();
            }
            ProcessDirectory(sync_entry);
            know_directories_.insert(complete_path);
          } else if (sync_entry->IsRegularFile()) {
            ProcessFile(sync_entry);
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
  printf("\n\t\t\tCreateDirectories INIT | target = '%s'\n", target.c_str());
  if (know_directories_.find(target) != know_directories_.end()) return;
  if (target == ".") return;

  std::string dirname = "";
  std::string filename = "";
  SplitPath(target, &dirname, &filename);
  CreateDirectories(dirname);

  if (dirname == ".") dirname = "";
  printf("\n\t\t\tCreateDirectories CREATING | dirname = %s, filename = '%s'\n",
         dirname.c_str(), filename.c_str());
  SharedPtr<SyncItem> dummy = SharedPtr<SyncItem>(
      new SyncItem(dirname, filename, this, kItemDir, kDummyDir));

  catalog::DirectoryEntryBase dirent = dummy->CreateBasicCatalogDirent();
  printf("dummy is directory: %d\n", dirent.IsDirectory());

  ProcessDirectory(dummy);
  know_directories_.insert(target);

  printf("\n\t\t\tCreateDirectories END | target = '%s'\n", target.c_str());
}

}  // namespace publish
