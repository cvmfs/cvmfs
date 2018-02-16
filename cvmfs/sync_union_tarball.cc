/**
 * This file is part of the CernVM File System
 */

#define __STDC_FORMAT_MACROS

#include "sync_union_tarball.h"

#include <archive.h>
#include <archive_entry.h>
#include <util/posix.h>

#include <set>
#include <string>

#include "sync_union.h"

#include "fs_traversal.h"
#include "sync_item.h"
#include "sync_mediator.h"

namespace publish {
/*
 * Still have to underastand how the madiator, scratch, read and union path
 * works together.
 */

// SyncUnionTarball::SyncUnionTarball(VirtualSyncMed)

SyncUnionTarball::SyncUnionTarball(AbstractSyncMediator *mediator,
                                   const std::string &rdonly_path,
                                   const std::string &union_path,
                                   const std::string &scratch_path,
                                   const std::string &tarball_path,
                                   const std::string &base_directory)
    : SyncUnion(mediator, rdonly_path, union_path, scratch_path),
      tarball_path_(tarball_path),
      base_directory_(base_directory) {
  working_dir_ = "";
}

/*
~SyncUnionTarball::SyncUnionTarball() {
  if (RemoveTree(working_dir_) != true) {
    LogCvmfs(kLogUnionFs, kLogStderr,
             "Impossible to remove the working directory. (errno %d)", errno);
  }
}
*/

/*
 * We traferse the tar and then we keep track (the path inside a set) of each
 * .tar we find, then we go back and repeat the procedure. Should we be worry by
 * tar-bombs? Maybe, we will think about it later.
 */
bool SyncUnionTarball::Initialize() {
  bool result;

  // We save the current working directory
  std::string cwd = GetCurrentWorkingDirectory();
  // Save the absolute path of the tarball
  std::string tarball_absolute_path = GetAbsolutePath(tarball_path_);
  // Create the actuall temp directory
  working_dir_ = CreateTempDir(base_directory_);
  if (working_dir_ == "") {
    LogCvmfs(kLogUnionFs, kLogStderr,
             "Impossible to create the destination directory. (errno %d)",
             errno);
    return false;
  }

  // Actually untar the whole archive
  result = untarPath(working_dir_, tarball_absolute_path);
  return result && SyncUnion::Initialize();
}

bool SyncUnionTarball::untarPath(const std::string &base_untar_directory_path,
                                 const std::string &tarball_path) {
  struct archive *src;  // source of the archive
  struct archive *dst;  // destination for the untar
  struct archive_entry *entry;

  std::string actuall_path;

  int result;
  int flags;
  flags = ARCHIVE_EXTRACT_TIME;
  flags |= ARCHIVE_EXTRACT_PERM;
  flags |= ARCHIVE_EXTRACT_ACL;
  flags |= ARCHIVE_EXTRACT_FFLAGS;
  flags |= ARCHIVE_EXTRACT_XATTR;
  flags |= ARCHIVE_EXTRACT_OWNER;

  // Not supported in libarchive 2.5 which is the one in sles11
  // flags |= ARCHIVE_EXTRACT_MAC_METADATA;
  // flags |= ARCHIVE_EXTRACT_SECURE_NOABSOLUTEPATHS;

  src = archive_read_new();
  assert(src);

  dst = archive_write_disk_new();
  assert(dst);

  // here we coule also accept _all instead of _tar, however it will require
  // more memory and it will provide a way more open interface, maybe too open.
  archive_read_support_format_tar(src);

  archive_write_disk_set_options(dst, flags);
  archive_write_disk_set_standard_lookup(dst);

  result = archive_read_open_filename(src, tarball_path.c_str(), 4096);

  if (result != ARCHIVE_OK) {
    LogCvmfs(kLogUnionFs, kLogStderr, "Impossible to open the archive, abort.");
    return false;
  }

  while (true) {
    result = archive_read_next_header(src, &entry);

    switch (result) {
      case ARCHIVE_FATAL:
        LogCvmfs(kLogUnionFs, kLogStderr,
                 "Fatal error in reading the archive, abort.");
        return false;
        break;

      case ARCHIVE_RETRY:
        LogCvmfs(kLogUnionFs, kLogStderr,
                 "Error in reading, possible to retry but it is not managed ",
                 "yet, abort.");
        return false;
        break;

      case ARCHIVE_EOF:
        return true;
        break;

      case ARCHIVE_WARN:
        LogCvmfs(kLogUnionFs, kLogStderr,
                 "Warning in uncompression reading, going on. \n %s",
                 archive_error_string(src));
        break;

      case ARCHIVE_OK: {
        // save the path of the file to extract
        actuall_path.assign(archive_entry_pathname(entry));
        // set the new path as base untar directory path + the path of the file
        archive_entry_set_pathname(
            entry, (base_untar_directory_path + "/" + actuall_path).c_str());

        result = archive_write_header(dst, entry);

        if (result == ARCHIVE_FATAL) {
          LogCvmfs(kLogUnionFs, kLogStderr,
                   "Fatal Error in writing the archive, abort.");
          return false;
        }

        if (result == ARCHIVE_WARN) {
          LogCvmfs(kLogUnionFs, kLogStderr,
                   "Warning in uncompression reading, going on. \n %s",
                   archive_error_string(dst));
        }

        if (result == ARCHIVE_RETRY) {
          LogCvmfs(kLogUnionFs, kLogStderr,
                   "Error in writing the archive, possible to retry, but it is "
                   "not manages yet, abort.");
          return false;
        }

        if (result == ARCHIVE_OK) {
          result = copy_data(src, dst);
          if (result != ARCHIVE_OK) return false;
        }
      } break;
    }
  }
}

int SyncUnionTarball::copy_data(struct archive *src, struct archive *dst) {
  int result;
  const void *buff;
  size_t size;
  off_t offset;

  while (true) {
    result = archive_read_data_block(src, &buff, &size, &offset);
    if (result == ARCHIVE_EOF) return ARCHIVE_OK;

    if (result == ARCHIVE_RETRY || result == ARCHIVE_FATAL) {
      LogCvmfs(kLogUnionFs, kLogStderr,
               "Error in getting the data to extract the archive, abort.\n%s",
               archive_error_string(src));
      return result;
    }

    result = archive_write_data_block(dst, buff, size, offset);
    if (result == ARCHIVE_RETRY || result == ARCHIVE_FATAL) {
      LogCvmfs(kLogUnionFs, kLogStderr,
               "Error in writing the data to extract the archive, abort.\n%s",
               archive_error_string(src));
      return result;
    }
  }
}

void SyncUnionTarball::Traverse() {
  assert(this->IsInitialized());

  FileSystemTraversal<SyncUnionTarball> traversal(this, working_dir_, true);

  traversal.fn_enter_dir = &SyncUnionTarball::EnterDirectory;
  traversal.fn_leave_dir = &SyncUnionTarball::LeaveDirectory;
  traversal.fn_new_file = &SyncUnionTarball::ProcessRegularFile;
  traversal.fn_new_character_dev = &SyncUnionTarball::ProcessCharacterDevice;
  traversal.fn_new_block_dev = &SyncUnionTarball::ProcessBlockDevice;
  traversal.fn_new_fifo = &SyncUnionTarball::ProcessFifo;
  traversal.fn_new_socket = &SyncUnionTarball::ProcessSocket;
  traversal.fn_ignore_file = &SyncUnionTarball::IgnoreFilePredicate;
  traversal.fn_new_dir_prefix = &SyncUnionTarball::ProcessDirectory;
  traversal.fn_new_symlink = &SyncUnionTarball::ProcessSymlink;

  LogCvmfs(kLogUnionFs, kLogVerboseMsg,
           "Tarball starting traversal "
           "recursion for working directory=[%s]",
           working_dir_.c_str());

  traversal.Recurse(working_dir_);
}

std::string SyncUnionTarball::UnwindWhiteoutFilename(
    const SyncItem &entry) const {
  return entry.filename();
}

bool SyncUnionTarball::IsOpaqueDirectory(const SyncItem &directory) const {
  return false;
}

bool SyncUnionTarball::IsWhiteoutEntry(const SyncItem &entry) const {
  return false;
}

}  // namespace publish
