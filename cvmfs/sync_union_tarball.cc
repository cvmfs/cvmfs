/**
 * This file is part of the CernVM File System
 */

#define __STDC_FORMAT_MACROS

#include "sync_union_tarball.h"

#include <cassert>
#include <set>
#include <string>

#include "sync_union.h"

#include "archive.h"
#include "archive_entry.h"

#include "fs_traversal.h"
#include "sync_item.h"
#include "sync_mediator.h"

#include "util/posix.h"

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
 * We traverse the tar and then we keep track (the path inside a set) of each
 * .tar we find, then we go back and repeat the procedure. Should we be worry by
 * tar-bombs? Maybe, we will think about it later.
 */
bool SyncUnionTarball::Initialize() {
  bool result;

  // We save the current working directory
  std::string cwd = GetCurrentWorkingDirectory();
  // Save the absolute path of the tarball
  std::string tarball_absolute_path = GetAbsolutePath(tarball_path_);
  // Create the actual temp directory
  working_dir_ = CreateTempDir(base_directory_);
  if (working_dir_ == "") {
    LogCvmfs(kLogUnionFs, kLogStderr,
             "Impossible to create the destination directory. (errno %d)",
             errno);
    return false;
  }

  // Actually untar the whole archive
  result = UntarPath(working_dir_, tarball_absolute_path);
  return result && SyncUnion::Initialize();
}

bool SyncUnionTarball::UntarPath(const std::string &base_untar_directory_path,
                                 const std::string &tarball_path) {
  struct archive *src;  // source of the archive
  struct archive *dst;  // destination for the untar
  struct archive_entry *entry;

  std::string actual_path;

  int result;
  int flags;
  flags = ARCHIVE_EXTRACT_TIME;
  flags |= ARCHIVE_EXTRACT_PERM;
  flags |= ARCHIVE_EXTRACT_ACL;
  flags |= ARCHIVE_EXTRACT_FFLAGS;
  flags |= ARCHIVE_EXTRACT_XATTR;
  flags |= ARCHIVE_EXTRACT_OWNER;
  flags |= ARCHIVE_EXTRACT_MAC_METADATA;

  bool retry_write_header;
  bool retry_read_header;

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
    LogCvmfs(kLogUnionFs, kLogStderr, "Impossible to open the archive.");
    return false;
  }

  while (true) {
    do {
      retry_read_header = false;
      result = archive_read_next_header(src, &entry);
      switch (result) {
        case ARCHIVE_FATAL: {
          LogCvmfs(kLogUnionFs, kLogStderr,
                   "Fatal error in reading the archive.");
          return false;
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
          return true;
          break;
        }

        case ARCHIVE_WARN: {
          LogCvmfs(kLogUnionFs, kLogStderr,
                   "Warning in uncompression reading, going on. \n %s",
                   archive_error_string(src));
          break;
        }

        case ARCHIVE_OK: {
          actual_path.assign(archive_entry_pathname(entry));
          archive_entry_set_pathname(
              entry, (base_untar_directory_path + "/" + actual_path).c_str());

          do {
            retry_write_header = false;
            result = archive_write_header(dst, entry);

            if (result == ARCHIVE_FATAL) {
              LogCvmfs(kLogUnionFs, kLogStderr,
                       "Fatal Error in writing the archive.");
              return false;
            }

            if (result == ARCHIVE_WARN) {
              LogCvmfs(kLogUnionFs, kLogStderr,
                       "Warning in uncompression reading, going on. \n %s",
                       archive_error_string(dst));
            }

            if (result == ARCHIVE_RETRY) {
              LogCvmfs(kLogUnionFs, kLogStderr,
                       "Error in writing the archive, retrying. \n "
                       "%s",
                       archive_error_string(dst));
              retry_write_header = true;
            }

            if (result == ARCHIVE_OK) {
              result = CopyData(src, dst);
              if (result != ARCHIVE_OK) return false;
            }
          } while (retry_write_header);
          break;
        }
        default:
          return false;
      }
    } while (retry_read_header);
  }
}

int SyncUnionTarball::CopyData(struct archive *src, struct archive *dst) {
  int result;
  const void *buff;
  size_t size;
  off_t offset;
  bool retry_read;
  bool retry_write;

  while (true) {
    retry_read = false;
    do {
      result = archive_read_data_block(src, &buff, &size, &offset);

      switch (result) {
        case ARCHIVE_EOF: {
          return ARCHIVE_OK;
          break;
        }
        case ARCHIVE_FATAL: {
          LogCvmfs(kLogUnionFs, kLogStderr,
                   "Error in getting the data to extract the archive.\n%s",
                   archive_error_string(src));
          return ARCHIVE_FATAL;
          break;
        }
        case ARCHIVE_RETRY: {
          retry_read = true;
          break;
        }
        case ARCHIVE_WARN: {
          LogCvmfs(kLogUnionFs, kLogStderr,
                   "Warning in reading the data from the archive, move on.\n%s",
                   archive_error_string(src));
          retry_read = false;
          break;
        }
        case ARCHIVE_OK:
        default: {
          retry_read = false;
          break;
        }
      }
    } while (retry_read);

    retry_write = false;
    do {
      result = archive_write_data_block(dst, buff, size, offset);

      switch (result) {
        case ARCHIVE_FATAL: {
          LogCvmfs(kLogUnionFs, kLogStderr,
                   "Error in writing the data to disk.\n%s",
                   archive_error_string(src));
          return ARCHIVE_FATAL;
          break;
        }
        case ARCHIVE_RETRY: {
          retry_write = true;
          break;
        }
        case ARCHIVE_WARN: {
          LogCvmfs(kLogUnionFs, kLogStderr,
                   "Warning in reading the data from the archive, move on.\n%s",
                   archive_error_string(src));
          retry_write = false;
          break;
        }
        default:
          retry_write = false;
          break;
      }
    } while (retry_write);
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
