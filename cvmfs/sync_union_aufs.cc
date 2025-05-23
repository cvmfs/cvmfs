/**
 * This file is part of the CernVM File System
 */

#define __STDC_FORMAT_MACROS

#include "sync_union_aufs.h"

#include <string>

#include "sync_mediator.h"
#include "sync_union.h"
#include "util/fs_traversal.h"

namespace publish {

SyncUnionAufs::SyncUnionAufs(SyncMediator *mediator,
                             const std::string &rdonly_path,
                             const std::string &union_path,
                             const std::string &scratch_path)
    : SyncUnion(mediator, rdonly_path, union_path, scratch_path) {
  // Ignored filenames
  ignore_filenames_.insert(".wh..wh..tmp");
  ignore_filenames_.insert(".wh..wh.plnk");
  ignore_filenames_.insert(".wh..wh.aufs");
  ignore_filenames_.insert(".wh..wh.orph");
  ignore_filenames_.insert(".wh..wh..opq");

  // set the whiteout prefix AUFS precedes for every whiteout file
  whiteout_prefix_ = ".wh.";
}

void SyncUnionAufs::Traverse() {
  assert(this->IsInitialized());

  FileSystemTraversal<SyncUnionAufs> traversal(this, scratch_path(), true);

  traversal.fn_enter_dir = &SyncUnionAufs::EnterDirectory;
  traversal.fn_leave_dir = &SyncUnionAufs::LeaveDirectory;
  traversal.fn_new_file = &SyncUnionAufs::ProcessRegularFile;
  traversal.fn_ignore_file = &SyncUnionAufs::IgnoreFilePredicate;
  traversal.fn_new_dir_prefix = &SyncUnionAufs::ProcessDirectory;
  traversal.fn_new_symlink = &SyncUnionAufs::ProcessSymlink;
  traversal.fn_new_character_dev = &SyncUnionAufs::ProcessCharacterDevice;
  traversal.fn_new_block_dev = &SyncUnionAufs::ProcessBlockDevice;
  traversal.fn_new_fifo = &SyncUnionAufs::ProcessFifo;
  traversal.fn_new_socket = &SyncUnionAufs::ProcessSocket;
  LogCvmfs(kLogUnionFs, kLogVerboseMsg,
           "Aufs starting traversal "
           "recursion for scratch_path=[%s] with external data set to %d",
           scratch_path().c_str(), mediator_->IsExternalData());

  traversal.Recurse(scratch_path());
}

bool SyncUnionAufs::IsWhiteoutEntry(SharedPtr<SyncItem> entry) const {
  return entry->filename().substr(0, whiteout_prefix_.length())
         == whiteout_prefix_;
}

bool SyncUnionAufs::IsOpaqueDirectory(SharedPtr<SyncItem> directory) const {
  return FileExists(directory->GetScratchPath() + "/.wh..wh..opq");
}

string SyncUnionAufs::UnwindWhiteoutFilename(SharedPtr<SyncItem> entry) const {
  const std::string &filename = entry->filename();
  return filename.substr(whiteout_prefix_.length());
}

bool SyncUnionAufs::IgnoreFilePredicate(const string &parent_dir,
                                        const string &filename) {
  return SyncUnion::IgnoreFilePredicate(parent_dir, filename)
         || (ignore_filenames_.find(filename) != ignore_filenames_.end());
}
}  // namespace publish
