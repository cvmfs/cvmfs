/**
 * This file is part of the CernVM File System
 */

#include "sync_union.h"

#include "util.h"
#include "fs_traversal.h"
#include "sync_item.h"
#include "sync_mediator.h"

using namespace std;  // NOLINT

namespace publish {

SyncUnion::SyncUnion(SyncMediator *mediator,
                     const std::string &rdonly_path,
                     const std::string &union_path,
                     const std::string &scratch_path) :
  rdonly_path_(rdonly_path),
  scratch_path_(scratch_path),
  union_path_(union_path),
  mediator_(mediator)
{
  mediator_->RegisterUnionEngine(this);
}


bool SyncUnion::ProcessDirectory(const string &parent_dir,
                                 const string &dir_name)
{
  SyncItem entry(parent_dir, dir_name, kItemDir, this);

  if (entry.IsNew()) {
    mediator_->Add(entry);
    return false; // <-- recursion will stop here... all content of new directory
                  //     will be added later on by the SyncMediator
  } else { // directory already existed...
    if (entry.IsOpaqueDirectory()) { // was directory completely overwritten?
      mediator_->Replace(entry);
      return false; // <-- replace does not need any further recursion
    } else { // directory was just changed internally... only touch needed
      mediator_->Touch(entry);
      return true;
    }
  }
}


void SyncUnion::ProcessRegularFile(const string &parent_dir,
                                   const string &filename)
{
  SyncItem entry(parent_dir, filename, kItemFile, this);
  ProcessFile(entry);
}


void SyncUnion::ProcessSymlink(const string &parent_dir,
                               const string &link_name)
{
  SyncItem entry(parent_dir, link_name, kItemSymlink, this);
  ProcessFile(entry);
}


void SyncUnion::ProcessFile(SyncItem &entry) {
  // Process whiteout prefix
  if (IsWhiteoutEntry(entry)) {
    string actual_filename = UnwindWhiteoutFilename(entry.filename());
    entry.MarkAsWhiteout(actual_filename);
    mediator_->Remove(entry);
  } else {
    // Process normal file
    if (entry.IsNew()) {
      mediator_->Add(entry);
    } else {
      mediator_->Touch(entry);
    }
  }
}


void SyncUnion::EnterDirectory(const string &parent_dir,
                               const string &dir_name)
{
  SyncItem entry(parent_dir, dir_name, kItemDir, this);
  mediator_->EnterDirectory(entry);
}


void SyncUnion::LeaveDirectory(const string &parent_dir,
                               const string &dir_name)
{
  SyncItem entry(parent_dir, dir_name, kItemDir, this);
	mediator_->LeaveDirectory(entry);
}


SyncUnionAufs::SyncUnionAufs(SyncMediator *mediator,
                             const std::string &rdonly_path,
                             const std::string &union_path,
                             const std::string &scratch_path) :
SyncUnion(mediator, rdonly_path, union_path, scratch_path) {

	// Ignored filenames
	ignore_filenames_.insert(".wh..wh..tmp");
	ignore_filenames_.insert(".wh..wh.plnk");
	ignore_filenames_.insert(".wh..wh.aufs");
  ignore_filenames_.insert(".wh..wh.orph");
	ignore_filenames_.insert(".wh..wh..opq");

	// set the whiteout prefix AUFS preceeds for every whiteout file
	whiteout_prefix_ = ".wh.";
}


void SyncUnionAufs::Traverse() {
	FileSystemTraversal<SyncUnionAufs>
    traversal(this, scratch_path(), true);

  traversal.fn_enter_dir = &SyncUnionAufs::EnterDirectory;
	traversal.fn_leave_dir = &SyncUnionAufs::LeaveDirectory;
	traversal.fn_new_file = &SyncUnionAufs::ProcessRegularFile;
	traversal.fn_ignore_file = &SyncUnionAufs::IgnoreFilePredicate;
	traversal.fn_new_dir_prefix = &SyncUnionAufs::ProcessDirectory;
	traversal.fn_new_symlink = &SyncUnionAufs::ProcessSymlink;

	traversal.Recurse(scratch_path());
}


bool SyncUnionAufs::IsWhiteoutEntry(const SyncItem &entry) const {
  return entry.filename().substr(0, whiteout_prefix_.length()) ==
         whiteout_prefix_;
}

bool SyncUnionAufs::IsOpaqueDirectory(const SyncItem &directory) const {
  return FileExists(directory.GetScratchPath() + "/.wh..wh..opq");
}

string SyncUnionAufs::UnwindWhiteoutFilename(const string &filename) const {
  return filename.substr(whiteout_prefix_.length());
}

bool SyncUnionAufs::IgnoreFilePredicate(const string &parent_dir,
                                        const string &filename) {
  return (ignore_filenames_.find(filename) != ignore_filenames_.end());
}


SyncUnionOverlayfs::SyncUnionOverlayfs(SyncMediator *mediator,
                             const std::string &rdonly_path,
                             const std::string &union_path,
                             const std::string &scratch_path) :
SyncUnion(mediator, rdonly_path, union_path, scratch_path) {

}


void SyncUnionOverlayfs::Traverse() {
  FileSystemTraversal<SyncUnionOverlayfs>
    traversal(this, scratch_path(), true, this->GetIgnoreFilenames());

  traversal.fn_enter_dir = &SyncUnionOverlayfs::EnterDirectory;
  traversal.fn_leave_dir = &SyncUnionOverlayfs::LeaveDirectory;
  traversal.fn_new_file = &SyncUnionOverlayfs::ProcessRegularFile;
  traversal.fn_new_dir_prefix = &SyncUnionOverlayfs::ProcessDirectory;
  traversal.fn_new_symlink = &SyncUnionOverlayfs::ProcessSymlink;
  
  traversal.Recurse(scratch_path());
}


bool SyncUnionOverlayfs::IsWhiteoutEntry(const SyncItem &entry) const {
  return (entry.IsSymlink() && 
	  (platform_readlink32(entry.GetScratchPath()) == "(overlay-whiteout)") && 
	  (platform_lgetxattr32(entry.GetScratchPath(), "trusted.overlay.whiteout") == "y"));
}

bool SyncUnionOverlayfs::IsOpaqueDirectory(const SyncItem &directory) const {
  return (platform_lgetxattr32(directory.GetScratchPath(), "trusted.overlay.opaque") == "y");
}

string SyncUnionOverlayfs::UnwindWhiteoutFilename(const string &filename) const {
  return filename;
}

set<string> SyncUnionOverlayfs::GetIgnoreFilenames() const {
  std::set<string> empty;
  return empty;
}


}  // namespace sync
