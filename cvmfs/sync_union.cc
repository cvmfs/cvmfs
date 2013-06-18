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
    LogCvmfs(kLogUnion, kLogVerboseMsg, "processing file [%s] as whiteout of [%s] (remove)",
	     entry.filename().c_str(), actual_filename.c_str());
    entry.MarkAsWhiteout(actual_filename);
    mediator_->Remove(entry);
  } else {
    // Process normal file
    if (entry.IsNew()) {
      LogCvmfs(kLogUnion, kLogVerboseMsg, "processing file [%s] as new (add)",
	       entry.filename().c_str());
      mediator_->Add(entry);
    } else {
      LogCvmfs(kLogUnion, kLogVerboseMsg, "processing file [%s] as existing (touch)",
	       entry.filename().c_str());
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
      traversal(this, scratch_path(), true);
    
    traversal.fn_enter_dir = &SyncUnionOverlayfs::EnterDirectory;
    traversal.fn_leave_dir = &SyncUnionOverlayfs::LeaveDirectory;
    traversal.fn_new_file = &SyncUnionOverlayfs::ProcessRegularFile;
    traversal.fn_ignore_file = &SyncUnionOverlayfs::IgnoreFilePredicate;
    traversal.fn_new_dir_prefix = &SyncUnionOverlayfs::ProcessDirectory;
    traversal.fn_new_symlink = &SyncUnionOverlayfs::ProcessSymlink;
    
    LogCvmfs(kLogUnion, kLogVerboseMsg, "overlayfs starting traversal "
	     "recursion for scratch_path=[%s]",
	     scratch_path().c_str());
    traversal.Recurse(scratch_path());
  }
  
  bool SyncUnionOverlayfs::IsWhiteoutEntry(const SyncItem &entry) const {
    return (entry.IsSymlink() && IsWhiteoutSymlinkPath(entry.GetScratchPath()));
  }
  
  bool SyncUnionOverlayfs::IsWhiteoutSymlinkPath(const std::string &path) const {
    std::string link_name = platform_readlink32(path.c_str());
    std::string whiteout_xattr = platform_lgetxattr32(path.c_str(), "trusted.overlay.whiteout");
    bool is_whiteout = (link_name == "(overlay-whiteout)" && 
			whiteout_xattr == "y");
    if (is_whiteout) {
      LogCvmfs(kLogUnion, kLogDebug, "overlayfs [%s] is whiteout symlink",
	       path.c_str());
    } else {
      LogCvmfs(kLogUnion, kLogDebug, "overlayfs [%s] is not a whiteout symlink link_name=[%s] whiteout_xattr=[%s]",
	       path.c_str(), link_name.c_str(), whiteout_xattr.c_str());
    }
    return is_whiteout;
  }
  
  bool SyncUnionOverlayfs::IsOpaqueDirectory(const SyncItem &directory) const {
    return (IsOpaqueDirPath(directory.GetScratchPath()));
  }
  
  bool SyncUnionOverlayfs::IsOpaqueDirPath(const std::string &path) const {
    bool is_opaque = (platform_lgetxattr32(path.c_str(), "trusted.overlay.opaque") == "y");
    if (is_opaque) {
      LogCvmfs(kLogUnion, kLogDebug, "overlayfs [%s] has opaque xattr", 
	       path.c_str());
    }
    return is_opaque;
  }
  
  std::string SyncUnionOverlayfs::UnwindWhiteoutFilename(const std::string &filename) const {
    return filename;
  }
  
  bool SyncUnionOverlayfs::IgnoreFilePredicate(const std::string &parent_dir,
                                               const std::string &filename) {
    std::string path = scratch_path() + "/" + (parent_dir.empty() ? 
					  filename : 
					  (parent_dir + (filename.empty() ? 
							 "" : 
							 ("/" + filename))));
    LogCvmfs(kLogUnion, kLogDebug, "overlayfs checking whether "
	     "to ignore [%s]",
	     path.c_str());
    
    if (IsOpaqueDirPath(path)) {
      platform_stat64 info;
      int retval = platform_lstat(path.c_str(), &info);
      assert(retval == 0);
      if (S_ISDIR(info.st_mode)) {
	LogCvmfs(kLogUnion, kLogVerboseMsg, "overlayfs ignoring [%s] "
		 "because it is an opaque dir",
		 path.c_str());
	return true;
      } else {
	LogCvmfs(kLogUnion, kLogVerboseMsg, "overlayfs not ignoring [%s] "
		 "because although it has an "
		 "opaque dir xattr, it is not "
		 "a dir",
		 path.c_str());
	return false;
      }
    }
    LogCvmfs(kLogUnion, kLogDebug, "overlayfs not ignoring [%s]",
	     path.c_str());
    return false;
  }
  
  
}  // namespace sync
