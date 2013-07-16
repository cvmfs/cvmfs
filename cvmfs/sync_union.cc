/**
 * This file is part of the CernVM File System
 */

#include "sync_union.h"

#include "util.h"
#include "fs_traversal.h"
#include "sync_item.h"
#include "sync_mediator.h"

#include <alloca.h>
#include <unistd.h>

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
  LogCvmfs(kLogUnion, kLogDebug, "SyncUnion::ProcessDirectory(%s, %s)",
           parent_dir.c_str(), dir_name.c_str());
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
  LogCvmfs(kLogUnion, kLogDebug, "SyncUnion::ProcessRegularFile(%s, %s)",
           parent_dir.c_str(), filename.c_str());
  SyncItem entry(parent_dir, filename, kItemFile, this);
  ProcessFile(entry);
}


void SyncUnion::ProcessSymlink(const string &parent_dir,
                               const string &link_name)
{
  LogCvmfs(kLogUnion, kLogDebug, "SyncUnion::ProcessSymlink(%s, %s)",
           parent_dir.c_str(), link_name.c_str());
  SyncItem entry(parent_dir, link_name, kItemSymlink, this);
  ProcessFile(entry);
}


void SyncUnion::ProcessFile(SyncItem &entry) {
  LogCvmfs(kLogUnion, kLogDebug, "SyncUnion::ProcessFile(%s)",
           entry.filename().c_str());
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
  SyncUnion(mediator, rdonly_path, union_path, scratch_path)
{
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
  FileSystemTraversal<SyncUnionAufs> traversal(this, scratch_path(), true);

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
                                        const string &filename)
{
  return (ignore_filenames_.find(filename) != ignore_filenames_.end());
}


SyncUnionOverlayfs::SyncUnionOverlayfs(SyncMediator *mediator,
                                       const std::string &rdonly_path,
                                       const std::string &union_path,
                                       const std::string &scratch_path) :
  SyncUnion(mediator, rdonly_path, union_path, scratch_path) {
  
}
  
  void SyncUnionOverlayfs::ProcessFile(SyncItem &entry) {
    LogCvmfs(kLogUnion, kLogDebug, "SyncUnionOverlayfs::ProcessFile(%s)",
             entry.filename().c_str());

    // if lower-level file exists and has multiple hard links, warn user 
    if (!entry.IsNew() && entry.GetRdOnlyLinkcount() > 1) {
    
      LogCvmfs(kLogPublish, kLogVerboseMsg, 
               "OverlayFS have copied-up file %s with "
               "existing hardlinks in lowerdir.",
               entry.GetUnionPath().c_str());

      hardlink_lower_inode_ = entry.GetRdOnlyInode();
      hardlink_lower_files_.clear();

      std::string rdonly_parent_dir = GetParentPath(entry.GetRdOnlyPath());
      std::string scratch_parent_dir = GetParentPath(entry.GetScratchPath());
      std::string union_parent_dir = GetParentPath(entry.GetUnionPath());

      // Find all hardlinks in lowerdir corresponding to this entry
      // (only check this dir since we don't allow cross-dir hardlinks in CVMFS)
      FileSystemTraversal<SyncUnionOverlayfs> traversal(this, rdonly_path(), false);
      traversal.fn_new_file = &SyncUnionOverlayfs::ProcessFileHardlinkCallback;
      traversal.fn_new_symlink = &SyncUnionOverlayfs::ProcessFileHardlinkCallback;
      traversal.Recurse(rdonly_parent_dir);

      // Should now have hardlink_lower_files_ populated with the files in this hardlink group
      if ( hardlink_lower_files_.size() != entry.GetRdOnlyLinkcount() ) {
        LogCvmfs(kLogUnion, kLogWarning, "Found %u entries in hardlink group for %s in overlayfs lowerdir directory %s, was expecting %u (do the hardlinks span directories?)",
                 hardlink_lower_files_.size(),
		 entry.GetRdOnlyPath().c_str(),
       	         rdonly_parent_dir.c_str(),
                 entry.GetRdOnlyLinkcount());
      } else {
        LogCvmfs(kLogUnion, kLogDebug, "Found %u entries in hardlink group for %s in overlayfs lowerdir directory %s, as expected.",
                 hardlink_lower_files_.size(),
		 entry.GetRdOnlyPath().c_str(),
       	         GetParentPath(entry.GetRdOnlyPath()).c_str());
	// have the expected number of entries in the hardlink group, 
        // check if they are all present in the scratch layer (upperdir)
        for (std::set<std::string>::iterator i = hardlink_lower_files_.begin(); i != hardlink_lower_files_.end(); ++i) {
	  std::string filename = *i;

	  std::string scratch_path = scratch_parent_dir + "/" + filename;
	  std::string union_path = union_parent_dir + "/" + filename;
	  platform_stat64 scratch_stat;
          LogCvmfs(kLogUnion, kLogDebug, "Checking file %s",
		   scratch_path.c_str());
          if ( platform_lstat(scratch_path.c_str(), &scratch_stat) < 0 ) {
	    if ( errno == ENOENT ) {
              // file is not present in scratch, warn, and/or abort
	      LogCvmfs(kLogUnion, kLogWarning, 
		       "[WARNING] a file in the OverlayFS lowerdir (%s) has multiple "
		       "hard links one of which (%s) has been modified in CVMFS.  " 
		       "Due to a limitation in OverlayFS, after this sync the modified "
		       "file would no longer be part of its previous hardlink group "
		       "(CVMFS inode %u).  The sync operation will now be aborted so "
                       "that this issue can be corrected manually by explicitly "
		       "including all files in the entire hardlink group in the "
		       "CVMFS transaction. To restore this hardlink, try something like:\n"
		       "rm %s && ln %s %s\n"
		       "To restore all hardlinks in this group, try something like:\n"
		       "for file in $(find %s -inum %u); do rm ${file} && ln %s ${file}; done",
		       filename.c_str(), 
		       entry.GetUnionPath().c_str(),
		       entry.GetRdOnlyInode(),
		       union_path.c_str(),
		       entry.GetUnionPath().c_str(), union_path.c_str(),
		       union_parent_dir.c_str(), entry.GetRdOnlyInode(), entry.GetUnionPath().c_str()
		       );
	      abort();
	    }
	  }
        }
      }
    }
    
    SyncUnion::ProcessFile(entry);
  }

  void SyncUnionOverlayfs::ProcessFileHardlinkCallback(const string &parent_dir,
                                                       const string &filename) {
    LogCvmfs(kLogUnion, kLogDebug, "SyncUnionOverlayfs::ProcessFileHardlinkCallback(%s, %s)",
             parent_dir.c_str(), filename.c_str());
    SyncItem entry(parent_dir, filename, kItemFile, this);
    if ( entry.GetRdOnlyLinkcount() > 1 ) {
      if ( hardlink_lower_inode_ == entry.GetRdOnlyInode() ) {
        LogCvmfs(kLogUnion, kLogDebug, "SyncUnionOverlayfs::ProcessFileHardlinkCallback "
                 "have member of inode group %u: %s/%s",
                 hardlink_lower_inode_, parent_dir.c_str(), filename.c_str());
        hardlink_lower_files_.insert(entry.filename());
      }
    }
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
    
    LogCvmfs(kLogUnion, kLogVerboseMsg, "OverlayFS starting traversal "
             "recursion for scratch_path=[%s]",
             scratch_path().c_str());
    traversal.Recurse(scratch_path());
  }

  /**
   * Wrapper around readlink to read the value of the symbolic link 
   * and return true if it is equal to the supplied value, or false 
   * otherwise (including if any errors occur)
   *
   * @param[in] path to the symbolic link
   * @param[in] value to compare to link value
   */
  bool SyncUnionOverlayfs::ReadlinkEquals(std::string const &path, 
                                          std::string const &compare_value) {
    char *buf;
    size_t compare_len;
    
    // compare to one more than compare_value length in case the link value 
    // begins with compare_value but ends with something else
    compare_len = compare_value.length() + 1;
    
    // allocate enough space for compare_len and terminating null
    buf = static_cast<char *>(alloca(compare_len+1));
    
    ssize_t len = ::readlink(path.c_str(), buf, compare_len);
    if (len != -1) {
      buf[len] = '\0';
      // have link, return true if it is equal to compare_value
      return (std::string(buf) == compare_value);
    } else {
      // error, return false
#ifdef DEBUGMSG
      printf("SyncUnionOverlayfs::ReadlinkEquals error reading link [%s]: %s\n", 
             path.c_str(), strerror(errno));
#endif
      return false;
    }
  }
  
  /**
   * Wrapper around lgetxattr to read the value of the specified 
   * xattr and return true if it is equal to the supplied value, 
   * or false otherwise (including if any errors occur)
   *
   * @param[in] path to the symbolic link
   * @param[in] name of the attribute for which to compare the value
   * @param[in] value to compare to xattr value
   */
  bool SyncUnionOverlayfs::XattrEquals(std::string const &path, std::string const &attr_name, std::string const &compare_value) {
    return platform_lgetxattr_buflen(path, attr_name, compare_value.length()+1) == compare_value;
  }
  
  bool SyncUnionOverlayfs::IsWhiteoutEntry(const SyncItem &entry) const {
    return (entry.IsSymlink() && IsWhiteoutSymlinkPath(entry.GetScratchPath()));
  }
  
  bool SyncUnionOverlayfs::IsWhiteoutSymlinkPath(const std::string &path) const {
    bool is_whiteout = ReadlinkEquals(path, "(overlay-whiteout)") && XattrEquals(path.c_str(), "trusted.overlay.whiteout", "y");
    if (is_whiteout) {
      LogCvmfs(kLogUnion, kLogDebug, "OverlayFS [%s] is whiteout symlink",
               path.c_str());
    } else {
      LogCvmfs(kLogUnion, kLogDebug, "OverlayFS [%s] is not a whiteout symlink",
               path.c_str());
    }
    return is_whiteout;
  }
  
  bool SyncUnionOverlayfs::IsOpaqueDirectory(const SyncItem &directory) const {
    return (IsOpaqueDirPath(directory.GetScratchPath()));
  }

  bool SyncUnionOverlayfs::IsOpaqueDirPath(const std::string &path) const {
    bool is_opaque = XattrEquals(path.c_str(), "trusted.overlay.opaque", "y");
    if (is_opaque) {
      LogCvmfs(kLogUnion, kLogDebug, "OverlayFS [%s] has opaque xattr", 
               path.c_str());
    }
    return is_opaque;
  }
  
  std::string SyncUnionOverlayfs::UnwindWhiteoutFilename(const std::string &filename) const {
    return filename;
  }
  
  bool SyncUnionOverlayfs::IgnoreFilePredicate(const std::string &parent_dir,
                                               const std::string &filename) {
    // no files need to be ignored for OverlayFS 
    return false;
  }

}  // namespace sync
