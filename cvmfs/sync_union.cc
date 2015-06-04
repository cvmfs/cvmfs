/**
 * This file is part of the CernVM File System
 */

#define __STDC_FORMAT_MACROS

#include "sync_union.h"

#include <alloca.h>
#include <errno.h>
#include <unistd.h>

// lgetxattr is only required for overlayfs which does not exist on OS X
#ifdef __APPLE__
#define lgetxattr(...) (-1)
#else
#include <attr/xattr.h>
#endif

#include "fs_traversal.h"
#include "logging.h"
#include "platform.h"
#include "sync_item.h"
#include "sync_mediator.h"
#include "util.h"

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
  LogCvmfs(kLogUnionFs, kLogDebug, "SyncUnion::ProcessDirectory(%s, %s)",
           parent_dir.c_str(), dir_name.c_str());
  SyncItem entry(parent_dir, dir_name, this, kItemDir);

  if (entry.IsNew()) {
    mediator_->Add(entry);
    // Recursion stops here. All content of new directory
    // is added later by the SyncMediator
    return false;
  } else {  // directory already existed...
    if (entry.IsOpaqueDirectory()) {  // was directory completely overwritten?
      mediator_->Replace(entry);
      return false;  // <-- replace does not need any further recursion
    } else {  // directory was just changed internally... only touch needed
      mediator_->Touch(entry);
      return true;
    }
  }
}


void SyncUnion::ProcessRegularFile(const string &parent_dir,
                                   const string &filename)
{
  LogCvmfs(kLogUnionFs, kLogDebug, "SyncUnion::ProcessRegularFile(%s, %s)",
           parent_dir.c_str(), filename.c_str());
  SyncItem entry(parent_dir, filename, this, kItemFile);
  ProcessFile(&entry);
}


void SyncUnion::ProcessSymlink(const string &parent_dir,
                               const string &link_name)
{
  LogCvmfs(kLogUnionFs, kLogDebug, "SyncUnion::ProcessSymlink(%s, %s)",
           parent_dir.c_str(), link_name.c_str());
  SyncItem entry(parent_dir, link_name, this, kItemSymlink);
  ProcessFile(&entry);
}


void SyncUnion::ProcessFile(SyncItem *entry) {
  LogCvmfs(kLogUnionFs, kLogDebug, "SyncUnion::ProcessFile(%s)",
           entry->filename().c_str());
  // Process whiteout prefix
  if (IsWhiteoutEntry(*entry)) {
    string actual_filename = UnwindWhiteoutFilename(entry->filename());
    LogCvmfs(kLogUnionFs, kLogVerboseMsg,
             "processing file [%s] as whiteout of [%s] (remove)",
             entry->filename().c_str(), actual_filename.c_str());
    entry->MarkAsWhiteout(actual_filename);
    mediator_->Remove(*entry);
  } else {
    // Process normal file
    if (entry->IsNew()) {
      LogCvmfs(kLogUnionFs, kLogVerboseMsg, "processing file [%s] as new (add)",
               entry->filename().c_str());
      mediator_->Add(*entry);
    } else {
      LogCvmfs(kLogUnionFs, kLogVerboseMsg,
               "processing file [%s] as existing (touch)",
               entry->filename().c_str());
      mediator_->Touch(*entry);
    }
  }
}


void SyncUnion::EnterDirectory(const string &parent_dir,
                               const string &dir_name)
{
  SyncItem entry(parent_dir, dir_name, this, kItemDir);
  mediator_->EnterDirectory(entry);
}


void SyncUnion::LeaveDirectory(const string &parent_dir,
                               const string &dir_name)
{
  SyncItem entry(parent_dir, dir_name, this, kItemDir);
  mediator_->LeaveDirectory(entry);
}


//------------------------------------------------------------------------------


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

  traversal.fn_enter_dir      = &SyncUnionAufs::EnterDirectory;
  traversal.fn_leave_dir      = &SyncUnionAufs::LeaveDirectory;
  traversal.fn_new_file       = &SyncUnionAufs::ProcessRegularFile;
  traversal.fn_ignore_file    = &SyncUnionAufs::IgnoreFilePredicate;
  traversal.fn_new_dir_prefix = &SyncUnionAufs::ProcessDirectory;
  traversal.fn_new_symlink    = &SyncUnionAufs::ProcessSymlink;

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


//------------------------------------------------------------------------------


SyncUnionOverlayfs::SyncUnionOverlayfs(SyncMediator *mediator,
                                       const string &rdonly_path,
                                       const string &union_path,
                                       const string &scratch_path) :
  SyncUnion(mediator, rdonly_path, union_path, scratch_path)
{
  hardlink_lower_inode_ = 0;
}


void SyncUnionOverlayfs::ProcessFile(SyncItem *entry) {
  LogCvmfs(kLogUnionFs, kLogDebug, "SyncUnionOverlayfs::ProcessFile(%s)",
           entry->filename().c_str());

  // If lower-level file exists and has multiple hard links, warn user
  if (!entry->IsNew() && entry->GetRdOnlyLinkcount() > 1) {
    LogCvmfs(kLogPublish, kLogVerboseMsg, "OverlayFS have copied-up file %s "
             "with existing hardlinks in lowerdir.",
             entry->GetUnionPath().c_str());

    hardlink_lower_inode_ = entry->GetRdOnlyInode();
    hardlink_lower_files_.clear();

    string rdonly_parent_dir = GetParentPath(entry->GetRdOnlyPath());
    string scratch_parent_dir = GetParentPath(entry->GetScratchPath());
    string union_parent_dir = GetParentPath(entry->GetUnionPath());

    // Find all hardlinks in lowerdir corresponding to this entry
    // (only check this dir since we don't allow cross-dir hardlinks in CVMFS)
    FileSystemTraversal<SyncUnionOverlayfs>
      traversal(this, rdonly_path(), false);
    traversal.fn_new_file = &SyncUnionOverlayfs::ProcessFileHardlinkCallback;
    traversal.fn_new_symlink = &SyncUnionOverlayfs::ProcessFileHardlinkCallback;
    traversal.Recurse(rdonly_parent_dir);

    // Should now have hardlink_lower_files_ populated with the files
    // in this hardlink group
    if (hardlink_lower_files_.size() != entry->GetRdOnlyLinkcount()) {
      LogCvmfs(kLogUnionFs, kLogWarning, "Found %u entries in hardlink group "
               "for %s in overlayfs lowerdir directory %s, "
               "was expecting %u (do the hardlinks span directories?)",
               hardlink_lower_files_.size(),
               entry->GetRdOnlyPath().c_str(),
               rdonly_parent_dir.c_str(),
               entry->GetRdOnlyLinkcount());
    } else {
      LogCvmfs(kLogUnionFs, kLogDebug, "Found %u entries in hardlink group "
               "for %s in overlayfs lowerdir directory %s, as expected.",
               hardlink_lower_files_.size(),
               entry->GetRdOnlyPath().c_str(),
               GetParentPath(entry->GetRdOnlyPath()).c_str());
      // have the expected number of entries in the hardlink group,
      // check if they are all present in the scratch layer (upperdir)
      for (set<string>::iterator i = hardlink_lower_files_.begin(),
           iend = hardlink_lower_files_.end(); i != iend; ++i)
      {
        string filename = *i;

        string scratch_path = scratch_parent_dir + "/" + filename;
        string union_path = union_parent_dir + "/" + filename;
        platform_stat64 scratch_stat;
        LogCvmfs(kLogUnionFs, kLogDebug, "Checking file %s",
                 scratch_path.c_str());
        if (platform_lstat(scratch_path.c_str(), &scratch_stat) < 0) {
          if (errno == ENOENT) {
            // file is not present in scratch, warn, and/or abort
            LogCvmfs(kLogUnionFs, kLogWarning,
  "[WARNING] a file in the OverlayFS lowerdir (%s) has multiple\n"
  "hard links, at least one of which (%s)\n"
  "has been modified in CernVM-FS.  Due to a limitation in OverlayFS, after\n"
  "this sync the modified file would no longer be part of its previous \n"
  "hardlink group (CernVM-FS inode %"PRIu64").\n"
  "The sync operation will now be aborted so that this issue can be corrected\n"
  "manually -- you must explicitly include all files belonging to this\n"
  "hardlink group in the CernVM-FS transaction (e.g. by touching them if you\n"
  "wish to break the link or linking them if you wish to preserve it).\n"
  "\n"
  "To restore only this hardlink, you could run:\n"
  "rm %s && ln %s %s\n"
  "\n"
  "To find all files that are part of this hardlink group, use:\n"
  "find %s -inum %"PRIu64"\n"
  "\n"
  "To restore all hardlinks in this group, try something like:\n"
  "for file in $(find %s -inum %"PRIu64"); do rm ${file} && ln %s ${file}; done"
  "\n\n"
  "Once you have corrected the issue, run `cvmfs_server publish` again\n",
                     filename.c_str(),
                     entry->GetUnionPath().c_str(),
                     entry->GetRdOnlyInode(),
                     union_path.c_str(),
                     entry->GetUnionPath().c_str(), union_path.c_str(),
                     union_parent_dir.c_str(), entry->GetRdOnlyInode(),
                     union_parent_dir.c_str(), entry->GetRdOnlyInode(),
                     entry->GetUnionPath().c_str());  // LogCvmfs
            abort();
          }  // error == ENOENT
        }  // platform_lstat < 0
      }  // for hardlink_lower_files_
    }  // hardlink_lower_files_.size() == entry->GetRdOnlyLinkcount()
  }  // Is hardlink

  SyncUnion::ProcessFile(entry);
}


void SyncUnionOverlayfs::ProcessFileHardlinkCallback(const string &parent_dir,
                                                     const string &filename)
{
  LogCvmfs(kLogUnionFs, kLogDebug,
           "SyncUnionOverlayfs::ProcessFileHardlinkCallback(%s, %s)",
           parent_dir.c_str(), filename.c_str());
  SyncItem entry(parent_dir, filename, this, kItemFile);
  if (entry.GetRdOnlyLinkcount() > 1) {
    if (hardlink_lower_inode_ == entry.GetRdOnlyInode()) {
      LogCvmfs(kLogUnionFs, kLogDebug,
               "SyncUnionOverlayfs::ProcessFileHardlinkCallback "
               "have member of inode group %u: %s/%s",
               hardlink_lower_inode_, parent_dir.c_str(), filename.c_str());
      hardlink_lower_files_.insert(entry.filename());
    }
  }
}


void SyncUnionOverlayfs::Traverse() {
  FileSystemTraversal<SyncUnionOverlayfs>
    traversal(this, scratch_path(), true);

  traversal.fn_enter_dir          = &SyncUnionOverlayfs::EnterDirectory;
  traversal.fn_leave_dir          = &SyncUnionOverlayfs::LeaveDirectory;
  traversal.fn_new_file           = &SyncUnionOverlayfs::ProcessRegularFile;
  traversal.fn_new_character_dev  = &SyncUnionOverlayfs::ProcessCharacterDevice;
  traversal.fn_ignore_file        = &SyncUnionOverlayfs::IgnoreFilePredicate;
  traversal.fn_new_dir_prefix     = &SyncUnionOverlayfs::ProcessDirectory;
  traversal.fn_new_symlink        = &SyncUnionOverlayfs::ProcessSymlink;

  LogCvmfs(kLogUnionFs, kLogVerboseMsg, "OverlayFS starting traversal "
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
bool SyncUnionOverlayfs::ReadlinkEquals(string const &path,
                                        string const &compare_value)
{
  char *buf;
  size_t compare_len;

  // Compare to one more than compare_value length in case the link value
  // begins with compare_value but ends with something else
  compare_len = compare_value.length() + 1;

  // Allocate enough space for compare_len and terminating null
  buf = static_cast<char *>(alloca(compare_len+1));

  ssize_t len = ::readlink(path.c_str(), buf, compare_len);
  if (len != -1) {
    buf[len] = '\0';
    // have link, return true if it is equal to compare_value
    return (std::string(buf) == compare_value);
  } else {
    // Error, return false
    LogCvmfs(kLogUnionFs, kLogDebug,
             "SyncUnionOverlayfs::ReadlinkEquals error reading link [%s]: %d\n",
             path.c_str(), errno);
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
bool SyncUnionOverlayfs::XattrEquals(string const &path,
                                     string const &attr_name,
                                     string const &compare_value)
{
  const size_t buf_len = compare_value.length()+1;
  char *buf = static_cast<char *>(alloca(buf_len+1));

  ssize_t len = lgetxattr(path.c_str(), attr_name.c_str(), buf, buf_len-1);
  if (len != -1) {
    buf[len] = '\0';
  } else {
    // Error
    LogCvmfs(kLogUnionFs, kLogDebug, "failed to read xattr %s from %s: %d\n",
             attr_name.c_str(), path.c_str(), errno);
    buf[0] = '\0';
  }

  return string(buf) == compare_value;
}


bool SyncUnionOverlayfs::IsWhiteoutEntry(const SyncItem &entry) const {
  return entry.IsCharacterDevice();
}


bool SyncUnionOverlayfs::IsWhiteoutSymlinkPath(const string &path) const {
  bool is_whiteout = ReadlinkEquals(path, "(overlay-whiteout)") &&
                     XattrEquals(path.c_str(), "trusted.overlay.whiteout", "y");
  if (is_whiteout) {
    LogCvmfs(kLogUnionFs, kLogDebug, "OverlayFS [%s] is whiteout symlink",
             path.c_str());
  } else {
    LogCvmfs(kLogUnionFs, kLogDebug, "OverlayFS [%s] is not a whiteout symlink",
             path.c_str());
  }
  return is_whiteout;
}


bool SyncUnionOverlayfs::IsOpaqueDirectory(const SyncItem &directory) const {
  return (IsOpaqueDirPath(directory.GetScratchPath()));
}


bool SyncUnionOverlayfs::IsOpaqueDirPath(const string &path) const {
  bool is_opaque = XattrEquals(path.c_str(), "trusted.overlay.opaque", "y");
  if (is_opaque) {
    LogCvmfs(kLogUnionFs, kLogDebug, "OverlayFS [%s] has opaque xattr",
             path.c_str());
  }
  return is_opaque;
}


string SyncUnionOverlayfs::UnwindWhiteoutFilename(const string &filename) const
{
  return filename;
}


bool SyncUnionOverlayfs::IgnoreFilePredicate(const string &parent_dir,
                                             const string &filename)
{
  // no files need to be ignored for OverlayFS
  return false;
}

void SyncUnionOverlayfs::ProcessCharacterDevice(const std::string &parent_dir,
                                                const std::string &filename) {
  LogCvmfs(kLogUnionFs, kLogDebug,
           "SyncUnionOverlayfs::ProcessCharacterDevice(%s, %s)",
           parent_dir.c_str(), filename.c_str());
  SyncItem entry(parent_dir, filename, kItemCharacterDevice, this);
  ProcessFile(&entry);
}

}  // namespace publish
