/**
 * This file is part of the CernVM File System
 */

#define __STDC_FORMAT_MACROS

#include "sync_union.h"

#include <alloca.h>
#include <errno.h>
#include <sys/capability.h>
#include <unistd.h>

#include <vector>

#include "fs_traversal.h"
#include "logging.h"
#include "platform.h"
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
  mediator_(mediator),
  initialized_(false) {}


bool SyncUnion::Initialize() {
  mediator_->RegisterUnionEngine(this);
  initialized_ = true;
  return true;
}


SyncItem SyncUnion::CreateSyncItem(const std::string  &relative_parent_path,
                                   const std::string  &filename,
                                   const SyncItemType  entry_type) const {
  SyncItem entry(relative_parent_path, filename, this, entry_type);
  PreprocessSyncItem(&entry);
  if (entry_type == kItemFile) {
    entry.SetExternalData(mediator_->IsExternalData());
    entry.SetCompressionAlgorithm(mediator_->GetCompressionAlgorithm());
  }
  return entry;
}


void SyncUnion::PreprocessSyncItem(SyncItem *entry) const {
  if (IsWhiteoutEntry(*entry)) {
    entry->MarkAsWhiteout(UnwindWhiteoutFilename(*entry));
  }

  if (IsOpaqueDirectory(*entry)) {
    entry->MarkAsOpaqueDirectory();
  }
}


bool SyncUnion::ProcessDirectory(const string &parent_dir,
                                 const string &dir_name)
{
  LogCvmfs(kLogUnionFs, kLogDebug, "SyncUnion::ProcessDirectory(%s, %s)",
           parent_dir.c_str(), dir_name.c_str());
  SyncItem entry = CreateSyncItem(parent_dir, dir_name, kItemDir);

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
  SyncItem entry = CreateSyncItem(parent_dir, filename, kItemFile);
  ProcessFile(entry);
}


void SyncUnion::ProcessSymlink(const string &parent_dir,
                               const string &link_name)
{
  LogCvmfs(kLogUnionFs, kLogDebug, "SyncUnion::ProcessSymlink(%s, %s)",
           parent_dir.c_str(), link_name.c_str());
  SyncItem entry = CreateSyncItem(parent_dir, link_name, kItemSymlink);
  ProcessFile(entry);
}


void SyncUnion::ProcessFile(const SyncItem &entry) {
  LogCvmfs(kLogUnionFs, kLogDebug, "SyncUnion::ProcessFile(%s)",
           entry.filename().c_str());
  if (entry.IsWhiteout()) {
    mediator_->Remove(entry);
  } else {
    if (entry.IsNew()) {
      LogCvmfs(kLogUnionFs, kLogVerboseMsg, "processing file [%s] as new (add)",
               entry.filename().c_str());
      mediator_->Add(entry);
    } else {
      LogCvmfs(kLogUnionFs, kLogVerboseMsg,
               "processing file [%s] as existing (touch)",
               entry.filename().c_str());
      mediator_->Touch(entry);
    }
  }
}


void SyncUnion::EnterDirectory(const string &parent_dir,
                               const string &dir_name)
{
  SyncItem entry = CreateSyncItem(parent_dir, dir_name, kItemDir);
  mediator_->EnterDirectory(entry);
}


void SyncUnion::LeaveDirectory(const string &parent_dir,
                               const string &dir_name)
{
  SyncItem entry = CreateSyncItem(parent_dir, dir_name, kItemDir);
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
  assert(this->IsInitialized());

  FileSystemTraversal<SyncUnionAufs> traversal(this, scratch_path(), true);

  traversal.fn_enter_dir      = &SyncUnionAufs::EnterDirectory;
  traversal.fn_leave_dir      = &SyncUnionAufs::LeaveDirectory;
  traversal.fn_new_file       = &SyncUnionAufs::ProcessRegularFile;
  traversal.fn_ignore_file    = &SyncUnionAufs::IgnoreFilePredicate;
  traversal.fn_new_dir_prefix = &SyncUnionAufs::ProcessDirectory;
  traversal.fn_new_symlink    = &SyncUnionAufs::ProcessSymlink;
  LogCvmfs(kLogUnionFs, kLogVerboseMsg, "Aufs starting traversal "
           "recursion for scratch_path=[%s] with external data set to %d",
           scratch_path().c_str(),
           mediator_->IsExternalData());

  traversal.Recurse(scratch_path());
}


bool SyncUnionAufs::IsWhiteoutEntry(const SyncItem &entry) const {
  return entry.filename().substr(0, whiteout_prefix_.length()) ==
         whiteout_prefix_;
}


bool SyncUnionAufs::IsOpaqueDirectory(const SyncItem &directory) const {
  return FileExists(directory.GetScratchPath() + "/.wh..wh..opq");
}


string SyncUnionAufs::UnwindWhiteoutFilename(const SyncItem &entry) const {
  const std::string &filename = entry.filename();
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
                                       const string &scratch_path)
  : SyncUnion(mediator, rdonly_path, union_path, scratch_path)
  , hardlink_lower_inode_(0)
{}


bool SyncUnionOverlayfs::Initialize() {
  // trying to obtain CAP_SYS_ADMIN to read 'trusted' xattrs in the scratch
  // directory of an OverlayFS installation
  return ObtainSysAdminCapability() && SyncUnion::Initialize();
}


bool ObtainSysAdminCapabilityInternal(cap_t caps) {
  /*const*/ cap_value_t cap = CAP_SYS_ADMIN;  // is non-const as cap_set_flag()
                                              // expects a non-const pointer
                                              // on RHEL 5 and older

  // do sanity-check if supported in <sys/capability.h> otherwise just pray...
  // Note: CAP_SYS_ADMIN is a rather common capability and is very likely to be
  //       supported by all our target systems. If it is not, one of the next
  //       commands will fail with a less descriptive error message.
  #ifdef CAP_IS_SUPPORTED
  if (!CAP_IS_SUPPORTED(cap)) {
    LogCvmfs(kLogUnionFs, kLogStderr, "System doesn't support CAP_SYS_ADMIN");
    return false;
  }
  #endif

  if (caps == NULL) {
    LogCvmfs(kLogUnionFs, kLogStderr, "Failed to obtain capability state "
                                      "of current process (errno: %d)",
                                      errno);
    return false;
  }

  cap_flag_value_t cap_state;
  if (cap_get_flag(caps, cap, CAP_EFFECTIVE, &cap_state) != 0) {
    LogCvmfs(kLogUnionFs, kLogStderr, "Failed to check effective set for "
                                      "CAP_SYS_ADMIN (errno: %d)",
                                      errno);
    return false;
  }

  if (cap_state == CAP_SET) {
    LogCvmfs(kLogUnionFs, kLogDebug, "CAP_SYS_ADMIN is already effective");
    return true;
  }

  if (cap_get_flag(caps, cap, CAP_PERMITTED, &cap_state) != 0) {
    LogCvmfs(kLogUnionFs, kLogStderr, "Failed to check permitted set for "
                                      "CAP_SYS_ADMIN (errno: %d)",
                                      errno);
    return false;
  }

  if (cap_state != CAP_SET) {
    LogCvmfs(kLogUnionFs, kLogStderr, "CAP_SYS_ADMIN cannot be obtained. It's "
                                      "not in the process's permitted-set.");
    return false;
  }

  if (cap_set_flag(caps, CAP_EFFECTIVE, 1, &cap, CAP_SET) != 0) {
    LogCvmfs(kLogUnionFs, kLogStderr, "Cannot set CAP_SYS_ADMIN as effective "
                                      "for the current process (errno: %d)",
                                      errno);
    return false;
  }

  if (cap_set_proc(caps) != 0) {
    LogCvmfs(kLogUnionFs, kLogStderr, "Cannot reset capabilities for current "
                                      "process (errno: %d)",
                                      errno);
    return false;
  }

  LogCvmfs(kLogUnionFs, kLogDebug, "Successfully obtained CAP_SYS_ADMIN");
  return true;
}


bool SyncUnionOverlayfs::ObtainSysAdminCapability() const {
  cap_t caps = cap_get_proc();
  const bool result = ObtainSysAdminCapabilityInternal(caps);
  cap_free(caps);
  return result;
}


void SyncUnionOverlayfs::PreprocessSyncItem(SyncItem *entry) const {
  SyncUnion::PreprocessSyncItem(entry);
  if (entry->IsGraftMarker() || entry->IsWhiteout() || entry->IsDirectory()) {
    return;
  }

  CheckForBrokenHardlink(*entry);
  MaskFileHardlinks(entry);
}


void SyncUnionOverlayfs::CheckForBrokenHardlink(const SyncItem &entry) const {
  if (!entry.IsNew()        &&
      !entry.WasDirectory() &&
       entry.GetRdOnlyLinkcount() > 1) {
    LogCvmfs(kLogPublish, kLogStderr, "OverlayFS has copied-up a file (%s) "
                                      "with existing hardlinks in lowerdir "
                                      "(linkcount %d). OverlayFS cannot handle "
                                      "hardlinks and would produce "
                                      "inconsistencies. \n\n"
                                      "Consider running this command: \n"
                                      "  cvmfs_server eliminate-hardlinks\n\n"
                                      "Aborting..." ,
             entry.GetUnionPath().c_str(), entry.GetRdOnlyLinkcount());
    abort();
  }
}

void SyncUnionOverlayfs::MaskFileHardlinks(SyncItem *entry) const {
  assert(entry->IsRegularFile() || entry->IsSymlink());
  if (entry->GetUnionLinkcount() > 1) {
    LogCvmfs(kLogPublish, kLogStderr, "Warning: Found file with linkcount > 1 "
                                      "(%s). We will break up these hardlinks.",
                                      entry->GetUnionPath().c_str());
    entry->MaskHardlink();
  }
}


void SyncUnionOverlayfs::Traverse() {
  assert(this->IsInitialized());

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
 * Checks if a given file path has a specified extended attribute attached.
 *
 * @param[in] path       to the file to be checked
 * @param[in] attr_name  fully qualified name of the extend attribute
 *                       (i.e. trusted.overlay.opaque)
 * @return               true if attribute is found
 */
bool SyncUnionOverlayfs::HasXattr(string const &path, string const &attr_name) {
  // TODO(reneme): it is quite heavy-weight to allocate an object that contains
  //               an std::map<> just to check if an xattr is there...
  UniquePtr<XattrList> xattrs(XattrList::CreateFromFile(path));
  assert(xattrs);

  std::vector<std::string> attrs = xattrs->ListKeys();
  std::vector<std::string>::const_iterator i    = attrs.begin();
  std::vector<std::string>::const_iterator iend = attrs.end();
  LogCvmfs(kLogCvmfs, kLogDebug, "Attrs:");
  for (; i != iend; ++i) {
    LogCvmfs(kLogCvmfs, kLogDebug, "Attr: %s", i->c_str());
  }

  return xattrs && xattrs->Has(attr_name);
}


bool SyncUnionOverlayfs::IsWhiteoutEntry(const SyncItem &entry) const {
  /**
   * There seem to be two versions of overlayfs out there and in production:
   * 1. whiteouts are 'character device' files
   * 2. whiteouts are symlinks pointing to '(overlay-whiteout)'
   */
  return entry.IsCharacterDevice() ||
        (entry.IsSymlink() && IsWhiteoutSymlinkPath(entry.GetScratchPath()));
}


bool SyncUnionOverlayfs::IsWhiteoutSymlinkPath(const string &path) const {
  const bool is_whiteout = ReadlinkEquals(path, "(overlay-whiteout)");
  // TODO(reneme): check for the xattr trusted.overlay.whiteout
  //         Note: This requires CAP_SYS_ADMIN or root... >.<
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
  const std::string path = directory.GetScratchPath();
  return DirectoryExists(path) && IsOpaqueDirPath(path);
}


bool SyncUnionOverlayfs::IsOpaqueDirPath(const string &path) const {
  bool is_opaque = HasXattr(path.c_str(), "trusted.overlay.opaque");
  if (is_opaque) {
    LogCvmfs(kLogUnionFs, kLogDebug, "OverlayFS [%s] has opaque xattr",
             path.c_str());
  }
  return is_opaque;
}


string SyncUnionOverlayfs::UnwindWhiteoutFilename(const SyncItem &entry) const {
  return entry.filename();
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
  SyncItem entry = CreateSyncItem(parent_dir, filename, kItemCharacterDevice);
  ProcessFile(entry);
}

}  // namespace publish
