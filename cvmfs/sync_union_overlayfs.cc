/**
 * This file is part of the CernVM File System
 */

#define __STDC_FORMAT_MACROS

#include "sync_union_overlayfs.h"

#include <sys/capability.h>

#include <string>
#include <vector>

#include "sync_mediator.h"
#include "sync_union.h"
#include "util/exception.h"
#include "util/fs_traversal.h"
#include "util/shared_ptr.h"

namespace publish {

SyncUnionOverlayfs::SyncUnionOverlayfs(SyncMediator *mediator,
                                       const string &rdonly_path,
                                       const string &union_path,
                                       const string &scratch_path)
    : SyncUnion(mediator, rdonly_path, union_path, scratch_path)
    , hardlink_lower_inode_(0) { }

bool SyncUnionOverlayfs::Initialize() {
  // trying to obtain CAP_SYS_ADMIN to read 'trusted' xattrs in the scratch
  // directory of an OverlayFS installation
  return ObtainSysAdminCapability() && SyncUnion::Initialize();
}

bool ObtainSysAdminCapabilityInternal(cap_t caps) {
  /*const*/ const cap_value_t
      cap = CAP_SYS_ADMIN;  // is non-const as cap_set_flag()
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
    LogCvmfs(kLogUnionFs, kLogStderr,
             "Failed to obtain capability state "
             "of current process (errno: %d)",
             errno);
    return false;
  }

  cap_flag_value_t cap_state;
  if (cap_get_flag(caps, cap, CAP_EFFECTIVE, &cap_state) != 0) {
    LogCvmfs(kLogUnionFs, kLogStderr,
             "Failed to check effective set for "
             "CAP_SYS_ADMIN (errno: %d)",
             errno);
    return false;
  }

  if (cap_state == CAP_SET) {
    LogCvmfs(kLogUnionFs, kLogDebug, "CAP_SYS_ADMIN is already effective");
    return true;
  }

  if (cap_get_flag(caps, cap, CAP_PERMITTED, &cap_state) != 0) {
    LogCvmfs(kLogUnionFs, kLogStderr,
             "Failed to check permitted set for "
             "CAP_SYS_ADMIN (errno: %d)",
             errno);
    return false;
  }

  if (cap_state != CAP_SET) {
    LogCvmfs(kLogUnionFs, kLogStderr,
             "CAP_SYS_ADMIN cannot be obtained. It's "
             "not in the process's permitted-set.");
    return false;
  }

  if (cap_set_flag(caps, CAP_EFFECTIVE, 1, &cap, CAP_SET) != 0) {
    LogCvmfs(kLogUnionFs, kLogStderr,
             "Cannot set CAP_SYS_ADMIN as effective "
             "for the current process (errno: %d)",
             errno);
    return false;
  }

  if (cap_set_proc(caps) != 0) {
    LogCvmfs(kLogUnionFs, kLogStderr,
             "Cannot reset capabilities for current "
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

void SyncUnionOverlayfs::PreprocessSyncItem(SharedPtr<SyncItem> entry) const {
  SyncUnion::PreprocessSyncItem(entry);
  if (entry->IsGraftMarker() || entry->IsWhiteout() || entry->IsDirectory()) {
    return;
  }

  CheckForBrokenHardlink(entry);
  MaskFileHardlinks(entry);
}

void SyncUnionOverlayfs::CheckForBrokenHardlink(
    SharedPtr<SyncItem> entry) const {
  if (!entry->IsNew() && !entry->WasDirectory()
      && entry->GetRdOnlyLinkcount() > 1) {
    PANIC(kLogStderr,
          "OverlayFS has copied-up a file (%s) "
          "with existing hardlinks in lowerdir "
          "(linkcount %d). OverlayFS cannot handle "
          "hardlinks and would produce "
          "inconsistencies. \n\n"
          "Consider running this command: \n"
          "  cvmfs_server eliminate-hardlinks\n\n"
          "Aborting...",
          entry->GetUnionPath().c_str(), entry->GetRdOnlyLinkcount());
  }
}

void SyncUnionOverlayfs::MaskFileHardlinks(SharedPtr<SyncItem> entry) const {
  assert(entry->IsRegularFile() || entry->IsSymlink()
         || entry->IsSpecialFile());
  if (entry->GetUnionLinkcount() > 1) {
    LogCvmfs(kLogPublish, kLogStderr,
             "Warning: Found file with linkcount > 1 "
             "(%s). We will break up these hardlinks.",
             entry->GetUnionPath().c_str());
    entry->MaskHardlink();
  }
}

void SyncUnionOverlayfs::Traverse() {
  assert(this->IsInitialized());

  FileSystemTraversal<SyncUnionOverlayfs> traversal(this, scratch_path(), true);

  traversal.fn_enter_dir = &SyncUnionOverlayfs::EnterDirectory;
  traversal.fn_leave_dir = &SyncUnionOverlayfs::LeaveDirectory;
  traversal.fn_new_file = &SyncUnionOverlayfs::ProcessRegularFile;
  traversal.fn_new_character_dev = &SyncUnionOverlayfs::ProcessCharacterDevice;
  traversal.fn_new_block_dev = &SyncUnionOverlayfs::ProcessBlockDevice;
  traversal.fn_new_fifo = &SyncUnionOverlayfs::ProcessFifo;
  traversal.fn_new_socket = &SyncUnionOverlayfs::ProcessSocket;
  traversal.fn_ignore_file = &SyncUnionOverlayfs::IgnoreFilePredicate;
  traversal.fn_new_dir_prefix = &SyncUnionOverlayfs::ProcessDirectory;
  traversal.fn_new_symlink = &SyncUnionOverlayfs::ProcessSymlink;

  LogCvmfs(kLogUnionFs, kLogVerboseMsg,
           "OverlayFS starting traversal "
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
                                        string const &compare_value) {
  char *buf;
  size_t compare_len;

  // Compare to one more than compare_value length in case the link value
  // begins with compare_value but ends with something else
  compare_len = compare_value.length() + 1;

  // Allocate enough space for compare_len and terminating null
  buf = static_cast<char *>(alloca(compare_len + 1));

  const ssize_t len = ::readlink(path.c_str(), buf, compare_len);
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
  const UniquePtr<XattrList> xattrs(XattrList::CreateFromFile(path));
  assert(xattrs.IsValid());

  std::vector<std::string> attrs = xattrs->ListKeys();
  std::vector<std::string>::const_iterator i = attrs.begin();
  const std::vector<std::string>::const_iterator iend = attrs.end();
  LogCvmfs(kLogCvmfs, kLogDebug, "Attrs:");
  for (; i != iend; ++i) {
    LogCvmfs(kLogCvmfs, kLogDebug, "Attr: %s", i->c_str());
  }

  return xattrs.IsValid() && xattrs->Has(attr_name);
}

bool SyncUnionOverlayfs::IsWhiteoutEntry(SharedPtr<SyncItem> entry) const {
  /**
   * There seem to be two versions of overlayfs out there and in production:
   * 1. whiteouts are 'character device' files
   * 2. whiteouts are symlinks pointing to '(overlay-whiteout)'
   * 3. whiteouts are marked as .wh. (as in aufs)
   */

  const bool is_chardev_whiteout = entry->IsCharacterDevice()
                                   && entry->GetRdevMajor() == 0
                                   && entry->GetRdevMinor() == 0;
  if (is_chardev_whiteout)
    return true;

  const std::string whiteout_prefix_ = ".wh.";
  const bool has_wh_prefix = HasPrefix(entry->filename().c_str(),
                                       whiteout_prefix_, true);
  if (has_wh_prefix)
    return true;

  const bool is_symlink_whiteout = entry->IsSymlink()
                                   && IsWhiteoutSymlinkPath(
                                       entry->GetScratchPath());
  if (is_symlink_whiteout)
    return true;

  return false;
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

bool SyncUnionOverlayfs::IsOpaqueDirectory(
    SharedPtr<SyncItem> directory) const {
  const std::string path = directory->GetScratchPath();
  return DirectoryExists(path) && IsOpaqueDirPath(path);
}

bool SyncUnionOverlayfs::IsOpaqueDirPath(const string &path) const {
  const bool is_opaque = HasXattr(path.c_str(), "trusted.overlay.opaque");
  if (is_opaque) {
    LogCvmfs(kLogUnionFs, kLogDebug, "OverlayFS [%s] has opaque xattr",
             path.c_str());
  }
  return is_opaque;
}

string SyncUnionOverlayfs::UnwindWhiteoutFilename(
    SharedPtr<SyncItem> entry) const {
  const std::string whiteout_prefix_ = ".wh.";

  if (HasPrefix(entry->filename().c_str(), whiteout_prefix_, true)) {
    return entry->filename().substr(whiteout_prefix_.length());
  } else {
    return entry->filename();
  }
}
}  // namespace publish
