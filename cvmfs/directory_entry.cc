/**
 * This file is part of the CernVM File System.
 */

#include "directory_entry.h"

namespace catalog {

DirectoryEntryBase::Differences DirectoryEntryBase::CompareTo(
  const DirectoryEntryBase &other) const
{
  Differences result = Difference::kIdentical;

  if (name() != other.name()) {
    result |= Difference::kName;
  }
  if (linkcount() != other.linkcount()) {
    result |= Difference::kLinkcount;
  }
  if (size() != other.size()) {
    result |= Difference::kSize;
  }
  if (mode() != other.mode()) {
    result |= Difference::kMode;
  }
  if (mtime() != other.mtime()) {
    result |= Difference::kMtime;
  }
  if (symlink() != other.symlink()) {
    result |= Difference::kSymlink;
  }
  if (checksum() != other.checksum()) {
    result |= Difference::kChecksum;
  }
  if (HasXattrs() != other.HasXattrs()) {
    result |= Difference::kHasXattrsFlag;
  }

  return result;
}

DirectoryEntryBase::Differences DirectoryEntry::CompareTo(
  const DirectoryEntry &other) const
{
  Differences result = DirectoryEntryBase::CompareTo(other);

  if (hardlink_group() != other.hardlink_group()) {
    result |= Difference::kHardlinkGroup;
  }
  if ( (IsNestedCatalogRoot() != other.IsNestedCatalogRoot()) ||
       (IsNestedCatalogMountpoint() != other.IsNestedCatalogMountpoint()) ) {
    result |= Difference::kNestedCatalogTransitionFlags;
  }
  if (IsChunkedFile() != other.IsChunkedFile()) {
    result |= Difference::kChunkedFileFlag;
  }
  if (IsExternalFile() != other.IsExternalFile()) {
    result |= Difference::kExternalFileFlag;
  }
  if (IsBindMountpoint() != other.IsBindMountpoint()) {
    result |= Difference::kBindMountpointFlag;
  }
  if (IsHidden() != other.IsHidden()) {
    result |= Difference::kHiddenFlag;
  }
  if (IsDirectIo() != other.IsDirectIo()) {
    result |= Difference::kDirectIoFlag;
  }

  return result;
}

}  // namespace catalog
