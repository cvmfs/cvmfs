/**
 * This file is part of the CernVM File System.
 */

#include "catalog_counters.h"

#include "directory_entry.h"
#include "util/exception.h"

namespace catalog {

void DeltaCounters::ApplyDelta(const DirectoryEntry &dirent, const int delta) {
  if (dirent.IsRegular()) {
    self.regular_files += delta;
    self.file_size += delta * dirent.size();
    if (dirent.IsChunkedFile()) {
      self.chunked_files += delta;
      self.chunked_file_size += delta * dirent.size();
    }
    if (dirent.IsExternalFile()) {
      self.externals += delta;
      self.external_file_size += delta * dirent.size();
    }
  } else if (dirent.IsLink()) {
    self.symlinks += delta;
  } else if (dirent.IsSpecial()) {
    self.specials += delta;
  } else if (dirent.IsDirectory()) {
    self.directories += delta;
  } else {
    PANIC(NULL);
  }
  if (dirent.HasXattrs()) {
    self.xattrs += delta;
  }
}


void DeltaCounters::PopulateToParent(DeltaCounters *parent) const {
  parent->subtree.Add(self);
  parent->subtree.Add(subtree);
}

void DeltaCounters::RemoveFromSubtree(const DeltaCounters &child) {
  subtree.Subtract(child.self);
  subtree.Subtract(child.subtree);
}


void Counters::ApplyDelta(const DeltaCounters &delta) {
  self.Add(delta.self);
  subtree.Add(delta.subtree);
}


void Counters::AddAsSubtree(DeltaCounters *delta) const {
  delta->subtree.Add(self);
  delta->subtree.Add(subtree);
}

void Counters::MergeIntoParent(DeltaCounters *parent_delta) const {
  parent_delta->self.Add(self);
  parent_delta->subtree.Subtract(self);
}


Counters_t Counters::GetSelfEntries() const {
  return self.regular_files + self.symlinks + self.specials + self.directories;
}


Counters_t Counters::GetSubtreeEntries() const {
  return subtree.regular_files + subtree.symlinks + subtree.specials
         + subtree.directories;
}


Counters_t Counters::GetAllEntries() const {
  return GetSelfEntries() + GetSubtreeEntries();
}


DeltaCounters Counters::Diff(const Counters &from, const Counters &to) {
  DeltaCounters result;
  result.self.Add(to.self);
  result.subtree.Add(to.subtree);
  result.self.Subtract(from.self);
  result.subtree.Subtract(from.subtree);
  return result;
}

}  // namespace catalog
