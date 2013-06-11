#include "catalog_counters.h"

#include "directory_entry.h"

using namespace catalog;

void DeltaCounters::ApplyDelta(const DirectoryEntry &dirent, const int delta) {
  if (dirent.IsRegular()) {
    self.regular_files += delta;
    if (dirent.IsChunkedFile()) {
      self.chunked_files += delta;
    }
  }
  else if (dirent.IsLink())
    self.symlinks += delta;
  else if (dirent.IsDirectory())
    self.directories += delta;
  else
    assert(false);
}


void DeltaCounters::PopulateToParent(DeltaCounters &parent) const {
  parent.subtree.Add(self);
  parent.subtree.Add(subtree);
}


void Counters::ApplyDelta(const DeltaCounters &delta) {
  self.Add(delta.self);
  subtree.Add(delta.subtree);
}


void Counters::AddAsSubtree(DeltaCounters &delta) const {
  delta.subtree.Add(self);
  delta.subtree.Add(subtree);
}


void Counters::MergeIntoParent(DeltaCounters &parent_delta) const {
  parent_delta.self.Add(self);
  parent_delta.subtree.Subtract(self);
}


uint64_t Counters::GetSelfEntries() const {
  return self.regular_files + self.symlinks + self.directories;
}


uint64_t Counters::GetSubtreeEntries() const {
  return subtree.regular_files + subtree.symlinks + subtree.directories;
}


uint64_t Counters::GetAllEntries() const {
  return GetSelfEntries() + GetSubtreeEntries();
}
