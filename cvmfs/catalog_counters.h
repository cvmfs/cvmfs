/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_CATALOG_COUNTERS_H_
#define CVMFS_CATALOG_COUNTERS_H_

#include <stdint.h>

namespace catalog {

class DirectoryEntry;

template<typename FieldT>
class TreeCountersBase {
 protected:
  template<typename T>
  struct Fields {
    Fields() : regular_files(0), symlinks(0), directories(0),
               nested_catalogs(0), chunked_files(0), number_of_file_chunks(0) {}

    template<typename U>
    void Add(const U &other) {
      Combine<U, 1>(other);
    }

    template<typename U>
    void Subtract(const U &other) {
      Combine<U, -1>(other);
    }

    template<typename U, int factor>
    void Combine(const U &other) {
      regular_files         += factor * other.regular_files;
      symlinks              += factor * other.symlinks;
      directories           += factor * other.directories;
      nested_catalogs       += factor * other.nested_catalogs;
      chunked_files         += factor * other.chunked_files;
      number_of_file_chunks += factor * other.number_of_file_chunks;
    }

    T regular_files;
    T symlinks;
    T directories;
    T nested_catalogs;
    T chunked_files;
    T number_of_file_chunks;
  };

 protected:
  void SetZero() {
    self    = Fields<FieldT>();
    subtree = Fields<FieldT>();
  }

 public:
  Fields<FieldT> self;
  Fields<FieldT> subtree;
};


class DeltaCounters : public TreeCountersBase<int64_t> {
  friend class Counters;

 public:
  void PopulateToParent(DeltaCounters &parent) const;
  void Increment(const DirectoryEntry &dirent) { ApplyDelta(dirent,  1); }
  void Decrement(const DirectoryEntry &dirent) { ApplyDelta(dirent, -1); }

 private:
  void ApplyDelta(const DirectoryEntry &dirent, const int delta);
};


class Counters : public TreeCountersBase<uint64_t> {
 public:
  void ApplyDelta(const DeltaCounters &delta);
  void AddAsSubtree(DeltaCounters &delta) const;
  void MergeIntoParent(DeltaCounters &parent_delta) const;
  uint64_t GetSelfEntries() const;
  uint64_t GetSubtreeEntries() const;
  uint64_t GetAllEntries() const;
};

}

#endif /* CVMFS_CATALOG_COUNTERS_H_ */
