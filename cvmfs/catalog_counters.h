/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_CATALOG_COUNTERS_H_
#define CVMFS_CATALOG_COUNTERS_H_

#include <stdint.h>
#include <map>
#include <string>
#include <gtest/gtest_prod.h>

namespace swissknife {
  class CommandCheck;
}

namespace catalog {

class DirectoryEntry;
class Database;

template<typename FieldT>
class TreeCountersBase {
  friend class swissknife::CommandCheck;
  FRIEND_TEST(T_CatalogCounters, FieldsCombinations);
  FRIEND_TEST(T_CatalogCounters, FieldsMap);

 protected:
  typedef std::map<std::string, const FieldT*> FieldsMap;
  template<typename T>
  struct Fields {
    Fields() : regular_files(0), symlinks(0), directories(0),
               nested_catalogs(0) {}

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
    }

    void FillFieldsMap(FieldsMap &map, const std::string &prefix) const {
      map[prefix + "regular"] = &regular_files;
      map[prefix + "symlink"] = &symlinks;
      map[prefix + "dir"]     = &directories;
      map[prefix + "nested"]  = &nested_catalogs;
    }

    T regular_files;
    T symlinks;
    T directories;
    T nested_catalogs;
  };

 public:
  bool ReadFromDatabase(const Database   &database);
  bool WriteToDatabase(const Database    &database) const;
  bool InsertIntoDatabase(const Database &database) const;

  void SetZero();

 protected:
  FieldsMap GetFieldsMap() const;

 public:
  Fields<FieldT> self;
  Fields<FieldT> subtree;
};


typedef int64_t DeltaCounters_t;
class DeltaCounters : public TreeCountersBase<DeltaCounters_t> {
  friend class Counters;

 public:
  void PopulateToParent(DeltaCounters &parent) const;
  void Increment(const DirectoryEntry &dirent) { ApplyDelta(dirent,  1); }
  void Decrement(const DirectoryEntry &dirent) { ApplyDelta(dirent, -1); }

 private:
  void ApplyDelta(const DirectoryEntry &dirent, const int delta);
};


typedef uint64_t Counters_t;
class Counters : public TreeCountersBase<Counters_t> {
 public:
  void ApplyDelta(const DeltaCounters &delta);
  void AddAsSubtree(DeltaCounters &delta) const;
  void MergeIntoParent(DeltaCounters &parent_delta) const;
  Counters_t GetSelfEntries() const;
  Counters_t GetSubtreeEntries() const;
  Counters_t GetAllEntries() const;
};

}

#include "catalog_counters_impl.h"

#endif /* CVMFS_CATALOG_COUNTERS_H_ */
