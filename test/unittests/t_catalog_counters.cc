#include <gtest/gtest.h>

#include "testutil.h"

#include "../../cvmfs/catalog_counters.h"

namespace catalog {


class T_CatalogCounters : public ::testing::Test {
 protected:
  DeltaCounters GetFilledDeltaCounters() const {
    DeltaCounters d;
    d.self.regular_files            = 102;
    d.self.symlinks                 = 10;
    d.self.directories              = 7;
    d.self.nested_catalogs          = 2;

    d.subtree.regular_files         = -22;
    d.subtree.symlinks              = 23;
    d.subtree.directories           = 100;
    d.subtree.nested_catalogs       = -1;
    return d;
  }

  Counters GetFilledCounters() const {
    Counters c;
    c.self.regular_files            = 152;
    c.self.symlinks                 = 7;
    c.self.directories              = 12;
    c.self.nested_catalogs          = 4;

    c.subtree.regular_files         = 82;
    c.subtree.symlinks              = 100;
    c.subtree.directories           = 72;
    c.subtree.nested_catalogs       = 2;
    return c;
  }
};


TEST_F(T_CatalogCounters, CounterInitialization) {
  Counters counters;
  EXPECT_EQ (0, counters.GetSelfEntries());
  EXPECT_EQ (0, counters.GetSubtreeEntries());
  EXPECT_EQ (0, counters.GetAllEntries());
}


TEST_F(T_CatalogCounters, DeltaInitialization) {
  DeltaCounters d_counters;;

  // just checking a random sample here...
  EXPECT_EQ (0, d_counters.self.regular_files);
  EXPECT_EQ (0, d_counters.self.nested_catalogs);
  EXPECT_EQ (0, d_counters.subtree.symlinks);
}


TEST_F(T_CatalogCounters, DeltaBasicIncrement) {
  DeltaCounters d_counters;

  DirectoryEntry regular_file = DirectoryEntryTestFactory::RegularFile();
  DirectoryEntry directory    = DirectoryEntryTestFactory::Directory();
  DirectoryEntry symlink      = DirectoryEntryTestFactory::Symlink();
  DirectoryEntry chunked_file = DirectoryEntryTestFactory::ChunkedFile();

  d_counters.Increment(regular_file);
  d_counters.Increment(regular_file);
  d_counters.Increment(regular_file);

  d_counters.Increment(directory);
  d_counters.Increment(directory);

  d_counters.Increment(symlink);
  d_counters.Increment(symlink);
  d_counters.Increment(symlink);

  d_counters.Increment(chunked_file);
  d_counters.Increment(chunked_file);

  EXPECT_EQ (5, d_counters.self.regular_files);
  EXPECT_EQ (2, d_counters.self.directories);
  EXPECT_EQ (3, d_counters.self.symlinks);
}


TEST_F(T_CatalogCounters, DeltaBasicDecrement) {
  DeltaCounters d_counters;
  d_counters.self.regular_files = 10;
  d_counters.self.directories   = 5;
  d_counters.self.symlinks      = 7;

  DirectoryEntry regular_file = DirectoryEntryTestFactory::RegularFile();
  DirectoryEntry directory    = DirectoryEntryTestFactory::Directory();
  DirectoryEntry symlink      = DirectoryEntryTestFactory::Symlink();
  DirectoryEntry chunked_file = DirectoryEntryTestFactory::ChunkedFile();

  d_counters.Decrement(regular_file);
  d_counters.Decrement(regular_file);
  d_counters.Decrement(regular_file);

  d_counters.Decrement(directory);
  d_counters.Decrement(directory);

  d_counters.Decrement(symlink);
  d_counters.Decrement(symlink);
  d_counters.Decrement(symlink);

  d_counters.Decrement(chunked_file);
  d_counters.Decrement(chunked_file);
  d_counters.Decrement(chunked_file);
  d_counters.Decrement(chunked_file);
  d_counters.Decrement(chunked_file);
  d_counters.Decrement(chunked_file);

  EXPECT_EQ (1,  d_counters.self.regular_files);
  EXPECT_EQ (3,  d_counters.self.directories);
  EXPECT_EQ (4,  d_counters.self.symlinks);
}


TEST_F(T_CatalogCounters, FieldsCombinations) {
  typedef TreeCountersBase<int>::Fields<int> IntFields;
  IntFields a;
  IntFields b;

  a.regular_files         = 1;
  a.symlinks              = 9;
  a.directories           = 3;
  a.nested_catalogs       = 11;

  b.regular_files         = 2;
  b.symlinks              = 10;
  b.directories           = 4;
  b.nested_catalogs       = 12;

  IntFields c = a;
  IntFields d = b;

  a.Add(b);

  EXPECT_EQ (3,  a.regular_files);
  EXPECT_EQ (19, a.symlinks);
  EXPECT_EQ (7,  a.directories);
  EXPECT_EQ (23, a.nested_catalogs);

  c.Add(a);
  c.Subtract(b);

  EXPECT_EQ (2,  c.regular_files);
  EXPECT_EQ (18, c.symlinks);
  EXPECT_EQ (6,  c.directories);
  EXPECT_EQ (22, c.nested_catalogs);

  d.Add(c);
  d.Add(c);
  d.Subtract(b);
  d.Subtract(d);

  EXPECT_EQ (0, d.regular_files);
  EXPECT_EQ (0, d.symlinks);
  EXPECT_EQ (0, d.directories);
  EXPECT_EQ (0, d.nested_catalogs);
}


TEST_F(T_CatalogCounters, DeltaPopulateToParent) {
  DeltaCounters d_parent = GetFilledDeltaCounters();

  DeltaCounters d_child;
  d_child.self.regular_files            = 10;
  d_child.self.directories              = 5;
  d_child.subtree.regular_files         = 12;

  d_child.PopulateToParent(d_parent);

  EXPECT_EQ (102, d_parent.self.regular_files);
  EXPECT_EQ (10,  d_parent.self.symlinks);
  EXPECT_EQ (7,   d_parent.self.directories);
  EXPECT_EQ (2,   d_parent.self.nested_catalogs);

  EXPECT_EQ (0,   d_parent.subtree.regular_files);   // self (10) + subtree (12)
  EXPECT_EQ (23,  d_parent.subtree.symlinks);
  EXPECT_EQ (105, d_parent.subtree.directories);
  EXPECT_EQ (-1,  d_parent.subtree.nested_catalogs);
}


TEST_F(T_CatalogCounters, CountersBasic) {
  Counters counters = GetFilledCounters();

  EXPECT_EQ (171, counters.GetSelfEntries());
  EXPECT_EQ (254, counters.GetSubtreeEntries());
  EXPECT_EQ (425, counters.GetAllEntries());
}


TEST_F(T_CatalogCounters, ApplyDeltaToCounters) {
  DeltaCounters d = GetFilledDeltaCounters();
  Counters      c = GetFilledCounters();

  c.ApplyDelta(d);

  EXPECT_EQ (254,  c.self.regular_files);
  EXPECT_EQ (17,   c.self.symlinks);
  EXPECT_EQ (19,   c.self.directories);
  EXPECT_EQ (6,    c.self.nested_catalogs);

  EXPECT_EQ (60,   c.subtree.regular_files);
  EXPECT_EQ (123,  c.subtree.symlinks);
  EXPECT_EQ (172,  c.subtree.directories);
  EXPECT_EQ (1,    c.subtree.nested_catalogs);
}


TEST_F(T_CatalogCounters, MergeIntoParent) {
  Counters      c_child  = GetFilledCounters();
  DeltaCounters d_parent = GetFilledDeltaCounters();

  c_child.MergeIntoParent(d_parent);

  EXPECT_EQ (254,   d_parent.self.regular_files);
  EXPECT_EQ (17,    d_parent.self.symlinks);
  EXPECT_EQ (19,    d_parent.self.directories);
  EXPECT_EQ (6,     d_parent.self.nested_catalogs);

  EXPECT_EQ (-174,  d_parent.subtree.regular_files);
  EXPECT_EQ (16,    d_parent.subtree.symlinks);
  EXPECT_EQ (88,    d_parent.subtree.directories);
  EXPECT_EQ (-5,    d_parent.subtree.nested_catalogs);
}


TEST_F(T_CatalogCounters, AddAsSubtree) {
  Counters      c_child  = GetFilledCounters();
  DeltaCounters d_parent = GetFilledDeltaCounters();

  c_child.AddAsSubtree(d_parent);

  EXPECT_EQ (102,  d_parent.self.regular_files);
  EXPECT_EQ (10,   d_parent.self.symlinks);
  EXPECT_EQ (7,    d_parent.self.directories);
  EXPECT_EQ (2,    d_parent.self.nested_catalogs);

  EXPECT_EQ (212,  d_parent.subtree.regular_files);
  EXPECT_EQ (130,  d_parent.subtree.symlinks);
  EXPECT_EQ (184,  d_parent.subtree.directories);
  EXPECT_EQ (5,    d_parent.subtree.nested_catalogs);
}


TEST_F(T_CatalogCounters, FieldsMap) {
  DeltaCounters d_counters;
  DeltaCounters d_parent;

  DirectoryEntry regular_file = DirectoryEntryTestFactory::RegularFile();
  DirectoryEntry directory    = DirectoryEntryTestFactory::Directory();
  DirectoryEntry symlink      = DirectoryEntryTestFactory::Symlink();
  DirectoryEntry chunked_file = DirectoryEntryTestFactory::ChunkedFile();

  DeltaCounters::FieldsMap map;

  d_counters.Increment(regular_file);
  d_counters.Increment(regular_file);
  d_counters.Increment(regular_file);

  map = d_counters.GetFieldsMap();
  EXPECT_EQ (3, *map["self_regular"]);
  EXPECT_EQ (0, *map["subtree_regular"]);

  d_counters.Increment(directory);
  d_counters.Increment(directory);

  map = d_counters.GetFieldsMap();
  EXPECT_EQ (3, *map["self_regular"]);
  EXPECT_EQ (0, *map["subtree_regular"]);
  EXPECT_EQ (2, *map["self_dir"]);
  EXPECT_EQ (0, *map["subtree_dir"]);

  d_counters.Increment(symlink);
  d_counters.Increment(symlink);
  d_counters.Increment(symlink);

  map = d_counters.GetFieldsMap();
  EXPECT_EQ (3, *map["self_regular"]);
  EXPECT_EQ (0, *map["subtree_regular"]);
  EXPECT_EQ (2, *map["self_dir"]);
  EXPECT_EQ (0, *map["subtree_dir"]);
  EXPECT_EQ (3, *map["self_symlink"]);
  EXPECT_EQ (0, *map["subtree_symlink"]);

  d_counters.Increment(chunked_file);
  d_counters.Increment(chunked_file);

  map = d_counters.GetFieldsMap();
  EXPECT_EQ (5, *map["self_regular"]);
  EXPECT_EQ (0, *map["subtree_regular"]);
  EXPECT_EQ (2, *map["self_dir"]);
  EXPECT_EQ (0, *map["subtree_dir"]);
  EXPECT_EQ (3, *map["self_symlink"]);
  EXPECT_EQ (0, *map["subtree_symlink"]);

  d_counters.PopulateToParent(d_parent);

  map = d_parent.GetFieldsMap();
  EXPECT_EQ (0, *map["self_regular"]);
  EXPECT_EQ (5, *map["subtree_regular"]);
  EXPECT_EQ (0, *map["self_dir"]);
  EXPECT_EQ (2, *map["subtree_dir"]);
  EXPECT_EQ (0, *map["self_symlink"]);
  EXPECT_EQ (3, *map["subtree_symlink"]);
}

}
