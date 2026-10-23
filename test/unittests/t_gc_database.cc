/**
 * This file is part of the CernVM File System.
 *
 * Unit tests for the GC SQLite database helper functions used by
 * cvmfs_swissknife ingest --gc-db.
 */

#include <gtest/gtest.h>

#include <cstdio>
#include <string>
#include <vector>

#include "duplex_sqlite3.h"
#include "util/posix.h"
#include "util/string.h"

// Declarations of the static functions under test.  They live in
// swissknife_ingest.cc – we re-declare them here so we can link against them
// without making them non-static.  An alternative would be to move them to
// a small library; for now we include the .cc directly.
//
// This is the same pattern used by other CernVM-FS unit tests that test
// file-scope helpers.
#include "swissknife_ingest_gc.h"

namespace {

class T_GCDatabase : public ::testing::Test {
 protected:
  void SetUp() override {
    db_path_ = CreateTempPath("/tmp/cvmfs_test_gc", 0600);
    ASSERT_FALSE(db_path_.empty());
    CreateTestDB();
  }

  void TearDown() override {
    if (!db_path_.empty()) {
      unlink(db_path_.c_str());
      // Also remove WAL/SHM files if they exist
      unlink((db_path_ + "-wal").c_str());
      unlink((db_path_ + "-shm").c_str());
    }
  }

  void CreateTestDB() {
    sqlite3 *db = NULL;
    ASSERT_EQ(SQLITE_OK,
              sqlite3_open_v2(db_path_.c_str(), &db,
                              SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE,
                              NULL));
    const char *schema =
        "CREATE TABLE gc_paths ("
        "  id        INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  path      TEXT    NOT NULL UNIQUE,"
        "  category  TEXT    NOT NULL,"
        "  scanned_at TEXT   NOT NULL,"
        "  deleted   INTEGER NOT NULL DEFAULT 0"
        ");"
        "CREATE TABLE repo_metadata ("
        "  id                 INTEGER PRIMARY KEY CHECK (id = 1),"
        "  repo_name          TEXT NOT NULL,"
        "  revision           TEXT NOT NULL,"
        "  root_catalog_hash  TEXT NOT NULL,"
        "  scanned_at         TEXT NOT NULL"
        ");";
    char *err_msg = NULL;
    ASSERT_EQ(SQLITE_OK, sqlite3_exec(db, schema, NULL, NULL, &err_msg))
        << "Schema creation failed: " << (err_msg ? err_msg : "");
    sqlite3_free(err_msg);
    sqlite3_close(db);
  }

  void InsertPaths(const std::vector<std::string> &paths,
                   const std::string &category, int deleted = 0) {
    sqlite3 *db = NULL;
    ASSERT_EQ(SQLITE_OK,
              sqlite3_open_v2(db_path_.c_str(), &db, SQLITE_OPEN_READWRITE,
                              NULL));
    for (size_t i = 0; i < paths.size(); ++i) {
      std::string sql = "INSERT INTO gc_paths (path, category, scanned_at, deleted) "
                        "VALUES ('" + paths[i] + "', '" + category + "', "
                        "'2026-01-01T00:00:00Z', " +
                        (deleted ? "1" : "0") + ")";
      ASSERT_EQ(SQLITE_OK, sqlite3_exec(db, sql.c_str(), NULL, NULL, NULL));
    }
    sqlite3_close(db);
  }

  int CountPending() {
    sqlite3 *db = NULL;
    sqlite3_open_v2(db_path_.c_str(), &db, SQLITE_OPEN_READONLY, NULL);
    sqlite3_stmt *stmt = NULL;
    sqlite3_prepare_v2(
        db, "SELECT COUNT(*) FROM gc_paths WHERE deleted = 0", -1, &stmt,
        NULL);
    sqlite3_step(stmt);
    int count = sqlite3_column_int(stmt, 0);
    sqlite3_finalize(stmt);
    sqlite3_close(db);
    return count;
  }

  int CountDeleted() {
    sqlite3 *db = NULL;
    sqlite3_open_v2(db_path_.c_str(), &db, SQLITE_OPEN_READONLY, NULL);
    sqlite3_stmt *stmt = NULL;
    sqlite3_prepare_v2(
        db, "SELECT COUNT(*) FROM gc_paths WHERE deleted = 1", -1, &stmt,
        NULL);
    sqlite3_step(stmt);
    int count = sqlite3_column_int(stmt, 0);
    sqlite3_finalize(stmt);
    sqlite3_close(db);
    return count;
  }

  std::string db_path_;
};

TEST_F(T_GCDatabase, ReadEmptyDatabase) {
  std::vector<std::string> paths;
  EXPECT_TRUE(ReadGCDatabase(db_path_, &paths));
  EXPECT_EQ(0u, paths.size());
}

TEST_F(T_GCDatabase, ReadPendingPaths) {
  InsertPaths({".layers/sha256/aaa", ".layers/sha256/bbb", ".flat/img1"},
              "layer");
  InsertPaths({".flat/img_deleted"}, "image", /*deleted=*/1);

  std::vector<std::string> paths;
  EXPECT_TRUE(ReadGCDatabase(db_path_, &paths));
  EXPECT_EQ(3u, paths.size());
  EXPECT_EQ(".layers/sha256/aaa", paths[0]);
  EXPECT_EQ(".layers/sha256/bbb", paths[1]);
  EXPECT_EQ(".flat/img1", paths[2]);
}

TEST_F(T_GCDatabase, ReadSkipsDeletedPaths) {
  InsertPaths({"a", "b"}, "layer", /*deleted=*/1);
  InsertPaths({"c"}, "layer", /*deleted=*/0);

  std::vector<std::string> paths;
  EXPECT_TRUE(ReadGCDatabase(db_path_, &paths));
  EXPECT_EQ(1u, paths.size());
  EXPECT_EQ("c", paths[0]);
}

TEST_F(T_GCDatabase, ReadNonexistentDB) {
  std::vector<std::string> paths;
  EXPECT_FALSE(ReadGCDatabase("/nonexistent/path/gc.db", &paths));
}

TEST_F(T_GCDatabase, MarkDeletedBasic) {
  InsertPaths({"x", "y", "z"}, "image");
  EXPECT_EQ(3, CountPending());
  EXPECT_EQ(0, CountDeleted());

  EXPECT_TRUE(MarkGCPathsDeleted(db_path_));

  EXPECT_EQ(0, CountPending());
  EXPECT_EQ(3, CountDeleted());
}

TEST_F(T_GCDatabase, MarkDeletedOnlyAffectsPending) {
  InsertPaths({"already_done"}, "layer", /*deleted=*/1);
  InsertPaths({"still_pending"}, "layer", /*deleted=*/0);

  EXPECT_TRUE(MarkGCPathsDeleted(db_path_));

  EXPECT_EQ(0, CountPending());
  EXPECT_EQ(2, CountDeleted());
}

TEST_F(T_GCDatabase, MarkDeletedEmptyDB) {
  EXPECT_TRUE(MarkGCPathsDeleted(db_path_));
  EXPECT_EQ(0, CountPending());
  EXPECT_EQ(0, CountDeleted());
}

TEST_F(T_GCDatabase, MarkDeletedNonexistentDB) {
  EXPECT_FALSE(MarkGCPathsDeleted("/nonexistent/path/gc.db"));
}

TEST_F(T_GCDatabase, ReadPreservesInsertionOrder) {
  InsertPaths({"z_first", "a_second", "m_third"}, "layer");

  std::vector<std::string> paths;
  EXPECT_TRUE(ReadGCDatabase(db_path_, &paths));
  ASSERT_EQ(3u, paths.size());
  EXPECT_EQ("z_first", paths[0]);
  EXPECT_EQ("a_second", paths[1]);
  EXPECT_EQ("m_third", paths[2]);
}

TEST_F(T_GCDatabase, LargeNumberOfPathsSlow) {
  std::vector<std::string> input;
  for (int i = 0; i < 5000; ++i) {
    input.push_back(".layers/sha256/" + StringifyInt(i));
  }
  InsertPaths(input, "layer");

  std::vector<std::string> paths;
  EXPECT_TRUE(ReadGCDatabase(db_path_, &paths));
  EXPECT_EQ(5000u, paths.size());

  EXPECT_TRUE(MarkGCPathsDeleted(db_path_));
  EXPECT_EQ(0, CountPending());
  EXPECT_EQ(5000, CountDeleted());
}

}  // anonymous namespace
