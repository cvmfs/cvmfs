/**
 * This file is part of the CernVM file system.
 */

#include <gtest/gtest.h>

#include "statistics_database.h"

using namespace std;  // NOLINT

class T_StatisticsSql : public ::testing::Test {
 protected:
  virtual void SetUp() { }
};

static void RevertToRevision1(StatisticsDatabase *db) {
  // SQLite does not support deleting columns, so instead of running the
  // ALTERs, drop the old table and create a new one from scratch

  ASSERT_TRUE(
      sqlite::Sql(db->sqlite_db(), "DROP TABLE publish_statistics;").Execute());
  ASSERT_TRUE(
      sqlite::Sql(
          db->sqlite_db(),
          "CREATE TABLE publish_statistics (publish_id INTEGER PRIMARY KEY,"
          "start_time TEXT,finished_time TEXT,files_added INTEGER,"
          "files_removed INTEGER,files_changed INTEGER,duplicated_files "
          "INTEGER,"
          "directories_added INTEGER,directories_removed INTEGER,"
          "directories_changed INTEGER,sz_bytes_added INTEGER,"
          "sz_bytes_removed INTEGER,sz_bytes_uploaded INTEGER);")
          .Execute());

  ASSERT_TRUE(sqlite::Sql(db->sqlite_db(),
                          "ALTER TABLE gc_statistics RENAME COLUMN finish_time "
                          "TO finished_time;")
                  .Execute());
  db->SetProperty("schema_revision", 1);
}

static void RevertToRevision2(StatisticsDatabase *db) {
  ASSERT_TRUE(
      sqlite::Sql(db->sqlite_db(), "DROP TABLE publish_statistics;").Execute());
  ASSERT_TRUE(sqlite::Sql(db->sqlite_db(),
                          "CREATE TABLE publish_statistics ("
                          "publish_id INTEGER PRIMARY KEY,"
                          "start_time TEXT,"
                          "finish_time TEXT,"
                          "revision INTEGER,"
                          "files_added INTEGER,"
                          "files_removed INTEGER,"
                          "files_changed INTEGER,"
                          "chunks_added INTEGER,"
                          "chunks_duplicated INTEGER,"
                          "catalogs_added INTEGER,"
                          "directories_added INTEGER,"
                          "directories_removed INTEGER,"
                          "directories_changed INTEGER,"
                          "symlinks_added INTEGER,"
                          "symlinks_removed INTEGER,"
                          "symlinks_changed INTEGER,"
                          "sz_bytes_added INTEGER,"
                          "sz_bytes_removed INTEGER,"
                          "sz_bytes_uploaded INTEGER,"
                          "sz_catalog_bytes_uploaded INTEGER);")
                  .Execute());

  ASSERT_TRUE(
      sqlite::Sql(db->sqlite_db(), "DROP TABLE gc_statistics;").Execute());
  ASSERT_TRUE(sqlite::Sql(db->sqlite_db(),
                          "CREATE TABLE gc_statistics ("
                          "gc_id INTEGER PRIMARY KEY,"
                          "start_time TEXT,"
                          "finish_time TEXT,"
                          "n_preserved_catalogs INTEGER,"
                          "n_condemned_catalogs INTEGER,"
                          "n_condemned_objects INTEGER,"
                          "sz_condemned_bytes INTEGER);")
                  .Execute());
  db->SetProperty("schema_revision", 2);
}


TEST_F(T_StatisticsSql, SchemaMigration1To3) {
  string path;
  FILE *ftmp = CreateTempFile("./cvmfs_stats.db", 0600, "w+", &path);
  ASSERT_TRUE(ftmp != NULL);
  fclose(ftmp);
  UnlinkGuard unlink_guard(path);

  // Revision 3 --> 1
  {
    UniquePtr<StatisticsDatabase> db(StatisticsDatabase::Create(path));
    ASSERT_TRUE(db.IsValid());
    RevertToRevision2(db.weak_ref());
    RevertToRevision1(db.weak_ref());
  }
  {
    UniquePtr<StatisticsDatabase> db(
        StatisticsDatabase::Open(path, StatisticsDatabase::kOpenReadWrite));
    EXPECT_EQ(StatisticsDatabase::kLatestSchemaRevision, db->schema_revision());

    sqlite::Sql sql1(
        db->sqlite_db(),
        "SELECT revision, finish_time, "
        "chunks_added, chunks_duplicated, symlinks_added, symlinks_removed, "
        "symlinks_changed, catalogs_added, sz_catalog_bytes_uploaded, success "
        "FROM publish_statistics;");
    ASSERT_TRUE(sql1.Execute());

    sqlite::Sql sql2(db->sqlite_db(), "SELECT finish_time FROM gc_statistics;");
    ASSERT_TRUE(sql2.Execute());
  }
}

TEST_F(T_StatisticsSql, SchemaMigration2To3) {
  string path;
  FILE *ftmp = CreateTempFile("./cvmfs_stats.db", 0600, "w+", &path);
  ASSERT_TRUE(ftmp != NULL);
  fclose(ftmp);
  UnlinkGuard unlink_guard(path);

  // Revision 2 --> 3
  {
    UniquePtr<StatisticsDatabase> db(StatisticsDatabase::Create(path));
    ASSERT_TRUE(db.IsValid());
    RevertToRevision2(db.weak_ref());
  }
  {
    UniquePtr<StatisticsDatabase> db(
        StatisticsDatabase::Open(path, StatisticsDatabase::kOpenReadWrite));
    EXPECT_EQ(StatisticsDatabase::kLatestSchemaRevision, db->schema_revision());

    sqlite::Sql sql1(db->sqlite_db(), "SELECT success "
                                      "FROM publish_statistics;");
    ASSERT_TRUE(sql1.Execute());

    sqlite::Sql sql2(db->sqlite_db(), "SELECT success FROM gc_statistics;");
    ASSERT_TRUE(sql2.Execute());
  }
}
