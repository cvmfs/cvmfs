/**
 * This file is part of the CernVM file system.
 */

#include <gtest/gtest.h>

#include "../../cvmfs/catalog_counters.h"
#include "../../cvmfs/catalog_sql.h"
#include "../../cvmfs/util.h"

using namespace std;  // NOLINT

class T_CatalogSql : public ::testing::Test {
 protected:
  virtual void SetUp() {
  }
};

static void RevertToRevision2(catalog::CatalogDatabase *db) {
  ASSERT_TRUE(sqlite::Sql(db->sqlite_db(),
    "DELETE FROM statistics WHERE counter='self_external';").Execute());
  ASSERT_TRUE(sqlite::Sql(db->sqlite_db(), "DELETE FROM statistics WHERE "
    "counter='self_external_file_size';").Execute());
  ASSERT_TRUE(sqlite::Sql(db->sqlite_db(),
    "DELETE FROM statistics WHERE counter='subtree_external';").Execute());
  ASSERT_TRUE(sqlite::Sql(db->sqlite_db(), "DELETE FROM statistics WHERE "
    "counter='subtree_external_file_size';").Execute());
  ASSERT_TRUE(sqlite::Sql(db->sqlite_db(),
    "UPDATE properties SET value=2 WHERE key='schema_revision';").Execute());
}

static void RevertToRevision1(catalog::CatalogDatabase *db) {
  RevertToRevision2(db);

  string table_sql;
  string indexes_sql;
  sqlite::Sql sql_schema(db->sqlite_db(),
    "SELECT sql FROM sqlite_master WHERE tbl_name='catalog';");
  ASSERT_TRUE(sql_schema.FetchRow());
  table_sql = sql_schema.RetrieveString(0);
  while (sql_schema.FetchRow()) {
    if (sql_schema.RetrieveType(0) == SQLITE_TEXT)
      indexes_sql += sql_schema.RetrieveString(0) + "; ";
  }
  string table_sql_r1 = ReplaceAll(table_sql, "xattr BLOB,", "");
  ASSERT_NE(table_sql_r1, table_sql);
  table_sql_r1 = ReplaceAll(table_sql_r1, "CREATE TABLE catalog ",
                            "CREATE TABLE catalog_r1 ");
  table_sql_r1 = ReplaceAll(table_sql_r1, "CREATE TABLE \"catalog\" ",
                            "CREATE TABLE catalog_r1 ");

  ASSERT_TRUE(sqlite::Sql(db->sqlite_db(), table_sql_r1).Execute());
  ASSERT_TRUE(sqlite::Sql(db->sqlite_db(), "DROP TABLE catalog;").Execute());
  ASSERT_TRUE(sqlite::Sql(db->sqlite_db(),
    "ALTER TABLE catalog_r1 RENAME TO catalog;").Execute());
  if (!indexes_sql.empty())
    ASSERT_TRUE(sqlite::Sql(db->sqlite_db(), indexes_sql).Execute());
  ASSERT_TRUE(sqlite::Sql(db->sqlite_db(),
    "DELETE FROM statistics WHERE counter='self_xattr';").Execute());
  ASSERT_TRUE(sqlite::Sql(db->sqlite_db(),
    "DELETE FROM statistics WHERE counter='subtree_xattr';").Execute());
  ASSERT_TRUE(sqlite::Sql(db->sqlite_db(),
    "UPDATE properties SET value=1 WHERE key='schema_revision';").Execute());
}

static void RevertToRevision0(catalog::CatalogDatabase *db) {
  RevertToRevision1(db);

  string table_sql;
  string indexes_sql;
  sqlite::Sql sql_schema(db->sqlite_db(),
    "SELECT sql FROM sqlite_master WHERE tbl_name='nested_catalogs';");
  ASSERT_TRUE(sql_schema.FetchRow());
  table_sql = sql_schema.RetrieveString(0);
  while (sql_schema.FetchRow()) {
    if (sql_schema.RetrieveType(0) == SQLITE_TEXT)
      indexes_sql += sql_schema.RetrieveString(0) + "; ";
  }
  string table_sql_r1 = ReplaceAll(table_sql, "size INTEGER,", "");
  ASSERT_NE(table_sql_r1, table_sql);
  table_sql_r1 = ReplaceAll(table_sql_r1, "CREATE TABLE nested_catalogs ",
                            "CREATE TABLE nested_catalogs_r0 ");
  table_sql_r1 = ReplaceAll(table_sql_r1, "CREATE TABLE \"nested_catalogs\" ",
                            "CREATE TABLE nested_catalogs_r0 ");

  ASSERT_TRUE(sqlite::Sql(db->sqlite_db(), table_sql_r1).Execute());
  ASSERT_TRUE(sqlite::Sql(db->sqlite_db(),
    "DROP TABLE nested_catalogs;").Execute());
  ASSERT_TRUE(sqlite::Sql(db->sqlite_db(),
    "ALTER TABLE nested_catalogs_r0 RENAME TO nested_catalogs;").Execute());
  if (!indexes_sql.empty())
    ASSERT_TRUE(sqlite::Sql(db->sqlite_db(), indexes_sql).Execute());
  ASSERT_TRUE(sqlite::Sql(db->sqlite_db(),
    "DELETE FROM properties WHERE key='schema_revision';").Execute());
}


TEST_F(T_CatalogSql, SchemaMigration) {
  string path;
  FILE *ftmp = CreateTempFile("./cvmfs_ut_catalog_sql", 0600, "w+", &path);
  ASSERT_TRUE(ftmp != NULL);
  fclose(ftmp);
  UnlinkGuard unlink_guard(path);

  // Revision 1 --> 3
  {
    UniquePtr<catalog::CatalogDatabase>
      db(catalog::CatalogDatabase::Create(path));
    ASSERT_TRUE(db.IsValid());
    catalog::Counters counters;
    EXPECT_TRUE(counters.InsertIntoDatabase(*db));
    RevertToRevision1(db.weak_ref());
  }
  {
    UniquePtr<catalog::CatalogDatabase> db(catalog::CatalogDatabase::Open(
      path, catalog::CatalogDatabase::kOpenReadWrite));
    sqlite::Sql sql1(db->sqlite_db(), "SELECT COUNT(xattr) FROM catalog;");
    ASSERT_TRUE(sql1.FetchRow());
    EXPECT_EQ(0, sql1.RetrieveInt(0));
    sqlite::Sql sql2(db->sqlite_db(),
      "SELECT value FROM properties WHERE key='schema_revision'");
    ASSERT_TRUE(sql2.FetchRow());
    EXPECT_EQ(3, sql2.RetrieveInt(0));
    sqlite::Sql sql3(db->sqlite_db(),
      "SELECT value FROM statistics WHERE counter='self_xattr'");
    ASSERT_TRUE(sql3.FetchRow());
    EXPECT_EQ(0, sql3.RetrieveInt(0));
    sqlite::Sql sql4(db->sqlite_db(),
      "SELECT value FROM statistics WHERE counter='subtree_xattr'");
    ASSERT_TRUE(sql4.FetchRow());
    EXPECT_EQ(0, sql4.RetrieveInt(0));
  }

  // Revision 0 --> 3
  {
    UniquePtr<catalog::CatalogDatabase> db(catalog::CatalogDatabase::Open(
      path, catalog::CatalogDatabase::kOpenReadWrite));
    ASSERT_TRUE(db.IsValid());
    RevertToRevision0(db.weak_ref());
  }
  {
    UniquePtr<catalog::CatalogDatabase> db(catalog::CatalogDatabase::Open(
      path, catalog::CatalogDatabase::kOpenReadWrite));
    sqlite::Sql sql1(db->sqlite_db(), "SELECT COUNT(xattr) FROM catalog");
    ASSERT_TRUE(sql1.FetchRow());
    EXPECT_EQ(0, sql1.RetrieveInt(0));
    sqlite::Sql sql2(db->sqlite_db(),
                     "SELECT COUNT(size) FROM nested_catalogs");
    ASSERT_TRUE(sql2.FetchRow());
    EXPECT_EQ(0, sql2.RetrieveInt(0));
    sqlite::Sql sql3(db->sqlite_db(),
      "SELECT value FROM properties WHERE key='schema_revision'");
    ASSERT_TRUE(sql3.FetchRow());
    EXPECT_EQ(3, sql3.RetrieveInt(0));
    sqlite::Sql sql4(db->sqlite_db(),
      "SELECT value FROM statistics WHERE counter='self_xattr'");
    ASSERT_TRUE(sql4.FetchRow());
    EXPECT_EQ(0, sql4.RetrieveInt(0));
    sqlite::Sql sql5(db->sqlite_db(),
      "SELECT value FROM statistics WHERE counter='subtree_xattr'");
    ASSERT_TRUE(sql5.FetchRow());
    EXPECT_EQ(0, sql5.RetrieveInt(0));
    sqlite::Sql sql6(db->sqlite_db(),
      "SELECT value FROM statistics WHERE counter='self_external'");
    ASSERT_TRUE(sql6.FetchRow());
    EXPECT_EQ(0, sql6.RetrieveInt(0));
    sqlite::Sql sql7(db->sqlite_db(),
      "SELECT value FROM statistics WHERE counter='self_external_file_size'");
    ASSERT_TRUE(sql7.FetchRow());
    EXPECT_EQ(0, sql7.RetrieveInt(0));
    sqlite::Sql sql8(db->sqlite_db(),
      "SELECT value FROM statistics WHERE counter='subtree_external'");
    ASSERT_TRUE(sql8.FetchRow());
    EXPECT_EQ(0, sql8.RetrieveInt(0));
    sqlite::Sql sql9(db->sqlite_db(), "SELECT value FROM statistics WHERE "
      "counter='subtree_external_file_size'");
    ASSERT_TRUE(sql9.FetchRow());
    EXPECT_EQ(0, sql9.RetrieveInt(0));
  }
}
