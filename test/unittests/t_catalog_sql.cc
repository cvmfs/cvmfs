#include <gtest/gtest.h>

#include "../../cvmfs/catalog_sql.h"
#include "../../cvmfs/util.h"

using namespace std;  // NOLINT

class T_CatalogSql : public ::testing::Test {
 protected:
  virtual void SetUp() {
  }
};

static void RevertToRevision1(catalog::CatalogDatabase *db) {
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
  FILE *ftmp = CreateTempFile("/tmp/cvmfs-test", 0600, "w+", &path);
  ASSERT_TRUE(ftmp != NULL);
  fclose(ftmp);
  UnlinkGuard unlink_guard(path);

  // Revision 1 --> 2
  {
    UniquePtr<catalog::CatalogDatabase>
      db(catalog::CatalogDatabase::Create(path));
    ASSERT_TRUE(db.IsValid());
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
    EXPECT_EQ(2, sql2.RetrieveInt(0));
  }

  // Revision 0 --> 2
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
    EXPECT_EQ(2, sql3.RetrieveInt(0));
  }
}
