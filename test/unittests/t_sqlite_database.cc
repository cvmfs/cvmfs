/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <string>

#include "../../cvmfs/sql.h"
#include "../../cvmfs/util.h"

struct RevisionFlags {
  enum T {
    kInitialRevision   = 1,
    kUpdatableRevision = 2,
    kUpdatedRevision   = 3,
    kFailingRevision   = 4,
  };
};

class DummyDatabase : public sqlite::Database<DummyDatabase> {
 public:
  // not const - needs to be adaptable!
  static float        kLatestSchema;
  // not const - needs to be adaptable!
  static unsigned     kLatestSchemaRevision;
  static const float  kLatestCompatibleSchema;

  static bool         compacting_fails;

  bool CreateEmptyDatabase() {
    ++create_empty_db_calls;

  return sqlite::Sql(sqlite_db(),
    "CREATE TABLE foobar (foo TEXT, bar TEXT, "
    "  CONSTRAINT pk_foo PRIMARY KEY (foo))").Execute();
  }

  bool CheckSchemaCompatibility() {
    ++check_compatibility_calls;
    return (schema_version() > kLatestCompatibleSchema - 0.1 &&
            schema_version() < kLatestCompatibleSchema + 0.1);
  }

  bool LiveSchemaUpgradeIfNecessary() {
    ++live_upgrade_calls;
    const unsigned int revision = schema_revision();

    if (revision == RevisionFlags::kInitialRevision) {
      return true;
    }

    if (revision == RevisionFlags::kUpdatableRevision) {
      set_schema_revision(RevisionFlags::kUpdatedRevision);
      StoreSchemaRevision();
      return true;
    }

    if (revision == RevisionFlags::kFailingRevision) {
      return false;
    }

    return false;
  }

  bool CompactDatabase() const {
    ++compact_calls;
    return !compacting_fails;
  }

  ~DummyDatabase() {
    --DummyDatabase::instances;
  }

 protected:
  // TODO(rmeusel): C++11 - constructor inheritance
  friend class sqlite::Database<DummyDatabase>;
  DummyDatabase(const std::string  &filename,
                const OpenMode      open_mode) :
    sqlite::Database<DummyDatabase>(filename, open_mode),
    create_empty_db_calls(0),  check_compatibility_calls(0),
    live_upgrade_calls(0), compact_calls(0)
  {
    ++DummyDatabase::instances;
  }

 public:
  static unsigned int  instances;

  unsigned int         create_empty_db_calls;
  unsigned int         check_compatibility_calls;
  unsigned int         live_upgrade_calls;
  mutable unsigned int compact_calls;
};

const float    DummyDatabase::kLatestCompatibleSchema = 1.0f;
float          DummyDatabase::kLatestSchema           = 1.0f;
unsigned       DummyDatabase::kLatestSchemaRevision   =
  RevisionFlags::kInitialRevision;
unsigned int   DummyDatabase::instances               = 0;
bool           DummyDatabase::compacting_fails        = false;


class T_SQLite_Wrapper : public ::testing::Test {
 protected:
  static const std::string sandbox;

 protected:
  virtual void SetUp() {
    DummyDatabase::kLatestSchema         = 1.0f;
    DummyDatabase::kLatestSchemaRevision = 1;
    DummyDatabase::instances             = 0;
    DummyDatabase::compacting_fails      = false;

    const bool retval = MkdirDeep(sandbox, 0700);
    ASSERT_TRUE(retval) << "failed to create sandbox";
  }

  virtual void TearDown() {
    const bool retval = RemoveTree(sandbox);
    ASSERT_TRUE(retval) << "failed to remove sandbox";
  }

  std::string GetDatabaseFilename() const {
    const std::string path = CreateTempPath(sandbox + "/catalog",
                                            0600);
    CheckEmpty(path);
    return path;
  }

 private:
  void CheckEmpty(const std::string &str) const {
    ASSERT_FALSE(str.empty());
  }
};

const std::string T_SQLite_Wrapper::sandbox = "/tmp/cvmfs_ut_sqlite_wrapper";


TEST_F(T_SQLite_Wrapper, Initialize) {}


TEST_F(T_SQLite_Wrapper, CreateEmptyDatabase) {
  DummyDatabase *db = DummyDatabase::Create(GetDatabaseFilename());
  ASSERT_NE(static_cast<DummyDatabase*>(NULL), db);
  EXPECT_EQ(1u, DummyDatabase::instances);
  EXPECT_EQ(1u, db->create_empty_db_calls);
  EXPECT_EQ(0u, db->check_compatibility_calls);
  EXPECT_EQ(0u, db->live_upgrade_calls);
  EXPECT_EQ(0u, db->compact_calls);
  EXPECT_EQ(DummyDatabase::kLatestSchemaRevision, db->schema_revision());
  EXPECT_TRUE(db->IsEqualSchema(db->schema_version(),
                                 DummyDatabase::kLatestSchema));
  EXPECT_FALSE(db->IsEqualSchema(db->schema_version(),
                                  DummyDatabase::kLatestSchema + 0.1));
  EXPECT_TRUE(db->read_write());
  delete db;
  EXPECT_EQ(0u, DummyDatabase::instances);
}


TEST_F(T_SQLite_Wrapper, CloseDatabase) {
  DummyDatabase *db = DummyDatabase::Create(GetDatabaseFilename());
  ASSERT_NE(static_cast<DummyDatabase*>(NULL), db);
  EXPECT_EQ(1u, DummyDatabase::instances);
  EXPECT_EQ(1u, db->create_empty_db_calls);
  EXPECT_EQ(0u, db->check_compatibility_calls);
  EXPECT_EQ(0u, db->live_upgrade_calls);
  EXPECT_EQ(0u, db->compact_calls);
  EXPECT_EQ(DummyDatabase::kLatestSchemaRevision, db->schema_revision());
  EXPECT_TRUE(db->IsEqualSchema(db->schema_version(),
                                 DummyDatabase::kLatestSchema));
  EXPECT_FALSE(db->IsEqualSchema(db->schema_version(),
                                  DummyDatabase::kLatestSchema + 0.1));
  EXPECT_TRUE(db->read_write());
  delete db;
  EXPECT_EQ(0u, DummyDatabase::instances);
}


TEST_F(T_SQLite_Wrapper, OpenDatabase) {
  const std::string dbp = GetDatabaseFilename();

  DummyDatabase *db1 = DummyDatabase::Create(dbp);
  ASSERT_NE(static_cast<DummyDatabase*>(NULL), db1);
  EXPECT_EQ(1u, DummyDatabase::instances);
  EXPECT_EQ(1u, db1->create_empty_db_calls);
  EXPECT_EQ(0u, db1->check_compatibility_calls);
  EXPECT_EQ(0u, db1->live_upgrade_calls);
  EXPECT_EQ(0u, db1->compact_calls);
  EXPECT_EQ(DummyDatabase::kLatestSchemaRevision, db1->schema_revision());
  EXPECT_EQ(dbp, db1->filename());
  EXPECT_TRUE(db1->IsEqualSchema(db1->schema_version(),
                                 DummyDatabase::kLatestSchema));
  EXPECT_FALSE(db1->IsEqualSchema(db1->schema_version(),
                                   DummyDatabase::kLatestSchema + 0.1));
  EXPECT_TRUE(db1->read_write());
  delete db1;
  EXPECT_EQ(0u, DummyDatabase::instances);

  DummyDatabase *db2 = DummyDatabase::Open(dbp, DummyDatabase::kOpenReadOnly);
  ASSERT_NE(static_cast<DummyDatabase*>(NULL), db2);
  EXPECT_EQ(1u, DummyDatabase::instances);
  EXPECT_EQ(0u, db2->create_empty_db_calls);
  EXPECT_EQ(1u, db2->check_compatibility_calls);
  EXPECT_EQ(0u, db2->live_upgrade_calls);
  EXPECT_EQ(0u, db2->compact_calls);
  EXPECT_EQ(DummyDatabase::kLatestSchemaRevision, db2->schema_revision());
  EXPECT_EQ(dbp, db2->filename());
  EXPECT_TRUE(db2->IsEqualSchema(db2->schema_version(),
                                 DummyDatabase::kLatestSchema));
  EXPECT_FALSE(db2->IsEqualSchema(db2->schema_version(),
                                   DummyDatabase::kLatestSchema + 0.1));
  EXPECT_FALSE(db2->read_write());

  delete db2;
  EXPECT_EQ(0u, DummyDatabase::instances);
}


TEST_F(T_SQLite_Wrapper, ReadWriteOpenDatabase) {
  const std::string dbp = GetDatabaseFilename();

  DummyDatabase *db1 = DummyDatabase::Create(dbp);
  ASSERT_NE(static_cast<DummyDatabase*>(NULL), db1);
  EXPECT_EQ(1u, DummyDatabase::instances);
  EXPECT_EQ(1u, db1->create_empty_db_calls);
  EXPECT_EQ(0u, db1->check_compatibility_calls);
  EXPECT_EQ(0u, db1->live_upgrade_calls);
  EXPECT_EQ(0u, db1->compact_calls);
  EXPECT_EQ(DummyDatabase::kLatestSchemaRevision, db1->schema_revision());
  EXPECT_EQ(dbp, db1->filename());
  EXPECT_TRUE(db1->IsEqualSchema(db1->schema_version(),
                                 DummyDatabase::kLatestSchema));
  EXPECT_FALSE(db1->IsEqualSchema(db1->schema_version(),
                                   DummyDatabase::kLatestSchema + 0.1));
  EXPECT_TRUE(db1->read_write());
  delete db1;
  EXPECT_EQ(0u, DummyDatabase::instances);

  DummyDatabase *db2 = DummyDatabase::Open(dbp, DummyDatabase::kOpenReadWrite);
  ASSERT_NE(static_cast<DummyDatabase*>(NULL), db2);
  EXPECT_EQ(1u, DummyDatabase::instances);
  EXPECT_EQ(0u, db2->create_empty_db_calls);
  EXPECT_EQ(1u, db2->check_compatibility_calls);
  EXPECT_EQ(1u, db2->live_upgrade_calls);
  EXPECT_EQ(0u, db2->compact_calls);
  EXPECT_EQ(DummyDatabase::kLatestSchemaRevision, db2->schema_revision());
  EXPECT_EQ(dbp, db2->filename());
  EXPECT_TRUE(db2->IsEqualSchema(db2->schema_version(),
                                  DummyDatabase::kLatestSchema));
  EXPECT_FALSE(db2->IsEqualSchema(db2->schema_version(),
                                   DummyDatabase::kLatestSchema + 0.1));
  EXPECT_TRUE(db2->read_write());
  delete db2;
  EXPECT_EQ(0u, DummyDatabase::instances);
}


TEST_F(T_SQLite_Wrapper, CompactDatabase) {
  const std::string dbp = GetDatabaseFilename();

  DummyDatabase *db1 = DummyDatabase::Create(dbp);
  ASSERT_NE(static_cast<DummyDatabase*>(NULL), db1);
  EXPECT_EQ(1u, DummyDatabase::instances);
  EXPECT_EQ(1u, db1->create_empty_db_calls);
  EXPECT_EQ(0u, db1->check_compatibility_calls);
  EXPECT_EQ(0u, db1->live_upgrade_calls);
  EXPECT_EQ(0u, db1->compact_calls);
  EXPECT_EQ(DummyDatabase::kLatestSchemaRevision, db1->schema_revision());
  EXPECT_EQ(dbp, db1->filename());
  EXPECT_TRUE(db1->IsEqualSchema(db1->schema_version(),
                                 DummyDatabase::kLatestSchema));
  EXPECT_FALSE(db1->IsEqualSchema(db1->schema_version(),
                                   DummyDatabase::kLatestSchema + 0.1));
  EXPECT_TRUE(db1->read_write());
  delete db1;
  EXPECT_EQ(0u, DummyDatabase::instances);

  DummyDatabase *db2 = DummyDatabase::Open(dbp, DummyDatabase::kOpenReadWrite);
  ASSERT_NE(static_cast<DummyDatabase*>(NULL), db2);
  EXPECT_EQ(1u, DummyDatabase::instances);
  EXPECT_EQ(0u, db2->create_empty_db_calls);
  EXPECT_EQ(1u, db2->check_compatibility_calls);
  EXPECT_EQ(1u, db2->live_upgrade_calls);
  EXPECT_EQ(0u, db2->compact_calls);
  EXPECT_EQ(DummyDatabase::kLatestSchemaRevision, db2->schema_revision());
  EXPECT_EQ(dbp, db2->filename());
  EXPECT_TRUE(db2->IsEqualSchema(db2->schema_version(),
                                  DummyDatabase::kLatestSchema));
  EXPECT_FALSE(db2->IsEqualSchema(db2->schema_version(),
                                   DummyDatabase::kLatestSchema + 0.1));

  db2->Vacuum();
  EXPECT_EQ(1u, DummyDatabase::instances);
  EXPECT_EQ(0u, db2->create_empty_db_calls);
  EXPECT_EQ(1u, db2->check_compatibility_calls);
  EXPECT_EQ(1u, db2->live_upgrade_calls);
  EXPECT_EQ(1u, db2->compact_calls);
  EXPECT_EQ(DummyDatabase::kLatestSchemaRevision, db2->schema_revision());
  EXPECT_EQ(dbp, db2->filename());
  EXPECT_TRUE(db2->IsEqualSchema(db2->schema_version(),
                                 DummyDatabase::kLatestSchema));
  EXPECT_FALSE(db2->IsEqualSchema(db2->schema_version(),
                                   DummyDatabase::kLatestSchema + 0.1));
  EXPECT_TRUE(db2->read_write());

  delete db2;
  EXPECT_EQ(0u, DummyDatabase::instances);
}


TEST_F(T_SQLite_Wrapper, PropertyWithoutReopen) {
  const std::string dbp = GetDatabaseFilename();

  DummyDatabase *db1 = DummyDatabase::Create(dbp);
  ASSERT_NE(static_cast<DummyDatabase*>(NULL), db1);

  EXPECT_TRUE(db1->SetProperty("foo",   "bar"));
  EXPECT_TRUE(db1->SetProperty("file",  dbp));
  EXPECT_TRUE(db1->SetProperty("int",   1337));
  EXPECT_TRUE(db1->SetProperty("float", 13.37));

  EXPECT_TRUE(db1->HasProperty("foo"));
  EXPECT_TRUE(db1->HasProperty("file"));
  EXPECT_TRUE(db1->HasProperty("int"));
  EXPECT_TRUE(db1->HasProperty("float"));

  EXPECT_FALSE(db1->HasProperty("mooh"));
  EXPECT_FALSE(db1->HasProperty("test"));
  EXPECT_FALSE(db1->HasProperty("integer"));
  EXPECT_FALSE(db1->HasProperty("double"));

  EXPECT_EQ("bar",   db1->GetProperty<std::string>("foo"));
  EXPECT_EQ(dbp,     db1->GetProperty<std::string>("file"));
  EXPECT_EQ(1337,    db1->GetProperty<int>("int"));
  EXPECT_LT(13.36,   db1->GetProperty<float>("float"));
  EXPECT_GT(13.38,   db1->GetProperty<double>("float"));

  delete db1;
  EXPECT_EQ(0u, DummyDatabase::instances);
}


TEST_F(T_SQLite_Wrapper, GetDefaultPropertyWithoutReopen) {
  const std::string dbp = GetDatabaseFilename();

  DummyDatabase *db1 = DummyDatabase::Create(dbp);
  ASSERT_NE(static_cast<DummyDatabase*>(NULL), db1);

  EXPECT_TRUE(db1->SetProperty("foo",   "bar"));
  EXPECT_TRUE(db1->SetProperty("file",  dbp));
  EXPECT_TRUE(db1->SetProperty("int",   1337));
  EXPECT_TRUE(db1->SetProperty("float", 13.37));

  EXPECT_EQ("bar", db1->GetPropertyDefault<std::string>("foo",  "0"));
  EXPECT_EQ(dbp,   db1->GetPropertyDefault<std::string>("file", "1"));
  EXPECT_EQ(1337,  db1->GetPropertyDefault<int>("int",           2));
  EXPECT_LT(13.36, db1->GetPropertyDefault<float>("float",       3));
  EXPECT_GT(13.38, db1->GetPropertyDefault<double>("float",      4));
  EXPECT_NE(42,    db1->GetPropertyDefault<int>("int",          42));

  EXPECT_FALSE(db1->HasProperty("moep"));
  EXPECT_FALSE(db1->HasProperty("baz"));

  EXPECT_EQ("tok!", db1->GetPropertyDefault<std::string>("moep", "tok!"));
  EXPECT_EQ(1337,   db1->GetPropertyDefault<int>("moep",         1337));
  EXPECT_LT(13.36,  db1->GetPropertyDefault<double>("moep",      13.37));
  EXPECT_GT(13.38f, db1->GetPropertyDefault<float>("moep",       13.37f));
  EXPECT_EQ(0,      db1->GetPropertyDefault<double>("baz",       0));

  delete db1;
  EXPECT_EQ(0u, DummyDatabase::instances);
}


TEST_F(T_SQLite_Wrapper, PropertyWithReadOnlyReopen) {
  const std::string dbp = GetDatabaseFilename();

  DummyDatabase *db1 = DummyDatabase::Create(dbp);
  ASSERT_NE(static_cast<DummyDatabase*>(NULL), db1);

  EXPECT_TRUE(db1->SetProperty("foo",   "bar"));
  EXPECT_TRUE(db1->SetProperty("file",  dbp));
  EXPECT_TRUE(db1->SetProperty("int",   1337));
  EXPECT_TRUE(db1->SetProperty("float", 13.37));

  delete db1;
  EXPECT_EQ(0u, DummyDatabase::instances);

  DummyDatabase *db2 = DummyDatabase::Open(dbp, DummyDatabase::kOpenReadOnly);
  ASSERT_NE(static_cast<DummyDatabase*>(NULL), db2);

  EXPECT_TRUE(db2->HasProperty("foo"));
  EXPECT_TRUE(db2->HasProperty("file"));
  EXPECT_TRUE(db2->HasProperty("int"));
  EXPECT_TRUE(db2->HasProperty("float"));

  EXPECT_FALSE(db2->HasProperty("mooh"));
  EXPECT_FALSE(db2->HasProperty("test"));
  EXPECT_FALSE(db2->HasProperty("integer"));
  EXPECT_FALSE(db2->HasProperty("double"));

  EXPECT_EQ("bar",   db2->GetProperty<std::string>("foo"));
  EXPECT_EQ(dbp,     db2->GetProperty<std::string>("file"));
  EXPECT_EQ(1337,    db2->GetProperty<int>("int"));
  EXPECT_LT(13.36,   db2->GetProperty<float>("float"));
  EXPECT_GT(13.38,   db2->GetProperty<double>("float"));

  delete db2;
  EXPECT_EQ(0u, DummyDatabase::instances);
}


TEST_F(T_SQLite_Wrapper, PropertyWithReadWriteReopen) {
  const std::string dbp = GetDatabaseFilename();

  DummyDatabase *db1 = DummyDatabase::Create(dbp);
  ASSERT_NE(static_cast<DummyDatabase*>(NULL), db1);

  EXPECT_TRUE(db1->SetProperty("foo",   "bar"));
  EXPECT_TRUE(db1->SetProperty("file",  dbp));
  EXPECT_TRUE(db1->SetProperty("int",   1337));
  EXPECT_TRUE(db1->SetProperty("float", 13.37));

  delete db1;
  EXPECT_EQ(0u, DummyDatabase::instances);

  DummyDatabase *db2 = DummyDatabase::Open(dbp, DummyDatabase::kOpenReadWrite);
  ASSERT_NE(static_cast<DummyDatabase*>(NULL), db2);

  EXPECT_TRUE(db2->HasProperty("foo"));
  EXPECT_TRUE(db2->HasProperty("file"));
  EXPECT_TRUE(db2->HasProperty("int"));
  EXPECT_TRUE(db2->HasProperty("float"));

  EXPECT_FALSE(db2->HasProperty("mooh"));
  EXPECT_FALSE(db2->HasProperty("test"));
  EXPECT_FALSE(db2->HasProperty("integer"));
  EXPECT_FALSE(db2->HasProperty("double"));

  EXPECT_EQ("bar",   db2->GetProperty<std::string>("foo"));
  EXPECT_EQ(dbp,     db2->GetProperty<std::string>("file"));
  EXPECT_EQ(1337,    db2->GetProperty<int>("int"));
  EXPECT_LT(13.36,   db2->GetProperty<float>("float"));
  EXPECT_GT(13.38,   db2->GetProperty<double>("float"));

  delete db2;
  EXPECT_EQ(0u, DummyDatabase::instances);
}


TEST_F(T_SQLite_Wrapper, IncompatibleSchema) {
  const std::string dbp = GetDatabaseFilename();

  DummyDatabase::kLatestSchema = 2.0;
  DummyDatabase *db1 = DummyDatabase::Create(dbp);
  ASSERT_NE(static_cast<DummyDatabase*>(NULL), db1);
  EXPECT_LT(1.9, db1->schema_version());
  EXPECT_GT(2.1, db1->schema_version());
  EXPECT_EQ(0u, db1->check_compatibility_calls);
  EXPECT_EQ(0u, db1->live_upgrade_calls);
  DummyDatabase::kLatestSchema = 1.0;

  delete db1;
  EXPECT_EQ(0u, DummyDatabase::instances);

  DummyDatabase *db2 = DummyDatabase::Open(dbp, DummyDatabase::kOpenReadWrite);
  ASSERT_EQ(static_cast<DummyDatabase*>(NULL), db2);
  EXPECT_EQ(0u, DummyDatabase::instances);

  delete db2;
  EXPECT_EQ(0u, DummyDatabase::instances);
}


TEST_F(T_SQLite_Wrapper, SuccessfulSchemaUpdate) {
  const std::string dbp = GetDatabaseFilename();

  DummyDatabase::kLatestSchemaRevision = RevisionFlags::kUpdatableRevision;
  DummyDatabase *db1 = DummyDatabase::Create(dbp);
  ASSERT_NE(static_cast<DummyDatabase*>(NULL), db1);
  EXPECT_EQ(unsigned(RevisionFlags::kUpdatableRevision),
             db1->schema_revision());
  EXPECT_EQ(0u, db1->check_compatibility_calls);
  EXPECT_EQ(0u, db1->live_upgrade_calls);
  DummyDatabase::kLatestSchemaRevision = RevisionFlags::kInitialRevision;

  delete db1;
  EXPECT_EQ(0u, DummyDatabase::instances);

  DummyDatabase *db2 = DummyDatabase::Open(dbp, DummyDatabase::kOpenReadWrite);
  ASSERT_NE(static_cast<DummyDatabase*>(NULL), db2);
  EXPECT_EQ(1u, db2->check_compatibility_calls);
  EXPECT_EQ(1u, db2->live_upgrade_calls);
  EXPECT_EQ(unsigned(RevisionFlags::kUpdatedRevision), db2->schema_revision());
  EXPECT_EQ(1u, DummyDatabase::instances);

  delete db2;
  EXPECT_EQ(0u, DummyDatabase::instances);

  DummyDatabase *db3 = DummyDatabase::Open(dbp, DummyDatabase::kOpenReadOnly);
  ASSERT_NE(static_cast<DummyDatabase*>(NULL), db3);
  EXPECT_EQ(1u, db3->check_compatibility_calls);
  EXPECT_EQ(0u, db3->live_upgrade_calls);
  EXPECT_EQ(unsigned(RevisionFlags::kUpdatedRevision), db3->schema_revision());
  EXPECT_EQ(1u, DummyDatabase::instances);

  delete db3;
  EXPECT_EQ(0u, DummyDatabase::instances);
}


TEST_F(T_SQLite_Wrapper, FailingSchemaUpdate) {
  const std::string dbp = GetDatabaseFilename();

  DummyDatabase::kLatestSchemaRevision = RevisionFlags::kFailingRevision;
  DummyDatabase *db1 = DummyDatabase::Create(dbp);
  ASSERT_NE(static_cast<DummyDatabase*>(NULL), db1);
  EXPECT_EQ(unsigned(RevisionFlags::kFailingRevision), db1->schema_revision());
  EXPECT_EQ(0u, db1->check_compatibility_calls);
  EXPECT_EQ(0u, db1->live_upgrade_calls);
  DummyDatabase::kLatestSchemaRevision = RevisionFlags::kInitialRevision;

  delete db1;
  EXPECT_EQ(0u, DummyDatabase::instances);

  DummyDatabase *db2 = DummyDatabase::Open(dbp, DummyDatabase::kOpenReadOnly);
  ASSERT_NE(static_cast<DummyDatabase*>(NULL), db2);
  EXPECT_EQ(1u, db2->check_compatibility_calls);
  EXPECT_EQ(0u, db2->live_upgrade_calls);
  EXPECT_EQ(unsigned(RevisionFlags::kFailingRevision), db2->schema_revision());
  EXPECT_EQ(1u, DummyDatabase::instances);

  delete db2;
  EXPECT_EQ(0u, DummyDatabase::instances);

  DummyDatabase *db3 = DummyDatabase::Open(dbp, DummyDatabase::kOpenReadWrite);
  ASSERT_EQ(static_cast<DummyDatabase*>(NULL), db3);
  EXPECT_EQ(0u, DummyDatabase::instances);
}


TEST_F(T_SQLite_Wrapper, DataAccessSlow) {
  // This test case might be dependent on SQLite updates
  // if it fails, check if the data compression or memory page behaviour of
  // SQLite changed...
  const std::string dbp = GetDatabaseFilename();
  const int entry_count = 50000;

  DummyDatabase *db1 = DummyDatabase::Create(dbp);
  ASSERT_NE(static_cast<DummyDatabase*>(NULL), db1);
  {
    sqlite::Sql insert(db1->sqlite_db(), "INSERT INTO foobar (foo, bar) "
                                         "VALUES (:f, :b);");

    EXPECT_TRUE(db1->BeginTransaction());
    for (int i = 0; i < entry_count; ++i) {
      EXPECT_TRUE(insert.BindTextTransient(1, "foobar!" + StringifyInt(i)));
      EXPECT_TRUE(insert.BindText(2, "this is a very useless text!!"));
      EXPECT_TRUE(insert.Execute());
      EXPECT_TRUE(insert.Reset());
    }
    EXPECT_TRUE(db1->CommitTransaction());
  }

  EXPECT_GT(0.1, db1->GetFreePageRatio());
  EXPECT_EQ(0u,   db1->compact_calls);

  delete db1;
  EXPECT_EQ(0u, DummyDatabase::instances);

  EXPECT_LT(2000000, GetFileSize(dbp));
  DummyDatabase *db2 = DummyDatabase::Open(dbp, DummyDatabase::kOpenReadOnly);
  ASSERT_NE(static_cast<DummyDatabase*>(NULL), db2);
  {
    sqlite::Sql count(db2->sqlite_db(), "SELECT count(*) FROM foobar;");

    EXPECT_TRUE(count.FetchRow());
    EXPECT_EQ(entry_count, count.Retrieve<int>(0));
  }

  delete db2;
  EXPECT_EQ(0u, DummyDatabase::instances);
}


TEST_F(T_SQLite_Wrapper, VacuumDatabaseSlow) {
  // This test case might be dependent on SQLite updates
  // if it fails, check if the VACUUM behaviour of SQLite changed...
  const std::string dbp = GetDatabaseFilename();
  const int entry_count = 50000;

  DummyDatabase *db1 = DummyDatabase::Create(dbp);
  ASSERT_NE(static_cast<DummyDatabase*>(NULL), db1);
  {
    sqlite::Sql insert(db1->sqlite_db(), "INSERT INTO foobar (foo, bar) "
                                         "VALUES (:f, :b);");

    EXPECT_TRUE(db1->BeginTransaction());
    for (int i = 0; i < entry_count; ++i) {
      EXPECT_TRUE(insert.BindTextTransient(1, "foobar!" + StringifyInt(i)));
      EXPECT_TRUE(insert.BindText(2, "this is a very useless text!!"));
      EXPECT_TRUE(insert.Execute());
      EXPECT_TRUE(insert.Reset());
    }
    EXPECT_TRUE(db1->CommitTransaction());
  }

  EXPECT_GT(0.1, db1->GetFreePageRatio());
  EXPECT_EQ(0u, db1->compact_calls);

  delete db1;
  EXPECT_EQ(0u, DummyDatabase::instances);

  EXPECT_LT(2000000, GetFileSize(dbp));
  DummyDatabase *db2 = DummyDatabase::Open(dbp, DummyDatabase::kOpenReadWrite);
  ASSERT_NE(static_cast<DummyDatabase*>(NULL), db2);
  {
    sqlite::Sql wipe(db2->sqlite_db(), "DELETE FROM foobar;");

    EXPECT_TRUE(wipe.Execute());
  }

  EXPECT_LT(0.9, db2->GetFreePageRatio());
  EXPECT_EQ(0u, db2->compact_calls);

  delete db2;
  EXPECT_EQ(0u, DummyDatabase::instances);

  EXPECT_LT(2000000, GetFileSize(dbp));
  DummyDatabase *db3 = DummyDatabase::Open(dbp, DummyDatabase::kOpenReadWrite);
  ASSERT_NE(static_cast<DummyDatabase*>(NULL), db3);
  EXPECT_LT(0.9, db3->GetFreePageRatio());
  EXPECT_EQ(0u, db3->compact_calls);

  EXPECT_TRUE(db3->Vacuum());

  EXPECT_GT(0.1, db3->GetFreePageRatio());
  EXPECT_EQ(1u, db3->compact_calls);

  delete db3;
  EXPECT_EQ(0u, DummyDatabase::instances);
  EXPECT_GT(150000, GetFileSize(dbp));

  DummyDatabase *db4 = DummyDatabase::Open(dbp, DummyDatabase::kOpenReadWrite);
  ASSERT_NE(static_cast<DummyDatabase*>(NULL), db4);
  EXPECT_GT(0.1, db4->GetFreePageRatio());
  EXPECT_EQ(0u, db4->compact_calls);

  EXPECT_TRUE(db4->Vacuum());
  EXPECT_GT(0.1, db4->GetFreePageRatio());
  EXPECT_EQ(1u, db4->compact_calls);

  EXPECT_TRUE(db4->Vacuum());
  EXPECT_GT(0.1, db4->GetFreePageRatio());
  EXPECT_EQ(2u, db4->compact_calls);

  delete db4;
  EXPECT_EQ(0u, DummyDatabase::instances);
  EXPECT_GT(150000, GetFileSize(dbp));
}


TEST_F(T_SQLite_Wrapper, FailingCompaction) {
  // This test case might be dependent on SQLite updates
  // if it fails, check if the VACUUM behaviour of SQLite changed...
  const std::string dbp = GetDatabaseFilename();
  const int entry_count = 50000;

  DummyDatabase *db1 = DummyDatabase::Create(dbp);
  ASSERT_NE(static_cast<DummyDatabase*>(NULL), db1);
  {
    sqlite::Sql insert(db1->sqlite_db(), "INSERT INTO foobar (foo, bar) "
                                         "VALUES (:f, :b);");

    EXPECT_TRUE(db1->BeginTransaction());
    for (int i = 1; i < entry_count; ++i) {
      EXPECT_TRUE(insert.BindTextTransient(1, "foobar!" + StringifyInt(i)));
      EXPECT_TRUE(insert.BindText(2, "this is a very useless text!!"));
      EXPECT_TRUE(insert.Execute());

      EXPECT_TRUE(insert.Reset());
    }
    EXPECT_TRUE(db1->CommitTransaction());
  }

  EXPECT_GT(0.1, db1->GetFreePageRatio());
  EXPECT_EQ(0u, db1->compact_calls);

  delete db1;
  EXPECT_EQ(0u, DummyDatabase::instances);

  EXPECT_LT(2000000, GetFileSize(dbp));
  DummyDatabase *db2 = DummyDatabase::Open(dbp, DummyDatabase::kOpenReadWrite);
  ASSERT_NE(static_cast<DummyDatabase*>(NULL), db2);
  {
    sqlite::Sql wipe(db2->sqlite_db(), "DELETE FROM foobar;");

    EXPECT_TRUE(wipe.Execute());
  }

  EXPECT_LT(0.9, db2->GetFreePageRatio());
  EXPECT_EQ(0u,   db2->compact_calls);

  DummyDatabase::compacting_fails = true;
  EXPECT_FALSE(db2->Vacuum());
  EXPECT_LT(0.9, db2->GetFreePageRatio());
  EXPECT_EQ(1u,   db2->compact_calls);

  DummyDatabase::compacting_fails = false;
  EXPECT_TRUE(db2->Vacuum());
  EXPECT_GT(0.1, db2->GetFreePageRatio());
  EXPECT_EQ(2u,   db2->compact_calls);

  delete db2;
  EXPECT_EQ(0u, DummyDatabase::instances);

  EXPECT_GT(150000, GetFileSize(dbp));
}


TEST_F(T_SQLite_Wrapper, TakeFileOwnership) {
  const std::string file_name1 = GetDatabaseFilename();
  ASSERT_TRUE(FileExists(file_name1));

  DummyDatabase *db1 = DummyDatabase::Create(file_name1);
  ASSERT_NE(static_cast<DummyDatabase*>(NULL), db1);
  EXPECT_FALSE(db1->OwnsFile());
  EXPECT_TRUE(FileExists(file_name1));
  delete db1;

  EXPECT_EQ(0u, DummyDatabase::instances);
  EXPECT_TRUE(FileExists(file_name1));

  DummyDatabase *db2 =
    DummyDatabase::Open(file_name1, DummyDatabase::kOpenReadOnly);
  ASSERT_NE(static_cast<DummyDatabase*>(NULL), db2);
  EXPECT_FALSE(db2->OwnsFile());
  EXPECT_TRUE(FileExists(file_name1));
  delete db2;

  EXPECT_EQ(0u, DummyDatabase::instances);
  EXPECT_TRUE(FileExists(file_name1));

  DummyDatabase *db3 =
    DummyDatabase::Open(file_name1, DummyDatabase::kOpenReadWrite);
  ASSERT_NE(static_cast<DummyDatabase*>(NULL), db3);
  EXPECT_FALSE(db3->OwnsFile());
  EXPECT_TRUE(FileExists(file_name1));
  delete db3;

  EXPECT_EQ(0u, DummyDatabase::instances);
  EXPECT_TRUE(FileExists(file_name1));

  DummyDatabase *db4 =
    DummyDatabase::Open(file_name1, DummyDatabase::kOpenReadOnly);
  ASSERT_NE(static_cast<DummyDatabase*>(NULL), db4);
  EXPECT_TRUE(FileExists(file_name1));
  EXPECT_FALSE(db4->OwnsFile());
  db4->TakeFileOwnership();
  EXPECT_TRUE(db4->OwnsFile());
  EXPECT_TRUE(FileExists(file_name1));
  delete db4;

  EXPECT_EQ(0u, DummyDatabase::instances);
  EXPECT_FALSE(FileExists(file_name1));

  const std::string file_name2 = GetDatabaseFilename();
  ASSERT_TRUE(FileExists(file_name2));
  DummyDatabase *db5 = DummyDatabase::Create(file_name2);
  ASSERT_NE(static_cast<DummyDatabase*>(NULL), db5);
  EXPECT_FALSE(db5->OwnsFile());
  EXPECT_TRUE(FileExists(file_name2));
  db5->TakeFileOwnership();
  EXPECT_TRUE(db5->OwnsFile());
  delete db5;

  EXPECT_EQ(0u, DummyDatabase::instances);
  EXPECT_FALSE(FileExists(file_name2));

  const std::string file_name3 = GetDatabaseFilename();
  ASSERT_TRUE(FileExists(file_name3));
  DummyDatabase *db6 = DummyDatabase::Create(file_name3);
  ASSERT_NE(static_cast<DummyDatabase*>(NULL), db6);
  EXPECT_FALSE(db6->OwnsFile());
  EXPECT_TRUE(FileExists(file_name3));
  delete db6;

  EXPECT_EQ(0u, DummyDatabase::instances);
  EXPECT_TRUE(FileExists(file_name3));

  DummyDatabase *db7 =
    DummyDatabase::Open(file_name3, DummyDatabase::kOpenReadWrite);
  ASSERT_NE(static_cast<DummyDatabase*>(NULL), db7);
  EXPECT_FALSE(db7->OwnsFile());
  EXPECT_TRUE(FileExists(file_name3));
  db7->TakeFileOwnership();
  EXPECT_TRUE(db7->OwnsFile());
  EXPECT_TRUE(FileExists(file_name3));
  delete db7;

  EXPECT_EQ(0u, DummyDatabase::instances);
  EXPECT_FALSE(FileExists(file_name3));
}


TEST_F(T_SQLite_Wrapper, DropFileOwnership) {
  const std::string file_name1 = GetDatabaseFilename();
  ASSERT_TRUE(FileExists(file_name1));

  DummyDatabase *db1 = DummyDatabase::Create(file_name1);
  ASSERT_NE(static_cast<DummyDatabase*>(NULL), db1);
  EXPECT_FALSE(db1->OwnsFile());
  EXPECT_TRUE(FileExists(file_name1));
  delete db1;

  EXPECT_EQ(0u, DummyDatabase::instances);
  EXPECT_TRUE(FileExists(file_name1));

  DummyDatabase *db2 =
    DummyDatabase::Open(file_name1, DummyDatabase::kOpenReadOnly);
  ASSERT_NE(static_cast<DummyDatabase*>(NULL), db2);
  EXPECT_FALSE(db2->OwnsFile());
  EXPECT_TRUE(FileExists(file_name1));
  delete db2;

  EXPECT_EQ(0u, DummyDatabase::instances);
  EXPECT_TRUE(FileExists(file_name1));

  DummyDatabase *db3 =
    DummyDatabase::Open(file_name1, DummyDatabase::kOpenReadWrite);
  ASSERT_NE(static_cast<DummyDatabase*>(NULL), db3);
  EXPECT_FALSE(db3->OwnsFile());
  EXPECT_TRUE(FileExists(file_name1));
  delete db3;

  EXPECT_EQ(0u, DummyDatabase::instances);
  EXPECT_TRUE(FileExists(file_name1));

  DummyDatabase *db4 =
    DummyDatabase::Open(file_name1, DummyDatabase::kOpenReadOnly);
  ASSERT_NE(static_cast<DummyDatabase*>(NULL), db4);
  EXPECT_TRUE(FileExists(file_name1));
  EXPECT_FALSE(db4->OwnsFile());
  db4->TakeFileOwnership();
  EXPECT_TRUE(db4->OwnsFile());
  db4->DropFileOwnership();
  EXPECT_FALSE(db4->OwnsFile());
  EXPECT_TRUE(FileExists(file_name1));
  delete db4;

  EXPECT_EQ(0u, DummyDatabase::instances);
  EXPECT_TRUE(FileExists(file_name1));

  const std::string file_name2 = GetDatabaseFilename();
  ASSERT_TRUE(FileExists(file_name2));
  DummyDatabase *db5 = DummyDatabase::Create(file_name2);
  ASSERT_NE(static_cast<DummyDatabase*>(NULL), db5);
  EXPECT_FALSE(db5->OwnsFile());
  EXPECT_TRUE(FileExists(file_name2));
  db5->TakeFileOwnership();
  EXPECT_TRUE(db5->OwnsFile());
  db5->DropFileOwnership();
  EXPECT_FALSE(db5->OwnsFile());
  delete db5;

  EXPECT_EQ(0u, DummyDatabase::instances);
  EXPECT_TRUE(FileExists(file_name2));

  const std::string file_name3 = GetDatabaseFilename();
  ASSERT_TRUE(FileExists(file_name3));
  DummyDatabase *db6 = DummyDatabase::Create(file_name3);
  ASSERT_NE(static_cast<DummyDatabase*>(NULL), db6);
  EXPECT_FALSE(db6->OwnsFile());
  EXPECT_TRUE(FileExists(file_name3));
  delete db6;

  EXPECT_EQ(0u, DummyDatabase::instances);
  EXPECT_TRUE(FileExists(file_name3));

  DummyDatabase *db7 =
    DummyDatabase::Open(file_name3, DummyDatabase::kOpenReadWrite);
  ASSERT_NE(static_cast<DummyDatabase*>(NULL), db7);
  EXPECT_FALSE(db7->OwnsFile());
  EXPECT_TRUE(FileExists(file_name3));
  db7->TakeFileOwnership();
  EXPECT_TRUE(db7->OwnsFile());
  db7->DropFileOwnership();
  EXPECT_FALSE(db7->OwnsFile());
  EXPECT_TRUE(FileExists(file_name3));
  delete db7;

  EXPECT_EQ(0u, DummyDatabase::instances);
  EXPECT_TRUE(FileExists(file_name3));
}
