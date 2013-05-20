/**
 * This file is part of the CernVM File System.
 */

#include "history.h"

#include <cstdlib>
#include <cassert>

#include "logging.h"

using namespace std;  // NOLINT

namespace history {

const float Database::kLatestSchema = 1.0;
const float Database::kLatestSupportedSchema = 1.0;
const float Database::kSchemaEpsilon = 0.0005;

Database::Database(const std::string filename,
                   const sqlite::DbOpenMode open_mode)
{
  filename_ = filename;
  ready_ = false;
  schema_version_ = 0.0;
  sqlite_db_ = NULL;

  int flags = SQLITE_OPEN_NOMUTEX;
  switch (open_mode) {
    case sqlite::kDbOpenReadOnly:
      flags |= SQLITE_OPEN_READONLY;
      read_write_ = false;
      break;
    case sqlite::kDbOpenReadWrite:
      flags |= SQLITE_OPEN_READWRITE;
      read_write_ = true;
      break;
    default:
      abort();
  }

  // Open database file (depending on the flags read-only or read-write)
  LogCvmfs(kLogHistory, kLogDebug, "opening database file %s",
           filename_.c_str());
  if (SQLITE_OK != sqlite3_open_v2(filename_.c_str(), &sqlite_db_, flags, NULL))
  {
    LogCvmfs(kLogHistory, kLogDebug, "cannot open catalog database file %s",
             filename_.c_str());
    return;
  }
  sqlite3_extended_result_codes(sqlite_db_, 1);

  {  // Get schema version
    sqlite::Sql sql_schema(sqlite_db_,
                           "SELECT value FROM properties WHERE key='schema';");
    if (sql_schema.FetchRow()) {
      schema_version_ = sql_schema.RetrieveDouble(0);
    } else {
      LogCvmfs(kLogHistory, kLogDebug, "failed to retrieve schema in %s",
               filename_.c_str());
      goto database_failure;
    }
  }
  LogCvmfs(kLogCatalog, kLogDebug, "open db with schema version %f",
           schema_version_);
  if ((schema_version_ < kLatestSupportedSchema-kSchemaEpsilon) ||
      (schema_version_ > kLatestSchema+kSchemaEpsilon))
  {
    LogCvmfs(kLogCatalog, kLogDebug, "schema version %f not supported (%s)",
             schema_version_, filename.c_str());
    goto database_failure;
  }

  ready_ = true;
  return;

 database_failure:
  sqlite3_close(sqlite_db_);
  sqlite_db_ = NULL;
}


/**
 * Private constructor.  Used to create a new sqlite database.
 */
Database::Database(sqlite3 *sqlite_db, const float schema, const bool rw) {
  sqlite_db_ = sqlite_db;
  filename_ = "TMP";
  schema_version_ = schema;
  read_write_ = rw;
  ready_ = false;  // Don't close on delete
}


Database::~Database() {
  if (ready_)
    sqlite3_close(sqlite_db_);
}


/**
 * This method creates a new database file and initializes the database schema.
 */
bool Database::Create(const string &filename)
{
  sqlite3 *sqlite_db;
  sqlite::Sql *sql_schema;
  int open_flags = SQLITE_OPEN_NOMUTEX | SQLITE_OPEN_READWRITE |
                   SQLITE_OPEN_CREATE;

  // Create the new catalog file and open it
  LogCvmfs(kLogCatalog, kLogVerboseMsg, "creating new history db at '%s'",
           filename.c_str());
  if (sqlite3_open_v2(filename.c_str(), &sqlite_db, open_flags, NULL) !=
      SQLITE_OK)
  {
    LogCvmfs(kLogCatalog, kLogStderr,
             "Cannot create and open history database file '%s'",
             filename.c_str());
    return false;
  }
  sqlite3_extended_result_codes(sqlite_db, 1);
  Database database(sqlite_db, kLatestSchema, true);

  bool retval;
  string sql;
  retval =
    sqlite::Sql(sqlite_db,
                "CREATE TABLE properties (key TEXT, value TEXT, "
                "CONSTRAINT pk_properties PRIMARY KEY (key));").Execute();
  if (!retval)
    goto create_schema_fail;

  sql_schema = new sqlite::Sql(sqlite_db, "INSERT INTO properties "
                               "(key, value) VALUES ('schema', :schema);");
  retval = sql_schema->BindDouble(1, kLatestSchema) && sql_schema->Execute();
  delete sql_schema;
  if (!retval)
    goto create_schema_fail;

  sqlite3_close(sqlite_db);
  return true;

 create_schema_fail:
  LogCvmfs(kLogSql, kLogVerboseMsg, "sql failure %s",
           sqlite3_errmsg(sqlite_db));
  sqlite3_close(sqlite_db);
  return false;
}


std::string Database::GetLastErrorMsg() const {
  std::string msg = sqlite3_errmsg(sqlite_db_);
  return msg;
}

}  // namespace history
