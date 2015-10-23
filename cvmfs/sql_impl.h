/**
 * This file is part of the CernVM file system.
 */

#ifndef CVMFS_SQL_IMPL_H_
#define CVMFS_SQL_IMPL_H_

#include <fcntl.h>

#include <cassert>
#include <cerrno>
#include <string>

#include "logging.h"
#include "platform.h"

namespace sqlite {

template <class DerivedT>
Database<DerivedT>::Database(const std::string  &filename,
                             const OpenMode      open_mode)
  : database_(filename, this)
  , read_write_(kOpenReadWrite == open_mode)
  , schema_version_(0.0f)
  , schema_revision_(0) {}


template <class DerivedT>
DerivedT* Database<DerivedT>::Create(const std::string &filename) {
  UniquePtr<DerivedT> database(new DerivedT(filename, kOpenReadWrite));

  if (!database.IsValid()) {
    LogCvmfs(kLogSql, kLogDebug, "Failed to create new database object");
    return NULL;
  }

  database->set_schema_version(DerivedT::kLatestSchema);
  database->set_schema_revision(DerivedT::kLatestSchemaRevision);

  const int open_flags = SQLITE_OPEN_NOMUTEX | SQLITE_OPEN_READWRITE |
                         SQLITE_OPEN_CREATE;
  if (!database->OpenDatabase(open_flags)) {
    LogCvmfs(kLogSql, kLogDebug, "Failed to create new database file");
    return NULL;
  }

  if (!database->CreatePropertiesTable()) {
    database->PrintSqlError("Failed to create common properties table");
    return NULL;
  }

  if (!database->CreateEmptyDatabase()) {
    database->PrintSqlError("Failed to create empty database");
    return NULL;
  }

  if (!database->PrepareCommonQueries()) {
    database->PrintSqlError("Failed to initialize properties queries");
    return NULL;
  }

  if (!database->StoreSchemaRevision()) {
    database->PrintSqlError("Failed to store initial schema revision");
    return NULL;
  }

  return database.Release();
}


template <class DerivedT>
DerivedT* Database<DerivedT>::Open(const std::string  &filename,
                                   const OpenMode      open_mode) {
  UniquePtr<DerivedT> database(new DerivedT(filename, open_mode));

  if (!database.IsValid()) {
    LogCvmfs(kLogSql, kLogDebug,
             "Failed to open database file '%s' - errno: %d",
             filename.c_str(), errno);
    return NULL;
  }

  if (!database->Initialize()) {
    return NULL;
  }

  return database.Release();
}


template <class DerivedT>
bool Database<DerivedT>::Initialize() {
  const int flags = (read_write_) ? SQLITE_OPEN_NOMUTEX | SQLITE_OPEN_READWRITE
                                  : SQLITE_OPEN_NOMUTEX | SQLITE_OPEN_READONLY;

  const bool successful =
    OpenDatabase(flags) &&
    Configure()         &&
    FileReadAhead()     &&
    PrepareCommonQueries();
  if (!successful) {
    LogCvmfs(kLogSql, kLogDebug, "failed to open database file '%s'",
                                 filename().c_str());
    return false;
  }

  ReadSchemaRevision();
  LogCvmfs(kLogSql, kLogDebug, "opened database with schema version %f "
                               "and revision %u",
                               schema_version_, schema_revision_);

  if (!static_cast<DerivedT*>(this)->CheckSchemaCompatibility()) {
    LogCvmfs(kLogSql, kLogDebug, "schema version %f not supported (%s)",
             schema_version_, filename().c_str());
    return false;
  }

  if (read_write_ &&
      !static_cast<DerivedT*>(this)->LiveSchemaUpgradeIfNecessary()) {
    LogCvmfs(kLogSql, kLogDebug, "failed tp upgrade schema revision");
    return false;
  }

  return true;
}


template <class DerivedT>
bool Database<DerivedT>::OpenDatabase(const int flags) {
  // Open database file (depending on the flags read-only or read-write)
  LogCvmfs(kLogSql, kLogDebug, "opening database file %s",
           filename().c_str());
  if (SQLITE_OK != sqlite3_open_v2(filename().c_str(),
                                   &database_.sqlite_db, flags, NULL)) {
    LogCvmfs(kLogSql, kLogDebug, "cannot open database file %s",
             filename().c_str());
    return false;
  }

  const int retval = sqlite3_extended_result_codes(sqlite_db(), 1);
  assert(SQLITE_OK == retval);

  return true;
}


template <class DerivedT>
Database<DerivedT>::DatabaseRaiiWrapper::~DatabaseRaiiWrapper() {
  if (NULL != sqlite_db) {
    LogCvmfs(kLogSql, kLogDebug, "closing SQLite database '%s' (unlink: %s)",
             filename().c_str(),
             (db_file_guard.IsEnabled() ? "yes" : "no"));
    const int result = sqlite3_close(sqlite_db);
    if (result != SQLITE_OK) {
      LogCvmfs(kLogSql, kLogDebug,
               "failed to close SQLite database '%s' (%d - %s)",
               filename().c_str(), result,
               delegate_->GetLastErrorMsg().c_str());
    }
    sqlite_db = NULL;
  }
}


template <class DerivedT>
bool Database<DerivedT>::Configure() {
  // Read-only databases should store temporary files in memory.  This avoids
  // unexpected open read-write file descriptors in the cache directory like
  // etilqs_<number>.
  if (!read_write_) {
    return Sql(sqlite_db() , "PRAGMA temp_store=2;").Execute() &&
           Sql(sqlite_db() , "PRAGMA locking_mode=EXCLUSIVE;").Execute();
  }
  return true;
}


template <class DerivedT>
bool Database<DerivedT>::FileReadAhead() {
  // Read-ahead into file system buffers
  // TODO(jblomer): mmap, re-readahead
  assert(filename().length() > 1);
  int fd_readahead;
  if (filename()[0] != '@') {
    fd_readahead = open(filename().c_str(), O_RDONLY);
    if (fd_readahead < 0) {
      LogCvmfs(kLogSql, kLogDebug, "failed to open %s for read-ahead (%d)",
               filename().c_str(), errno);
      return false;
    }
    const int retval = platform_readahead(fd_readahead);
    close(fd_readahead);
    if (retval != 0) {
      LogCvmfs(kLogSql, kLogDebug | kLogSyslogWarn,
               "failed to read-ahead %s (%d)", filename().c_str(), errno);
      // Read-ahead is known to fail on tmpfs.  Don't consider it as a fatal
      // error.
      // return false;
    }
  }

  return true;
}


template <class DerivedT>
bool Database<DerivedT>::PrepareCommonQueries() {
  sqlite3 *db = sqlite_db();
  begin_transaction_  = new Sql(db, "BEGIN;");
  commit_transaction_ = new Sql(db, "COMMIT;");
  has_property_       = new Sql(db, "SELECT count(*) FROM properties "
                                    "WHERE key = :key;");
  get_property_       = new Sql(db, "SELECT value FROM properties "
                                    "WHERE key = :key;");
  set_property_       = new Sql(db, "INSERT OR REPLACE INTO properties "
                                    "(key, value) VALUES (:key, :value);");
  return (begin_transaction_ && commit_transaction_ &&
          has_property_ && get_property_ && set_property_);
}


template <class DerivedT>
void Database<DerivedT>::ReadSchemaRevision() {
  schema_version_  = (this->HasProperty(kSchemaVersionKey))
                        ? this->GetProperty<double>(kSchemaVersionKey)
                        : 1.0;
  schema_revision_ = (this->HasProperty(kSchemaRevisionKey))
                        ? this->GetProperty<int>(kSchemaRevisionKey)
                        : 0;
}


template <class DerivedT>
bool Database<DerivedT>::StoreSchemaRevision() {
  return this->SetProperty(kSchemaVersionKey,  schema_version_)   &&
         this->SetProperty(kSchemaRevisionKey, schema_revision_);
}


template <class DerivedT>
bool Database<DerivedT>::BeginTransaction() const {
  return begin_transaction_->Execute() &&
         begin_transaction_->Reset();
}


template <class DerivedT>
bool Database<DerivedT>::CommitTransaction() const {
  return commit_transaction_->Execute() &&
         commit_transaction_->Reset();
}


template <class DerivedT>
bool Database<DerivedT>::CreatePropertiesTable() {
  return Sql(sqlite_db(),
    "CREATE TABLE properties (key TEXT, value TEXT, "
    "CONSTRAINT pk_properties PRIMARY KEY (key));").Execute();
}


template <class DerivedT>
bool Database<DerivedT>::HasProperty(const std::string &key) const {
  assert(has_property_);
  const bool retval = has_property_->BindText(1, key) &&
                      has_property_->FetchRow();
  assert(retval);
  const bool result = has_property_->RetrieveInt64(0) > 0;
  has_property_->Reset();
  return result;
}

template <class DerivedT>
template <typename T>
T Database<DerivedT>::GetProperty(const std::string &key) const {
  assert(get_property_);
  const bool retval = get_property_->BindText(1, key) &&
                      get_property_->FetchRow();
  assert(retval);
  const T result = get_property_->Retrieve<T>(0);
  get_property_->Reset();
  return result;
}

template <class DerivedT>
template <typename T>
T Database<DerivedT>::GetPropertyDefault(const std::string &key,
                                         const T default_value) const {
  return (HasProperty(key)) ? GetProperty<T>(key)
                            : default_value;
}

template <class DerivedT>
template <typename T>
bool Database<DerivedT>::SetProperty(const std::string &key,
                                     const T            value) {
  assert(set_property_);
  return set_property_->BindText(1, key) &&
         set_property_->Bind(2, value)   &&
         set_property_->Execute()        &&
         set_property_->Reset();
}

template <class DerivedT>
std::string Database<DerivedT>::GetLastErrorMsg() const {
  const std::string msg = sqlite3_errmsg(sqlite_db());
  return msg;
}


template <class DerivedT>
void Database<DerivedT>::TakeFileOwnership() {
  database_.TakeFileOwnership();
  LogCvmfs(kLogSql, kLogDebug, "Database object took ownership of '%s'",
           database_.filename().c_str());
}


template <class DerivedT>
void Database<DerivedT>::DropFileOwnership() {
  database_.DropFileOwnership();
  LogCvmfs(kLogSql, kLogDebug, "Database object dropped ownership of '%s'",
           database_.filename().c_str());
}


template <class DerivedT>
unsigned Database<DerivedT>::GetModifiedRowCount() const {
  const int modified_rows = sqlite3_total_changes(sqlite_db());
  assert(modified_rows >= 0);
  return static_cast<unsigned>(modified_rows);
}

/**
 * Used to check if the database needs cleanup
 */
template <class DerivedT>
double Database<DerivedT>::GetFreePageRatio() const {
  Sql free_page_count_query(this->sqlite_db(), "PRAGMA freelist_count;");
  Sql page_count_query(this->sqlite_db(), "PRAGMA page_count;");

  const bool retval = page_count_query.FetchRow() &&
                      free_page_count_query.FetchRow();
  assert(retval);

  int64_t pages      = page_count_query.RetrieveInt64(0);
  int64_t free_pages = free_page_count_query.RetrieveInt64(0);
  assert(pages > 0);

  return (static_cast<double>(free_pages) / static_cast<double>(pages));
}


template <class DerivedT>
bool Database<DerivedT>::Vacuum() const {
  assert(read_write_);
  return static_cast<const DerivedT*>(this)->CompactDatabase() &&
         Sql(this->sqlite_db(), "VACUUM;").Execute();
}


template <class DerivedT>
void Database<DerivedT>::PrintSqlError(const std::string &error_msg) {
  LogCvmfs(kLogSql, kLogStderr, "%s\nSQLite said: '%s'",
           error_msg.c_str(), this->GetLastErrorMsg().c_str());
}

template <class DerivedT>
const float Database<DerivedT>::kSchemaEpsilon = 0.0005;
template <class DerivedT>
const std::string Database<DerivedT>::kSchemaVersionKey = "schema";
template <class DerivedT>
const std::string Database<DerivedT>::kSchemaRevisionKey = "schema_revision";


//
// # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
//


template <>
inline bool Sql::Bind(const int index, const int value) {
  return this->BindInt64(index, value);
}

template <>
inline bool Sql::Bind(const int index, const unsigned int value) {
  return this->BindInt64(index, static_cast<int>(value));
}

template <>
inline bool Sql::Bind(const int index, const uint64_t value) {
  return this->BindInt64(index, value);
}

template <>
inline bool Sql::Bind(const int index, const sqlite3_int64 value) {
  return this->BindInt64(index, value);
}

template <>
inline bool Sql::Bind(const int index, const std::string value) {
  return this->BindTextTransient(index, value);
}

template <>
inline bool Sql::Bind(const int index, const char *value) {
  return this->BindTextTransient(index, value, strlen(value));
}

template <>
inline bool Sql::Bind(const int index, const float value) {
  return this->BindDouble(index, value);
}

template <>
inline bool Sql::Bind(const int index, const double value) {
  return this->BindDouble(index, value);
}


template <>
inline int Sql::Retrieve(const int index) {
  return this->RetrieveInt64(index);
}

template <>
inline bool Sql::Retrieve(const int index) {
  return static_cast<bool>(this->RetrieveInt(index));
}

template <>
inline sqlite3_int64 Sql::Retrieve(const int index) {
  return this->RetrieveInt64(index);
}

template <>
inline uint64_t Sql::Retrieve(const int index) {
  return static_cast<uint64_t>(this->RetrieveInt64(index));
}

template <>
inline std::string Sql::Retrieve(const int index) {
  return RetrieveString(index);
}

template <>
inline float Sql::Retrieve(const int index) {
  return this->RetrieveDouble(index);
}

template <>
inline double Sql::Retrieve(const int index) {
  return this->RetrieveDouble(index);
}

}  // namespace sqlite

#endif  // CVMFS_SQL_IMPL_H_
