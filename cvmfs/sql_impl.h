/**
 * This file is part of the CernVM file system.
 */

#ifndef CVMFS_SQL_IMPL_H_
#define CVMFS_SQL_IMPL_H_

#include <fcntl.h>

#include <cassert>
#include <cerrno>
#include <string>

#include "sqlitemem.h"
#include "util/logging.h"
#include "util/platform.h"

namespace sqlite {

template<class DerivedT>
Database<DerivedT>::Database(const std::string &filename,
                             const OpenMode open_mode)
    : database_(filename, this)
    , read_write_(kOpenReadWrite == open_mode)
    , schema_version_(0.0f)
    , schema_revision_(0) { }


template<class DerivedT>
DerivedT *Database<DerivedT>::Create(const std::string &filename) {
  UniquePtr<DerivedT> database(new DerivedT(filename, kOpenReadWrite));

  if (!database.IsValid()) {
    LogCvmfs(kLogSql, kLogDebug, "Failed to create new database object");
    return NULL;
  }

  database->set_schema_version(DerivedT::kLatestSchema);
  database->set_schema_revision(DerivedT::kLatestSchemaRevision);

  const int open_flags = SQLITE_OPEN_NOMUTEX | SQLITE_OPEN_READWRITE
                         | SQLITE_OPEN_CREATE;
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


template<class DerivedT>
DerivedT *Database<DerivedT>::Open(const std::string &filename,
                                   const OpenMode open_mode) {
  UniquePtr<DerivedT> database(new DerivedT(filename, open_mode));

  if (!database.IsValid()) {
    LogCvmfs(kLogSql, kLogDebug,
             "Failed to open database file '%s' - errno: %d", filename.c_str(),
             errno);
    return NULL;
  }

  if (!database->Initialize()) {
    return NULL;
  }

  return database.Release();
}


template<class DerivedT>
bool Database<DerivedT>::Initialize() {
  const int flags = (read_write_) ? SQLITE_OPEN_NOMUTEX | SQLITE_OPEN_READWRITE
                                  : SQLITE_OPEN_NOMUTEX | SQLITE_OPEN_READONLY;

  bool successful = OpenDatabase(flags) && Configure() && FileReadAhead()
                    && PrepareCommonQueries();
  if (!successful) {
    LogCvmfs(kLogSql, kLogDebug, "failed to open database file '%s'",
             filename().c_str());
    return false;
  }

  ReadSchemaRevision();
  LogCvmfs(kLogSql, kLogDebug,
           "opened database with schema version %f "
           "and revision %u",
           schema_version_, schema_revision_);

  if (!static_cast<DerivedT *>(this)->CheckSchemaCompatibility()) {
    LogCvmfs(kLogSql, kLogDebug, "schema version %f not supported (%s)",
             schema_version_, filename().c_str());
    return false;
  }

  if (read_write_
      && !static_cast<DerivedT *>(this)->LiveSchemaUpgradeIfNecessary()) {
    LogCvmfs(kLogSql, kLogDebug, "failed tp upgrade schema revision");
    return false;
  }

  return true;
}


template<class DerivedT>
bool Database<DerivedT>::OpenDatabase(const int flags) {
  // Open database file (depending on the flags read-only or read-write)
  LogCvmfs(kLogSql, kLogDebug, "opening database file %s", filename().c_str());
  int retval = sqlite3_open_v2(filename().c_str(),
                               &database_.sqlite_db,
                               flags | SQLITE_OPEN_EXRESCODE,
                               NULL);
  if (retval != SQLITE_OK) {
    LogCvmfs(kLogSql, kLogDebug, "cannot open database file %s (%d - %d)",
             filename().c_str(), retval, errno);
    return false;
  }

  return true;
}


template<class DerivedT>
Database<DerivedT>::DatabaseRaiiWrapper::~DatabaseRaiiWrapper() {
  if (NULL != sqlite_db) {
    const bool close_successful = Close();
    assert(close_successful);
  }
}


template<class DerivedT>
bool Database<DerivedT>::DatabaseRaiiWrapper::Close() {
  assert(NULL != sqlite_db);

  LogCvmfs(kLogSql, kLogDebug, "closing SQLite database '%s' (unlink: %s)",
           filename().c_str(), (db_file_guard.IsEnabled() ? "yes" : "no"));
  const int result = sqlite3_close(sqlite_db);

  if (result != SQLITE_OK) {
    LogCvmfs(kLogSql, kLogDebug,
             "failed to close SQLite database '%s' (%d - %s)",
             filename().c_str(), result, delegate_->GetLastErrorMsg().c_str());
    return false;
  }

  sqlite_db = NULL;
  if (lookaside_buffer != NULL) {
    SqliteMemoryManager::GetInstance()->ReleaseLookasideBuffer(
        lookaside_buffer);
    lookaside_buffer = NULL;
  }
  return true;
}


template<class DerivedT>
bool Database<DerivedT>::Configure() {
  // Read-only databases should store temporary files in memory.  This avoids
  // unexpected open read-write file descriptors in the cache directory like
  // etilqs_<number>.  They also use the optimized memory manager, if it is
  // available.
  if (!read_write_) {
    if (SqliteMemoryManager::HasInstance()) {
      database_.lookaside_buffer = SqliteMemoryManager::GetInstance()
                                       ->AssignLookasideBuffer(sqlite_db());
    }

    return Sql(sqlite_db(), "PRAGMA temp_store=2;").Execute()
           && Sql(sqlite_db(), "PRAGMA locking_mode=EXCLUSIVE;").Execute();
  }
  return true;
}


template<class DerivedT>
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
    const ssize_t retval = platform_readahead(fd_readahead);
    close(fd_readahead);

    // Read-ahead is known to fail on tmpfs with EINVAL
    // EINVAL = "readahead() cannot be applied to that a file type"
    // It can also fail with ENOSYS on kernel interfaces that do not implement
    // readahead(), such as LX-Brand zones on illumos.
    // Don't consider either a fatal error.
    if (retval != 0 && errno != EINVAL && errno != ENOSYS) {
      LogCvmfs(kLogSql, kLogDebug | kLogSyslogWarn,
               "failed to read-ahead %s: invalid file descrp. or not open for "
               "reading (%d)",
               filename().c_str(), errno);
      return false;
    }
  }

  return true;
}


template<class DerivedT>
bool Database<DerivedT>::PrepareCommonQueries() {
  sqlite3 *db = sqlite_db();
  begin_transaction_ = new Sql(db, "BEGIN;");
  commit_transaction_ = new Sql(db, "COMMIT;");
  has_property_ = new Sql(db, "SELECT count(*) FROM properties "
                              "WHERE key = :key;");
  get_property_ = new Sql(db, "SELECT value FROM properties "
                              "WHERE key = :key;");
  set_property_ = new Sql(db, "INSERT OR REPLACE INTO properties "
                              "(key, value) VALUES (:key, :value);");
  return (begin_transaction_.IsValid() && commit_transaction_.IsValid()
          && has_property_.IsValid() && get_property_.IsValid()
          && set_property_.IsValid());
}


template<class DerivedT>
void Database<DerivedT>::ReadSchemaRevision() {
  schema_version_ = (this->HasProperty(kSchemaVersionKey))
                        ? this->GetProperty<double>(kSchemaVersionKey)
                        : 1.0;
  schema_revision_ = (this->HasProperty(kSchemaRevisionKey))
                         ? this->GetProperty<int>(kSchemaRevisionKey)
                         : 0;
}


template<class DerivedT>
bool Database<DerivedT>::StoreSchemaRevision() {
  return this->SetProperty(kSchemaVersionKey, schema_version_)
         && this->SetProperty(kSchemaRevisionKey, schema_revision_);
}


template<class DerivedT>
bool Database<DerivedT>::BeginTransaction() const {
  return begin_transaction_->Execute() && begin_transaction_->Reset();
}


template<class DerivedT>
bool Database<DerivedT>::CommitTransaction() const {
  return commit_transaction_->Execute() && commit_transaction_->Reset();
}


template<class DerivedT>
bool Database<DerivedT>::CreatePropertiesTable() {
  return Sql(sqlite_db(),
             "CREATE TABLE properties (key TEXT, value TEXT, "
             "CONSTRAINT pk_properties PRIMARY KEY (key));")
      .Execute();
}


template<class DerivedT>
bool Database<DerivedT>::HasProperty(const std::string &key) const {
  assert(has_property_.IsValid());
  const bool retval = has_property_->BindText(1, key)
                      && has_property_->FetchRow();
  assert(retval);
  const bool result = has_property_->RetrieveInt64(0) > 0;
  has_property_->Reset();
  return result;
}

template<class DerivedT>
template<typename T>
T Database<DerivedT>::GetProperty(const std::string &key) const {
  assert(get_property_.IsValid());
  const bool retval = get_property_->BindText(1, key)
                      && get_property_->FetchRow();
  assert(retval);
  const T result = get_property_->Retrieve<T>(0);
  get_property_->Reset();
  return result;
}

template<class DerivedT>
template<typename T>
T Database<DerivedT>::GetPropertyDefault(const std::string &key,
                                         const T default_value) const {
  return (HasProperty(key)) ? GetProperty<T>(key) : default_value;
}

template<class DerivedT>
template<typename T>
bool Database<DerivedT>::SetProperty(const std::string &key, const T value) {
  assert(set_property_.IsValid());
  return set_property_->BindText(1, key) && set_property_->Bind(2, value)
         && set_property_->Execute() && set_property_->Reset();
}

template<class DerivedT>
std::string Database<DerivedT>::GetLastErrorMsg() const {
  std::string msg = sqlite3_errmsg(sqlite_db());
  return msg;
}


template<class DerivedT>
void Database<DerivedT>::TakeFileOwnership() {
  database_.TakeFileOwnership();
  LogCvmfs(kLogSql, kLogDebug, "Database object took ownership of '%s'",
           database_.filename().c_str());
}


template<class DerivedT>
void Database<DerivedT>::DropFileOwnership() {
  database_.DropFileOwnership();
  LogCvmfs(kLogSql, kLogDebug, "Database object dropped ownership of '%s'",
           database_.filename().c_str());
}


template<class DerivedT>
unsigned Database<DerivedT>::GetModifiedRowCount() const {
  const int modified_rows = sqlite3_total_changes(sqlite_db());
  assert(modified_rows >= 0);
  return static_cast<unsigned>(modified_rows);
}

/**
 * Ask SQlite for per-connection memory statistics
 */
template<class DerivedT>
void Database<DerivedT>::GetMemStatistics(MemStatistics *stats) const {
  const int reset = 0;
  int current;
  int highwater;
  int retval = SQLITE_OK;
  retval |= sqlite3_db_status(sqlite_db(), SQLITE_DBSTATUS_LOOKASIDE_USED,
                              &current, &highwater, reset);
  stats->lookaside_slots_used = current;
  stats->lookaside_slots_max = highwater;
  retval |= sqlite3_db_status(sqlite_db(), SQLITE_DBSTATUS_LOOKASIDE_HIT,
                              &current, &highwater, reset);
  stats->lookaside_hit = highwater;
  retval |= sqlite3_db_status(sqlite_db(), SQLITE_DBSTATUS_LOOKASIDE_MISS_SIZE,
                              &current, &highwater, reset);
  stats->lookaside_miss_size = highwater;
  retval |= sqlite3_db_status(sqlite_db(), SQLITE_DBSTATUS_LOOKASIDE_MISS_FULL,
                              &current, &highwater, reset);
  stats->lookaside_miss_full = highwater;
  retval |= sqlite3_db_status(sqlite_db(), SQLITE_DBSTATUS_CACHE_USED, &current,
                              &highwater, reset);
  stats->page_cache_used = current;
  retval |= sqlite3_db_status(sqlite_db(), SQLITE_DBSTATUS_CACHE_HIT, &current,
                              &highwater, reset);
  stats->page_cache_hit = current;
  retval |= sqlite3_db_status(sqlite_db(), SQLITE_DBSTATUS_CACHE_MISS, &current,
                              &highwater, reset);
  stats->page_cache_miss = current;
  retval |= sqlite3_db_status(sqlite_db(), SQLITE_DBSTATUS_SCHEMA_USED,
                              &current, &highwater, reset);
  stats->schema_used = current;
  retval |= sqlite3_db_status(sqlite_db(), SQLITE_DBSTATUS_STMT_USED, &current,
                              &highwater, reset);
  stats->stmt_used = current;
  assert(retval == SQLITE_OK);
}


template<class DerivedT>
double Database<DerivedT>::GetFreePageRatio() const {
  Sql free_page_count_query(this->sqlite_db(), "PRAGMA freelist_count;");
  Sql page_count_query(this->sqlite_db(), "PRAGMA page_count;");

  const bool retval = page_count_query.FetchRow()
                      && free_page_count_query.FetchRow();
  assert(retval);

  const int64_t pages = page_count_query.RetrieveInt64(0);
  const int64_t free_pages = free_page_count_query.RetrieveInt64(0);
  assert(pages > 0);

  return (static_cast<double>(free_pages) / static_cast<double>(pages));
}


template<class DerivedT>
bool Database<DerivedT>::Vacuum() const {
  assert(read_write_);
  return static_cast<const DerivedT *>(this)->CompactDatabase()
         && Sql(this->sqlite_db(), "VACUUM;").Execute();
}


template<class DerivedT>
void Database<DerivedT>::PrintSqlError(const std::string &error_msg) {
  LogCvmfs(kLogSql, kLogStderr, "%s\nSQLite said: '%s'", error_msg.c_str(),
           this->GetLastErrorMsg().c_str());
}

template<class DerivedT>
const float Database<DerivedT>::kSchemaEpsilon = 0.0005;
template<class DerivedT>
const char *Database<DerivedT>::kSchemaVersionKey = "schema";
template<class DerivedT>
const char *Database<DerivedT>::kSchemaRevisionKey = "schema_revision";


//
// # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
//


template<>
inline bool Sql::Bind(const int index, const int &value) {
  return this->BindInt64(index, value);
}

template<>
inline bool Sql::Bind(const int index, const unsigned int &value) {
  return this->BindInt64(index, static_cast<int>(value));
}

template<>
inline bool Sql::Bind(const int index, const uint64_t &value) {
  return this->BindInt64(index, static_cast<int64_t>(value));
}

template<>
inline bool Sql::Bind(const int index, const sqlite3_int64 &value) {
  return this->BindInt64(index, value);
}

template<>
inline bool Sql::Bind(const int index, const std::string &value) {
  return this->BindTextTransient(index, value);
}

template<>
inline bool Sql::Bind(const int index, const float &value) {
  return this->BindDouble(index, value);
}

template<>
inline bool Sql::Bind(const int index, const double &value) {
  return this->BindDouble(index, value);
}


template<>
inline int Sql::Retrieve(const int index) {
  return static_cast<int>(this->RetrieveInt64(index));
}

template<>
inline bool Sql::Retrieve(const int index) {
  return static_cast<bool>(this->RetrieveInt(index));
}

template<>
inline sqlite3_int64 Sql::Retrieve(const int index) {
  return this->RetrieveInt64(index);
}

template<>
inline uint64_t Sql::Retrieve(const int index) {
  return static_cast<uint64_t>(this->RetrieveInt64(index));
}

template<>
inline std::string Sql::Retrieve(const int index) {
  return RetrieveString(index);
}

template<>
inline float Sql::Retrieve(const int index) {
  return static_cast<float>(this->RetrieveDouble(index));
}

template<>
inline double Sql::Retrieve(const int index) {
  return this->RetrieveDouble(index);
}

}  // namespace sqlite

#endif  // CVMFS_SQL_IMPL_H_
