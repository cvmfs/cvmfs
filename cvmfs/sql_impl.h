/**
 * This file is part of the CernVM file system.
 */

#include <fcntl.h>
#include <cerrno>
#include <cassert>

#include "platform.h"
#include "logging.h"

namespace sqlite {


template <class DerivedT>
Database<DerivedT>::Database(const std::string  &filename,
                             const OpenMode      open_mode) :
  sqlite_db_(NULL),
  filename_(filename),
  read_write_(kOpenReadWrite == open_mode),
  schema_version_(0.0f),
  schema_revision_(0) {}


template <class DerivedT>
DerivedT* Database<DerivedT>::Create(const std::string &filename) {
  DerivedT *database = new DerivedT(filename, kOpenReadWrite);
  database->set_schema_version(DerivedT::kLatestSchema);
  database->set_schema_revision(DerivedT::kLatestSchemaRevision);

  if (! database) {
    LogCvmfs(kLogSql, kLogDebug, "Failed to create new database object");
    return NULL;
  }

  const int open_flags = SQLITE_OPEN_NOMUTEX | SQLITE_OPEN_READWRITE |
                         SQLITE_OPEN_CREATE;
  if (! database->OpenDatabase(open_flags)) {
    LogCvmfs(kLogSql, kLogDebug, "Failed to create new database file");
    return NULL;
  }

  if (! database->CreatePropertiesTable()) {
    database->PrintSqlError("Failed to create common properties table");
    return NULL;
  }

  if (! database->CreateEmptyDatabase()) {
    database->PrintSqlError("Failed to create empty database");
    return NULL;
  }

  if (! database->PreparePropertiesQueries()) {
    database->PrintSqlError("Failed to initialize properties queries");
    return NULL;
  }

  if (! database->StoreSchemaRevision()) {
    database->PrintSqlError("Failed to store initial schema revision");
    return NULL;
  }

  return database;
}


template <class DerivedT>
DerivedT* Database<DerivedT>::Open(const std::string  &filename,
                                   const OpenMode      open_mode) {
  DerivedT* database = new DerivedT(filename, open_mode);
  if (! database->Initialize()) {
    delete database;
    database = NULL;
  }

  return database;
}


template <class DerivedT>
bool Database<DerivedT>::Initialize() {
  const int flags = (read_write_) ? SQLITE_OPEN_NOMUTEX | SQLITE_OPEN_READWRITE
                                  : SQLITE_OPEN_NOMUTEX | SQLITE_OPEN_READONLY;

  const bool successful = OpenDatabase(flags)        &&
                          FileReadAhead()            &&
                          PreparePropertiesQueries();
  if (! successful) {
    LogCvmfs(kLogSql, kLogDebug, "failed to open database file '%s'",
                                 filename_.c_str());
    return false;
  }

  ReadSchemaRevision();
  LogCvmfs(kLogSql, kLogDebug, "opened database with schema version %f "
                               "and revision %u",
                               schema_version_, schema_revision_);

  if (! static_cast<DerivedT*>(this)->CheckSchemaCompatibility()) {
    LogCvmfs(kLogSql, kLogDebug, "schema version %f not supported (%s)",
             schema_version_, filename_.c_str());
    return false;
  }

  if (read_write_ &&
      ! static_cast<DerivedT*>(this)->LiveSchemaUpgradeIfNecessary()) {
    LogCvmfs(kLogSql, kLogDebug, "failed tp upgrade schema revision");
    return false;
  }

  return true;
}


template <class DerivedT>
bool Database<DerivedT>::OpenDatabase(const int flags) {
  // Open database file (depending on the flags read-only or read-write)
  LogCvmfs(kLogSql, kLogDebug, "opening database file %s",
           filename_.c_str());
  if (SQLITE_OK != sqlite3_open_v2(filename_.c_str(), &sqlite_db_, flags, NULL))
  {
    LogCvmfs(kLogSql, kLogDebug, "cannot open database file %s",
             filename_.c_str());
    return false;
  }

  const int retval = sqlite3_extended_result_codes(sqlite_db_, 1);
  assert (SQLITE_OK == retval);

  return true;
}


template <class DerivedT>
Database<DerivedT>::~Database() {
  if (NULL != sqlite_db_) {
    sqlite3_close(sqlite_db_);
    sqlite_db_ = NULL;
  }
}


template <class DerivedT>
bool Database<DerivedT>::FileReadAhead() {
  // Read-ahead into file system buffers
  // TODO: mmap, re-readahead
  int fd_readahead = open(filename_.c_str(), O_RDONLY);
  if (fd_readahead < 0) {
    LogCvmfs(kLogSql, kLogDebug, "failed to open %s for read-ahead (%d)",
             filename_.c_str(), errno);
    return false;
  }

  const int retval = platform_readahead(fd_readahead);
  close(fd_readahead);
  if (retval != 0) {
    LogCvmfs(kLogSql, kLogDebug | kLogSyslogWarn,
             "failed to read-ahead %s (%d)", filename_.c_str(), errno);
    return false;
  }

  return true;
}


template <class DerivedT>
bool Database<DerivedT>::PreparePropertiesQueries() {
  has_property_ = new Sql(sqlite_db_, "SELECT count(*) FROM properties "
                                      "WHERE key = :key;");
  get_property_ = new Sql(sqlite_db_, "SELECT value FROM properties "
                                      "WHERE key = :key;");
  set_property_ = new Sql(sqlite_db_, "INSERT OR REPLACE INTO properties "
                                      "(key, value) VALUES (:key, :value);");
  return (has_property_ && get_property_ && set_property_);
}


template <class DerivedT>
bool Database<DerivedT>::ReadSchemaRevision() {
  Sql sql_schema(this->sqlite_db(),
                 "SELECT value FROM properties WHERE key='schema';");
  schema_version_ = (sql_schema.FetchRow()) ? sql_schema.RetrieveDouble(0)
                                            : 1.0;
  Sql sql_revision(this->sqlite_db(),
                   "SELECT value FROM properties WHERE key='schema_revision';");
  if (sql_revision.FetchRow()) {
    schema_revision_ = sql_revision.RetrieveInt64(0);
  }

  return true;
}


template <class DerivedT>
bool Database<DerivedT>::StoreSchemaRevision() const {
  Sql store_schema_revision(this->sqlite_db(),
    "INSERT OR REPLACE INTO properties (key, value) "
    "  VALUES ('schema',          :schema), "
    "         ('schema_revision', :schema_revision);");
  return store_schema_revision.BindDouble(1, schema_version_)  &&
         store_schema_revision.BindDouble(2, schema_revision_) &&
         store_schema_revision.Execute();
}


template <class DerivedT>
bool Database<DerivedT>::CreatePropertiesTable() {
  return Sql(sqlite_db_,
    "CREATE TABLE properties (key TEXT, value TEXT, "
    "CONSTRAINT pk_properties PRIMARY KEY (key));").Execute();
}


template <class DerivedT>
bool Database<DerivedT>::HasProperty(const std::string &key) const {
  assert (has_property_ && ! has_property_->IsBusy());
  const bool retval = has_property_->BindText(1, key) &&
                      has_property_->FetchRow();
  assert (retval);
  const bool result = has_property_->RetrieveInt64(0) > 0;
  has_property_->Reset();
  return result;
}

template <class DerivedT>
template <typename T>
T Database<DerivedT>::GetProperty(const std::string &key) const {
  assert (get_property_ && ! get_property_->IsBusy());
  const bool retval = get_property_->BindText(1, key);
                      get_property_->FetchRow();
  assert (retval);
  const T result = get_property_->Retrieve<T>(0);
  get_property_->Reset();
  return result;
}

template <class DerivedT>
template <typename T>
bool Database<DerivedT>::SetProperty(const std::string &key,
                                     const T            value) {
  assert (set_property_ && ! set_property_->IsBusy());
  return set_property_->BindText(1, key) &&
         set_property_->Bind(2, value)   &&
         set_property_->Execute()        &&
         set_property_->Reset();
}


template <class DerivedT>
std::string Database<DerivedT>::GetLastErrorMsg() const {
  const std::string msg = sqlite3_errmsg(sqlite_db_);
  return msg;
}


/**
 * Used to check if the database needs cleanup
 */
template <class DerivedT>
double Database<DerivedT>::GetFreePageRatio() const {
  Sql free_page_count_query(this->sqlite_db(), "PRAGMA freelist_count;");
  Sql page_count_query     (this->sqlite_db(), "PRAGMA page_count;");

  const bool retval = page_count_query.FetchRow() &&
                      free_page_count_query.FetchRow();
  assert (retval);

  int64_t pages      = page_count_query.RetrieveInt64(0);
  int64_t free_pages = free_page_count_query.RetrieveInt64(0);
  assert (pages > 0);

  return ((double)free_pages) / ((double)pages);
}


template <class DerivedT>
bool Database<DerivedT>::Vacuum() const {
  assert (read_write_);
  return static_cast<const DerivedT*>(this)->CompactizeDatabase() &&
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
inline bool Sql::Bind(const int index, const sqlite3_int64 value) {
  return this->BindInt64(index, value);
}

template <>
inline bool Sql::Bind(const int index, const std::string value) {
  return this->BindText(index, value);
}

template <>
inline bool Sql::Bind(const int index, const char *value) {
  return this->BindText(index, value);
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
inline sqlite3_int64 Sql::Retrieve(const int index) {
  return this->RetrieveInt64(index);
}

template <>
inline std::string Sql::Retrieve(const int index) {
  return reinterpret_cast<const char *>(this->RetrieveText(index));
}

template <>
inline float Sql::Retrieve(const int index) {
  return this->RetrieveDouble(index);
}

template <>
inline double Sql::Retrieve(const int index) {
  return this->RetrieveDouble(index);
}

}
