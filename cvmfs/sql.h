/**
 * This file is part of the CernVM file system.
 */

#ifndef CVMFS_SQL_H_
#define CVMFS_SQL_H_

#include <string>
#include "duplex_sqlite3.h"
#include "util.h"

namespace sqlite {

/**
 * Encapsulates an SQlite connection.
 * Note: This implements a Curiously Recurring Template Pattern in order to
 *       implement Database::Create and Database::Open as a static polymorphism
 * TODO: C++11 Move Constructors to allow for stack allocated database
 */
template <class DerivedT>
class Database : SingleCopy {
 public:
  enum OpenMode {
    kOpenReadOnly,
    kOpenReadWrite,
  };

 public:
  static const float kSchemaEpsilon;  // floats get imprecise in SQlite

  static DerivedT* Create(const std::string  &filename);
  static DerivedT*   Open(const std::string  &filename,
                          const OpenMode      open_mode);

  ~Database();

  bool IsEqualSchema(const float value, const float compare) const {
    return (value > compare - kSchemaEpsilon &&
            value < compare + kSchemaEpsilon);
  }

  sqlite3     *sqlite_db()       const { return sqlite_db_;       }
  std::string  filename()        const { return filename_;        }
  float        schema_version()  const { return schema_version_;  }
  unsigned     schema_revision() const { return schema_revision_; }
  bool         read_write()      const { return read_write_;      }

  double GetFreePageRatio() const;
  bool Vacuum() const;

  void PrintSqlError(const std::string &error_msg);

  /**
   * Returns the english language error description of the last error
   * happened in the context of the encapsulated sqlite3 database object.
   * Note: In a multithreaded context it might be unpredictable which
   *       the actual last error is.
   * @return   english language error description of last error
   */
  std::string GetLastErrorMsg() const;

 protected:
  Database(const std::string  &filename,
           const OpenMode      open_mode);

  bool Initialize();

  bool CreatePropertiesTable();

  bool OpenDatabase(const int sqlite_open_flags);
  bool FileReadAhead();
  bool ReadSchemaRevision();
  bool StoreSchemaRevision() const;

  void set_schema_version(const float ver)     { schema_version_  = ver; }
  void set_schema_revision(const unsigned rev) { schema_revision_ = rev; }

 private:
  sqlite3            *sqlite_db_;
  const std::string   filename_;
  const bool          read_write_;
  float               schema_version_;
  unsigned            schema_revision_;
};

/**
 * Base class for all SQL statement classes.  It wraps a single SQL statement
 * and all neccessary calls of the sqlite3 API to deal with this statement.
 */
class Sql {
 public:
  /**
   * Basic constructor to use this class for a specific statement.
   * @param database the database to use the query on
   * @param statement the statement to prepare
   */
  Sql(sqlite3 *sqlite_db, const std::string &statement);
  virtual ~Sql();

  bool Execute();
  bool FetchRow();
  std::string DebugResultTable();
  bool Reset();
  inline int GetLastError() const { return last_error_code_; }

  /**
   * Returns the english language error description of the last error
   * happened in the context of the sqlite3 database object this statement is
   * registered to.
   * Note: In a multithreaded context it might be unpredictable which
   *       the actual last error is.
   * @return   english language error description of last error
   */
  std::string GetLastErrorMsg() const;

  bool BindBlob(const int index, const void* value, const int size) {
    last_error_code_ = sqlite3_bind_blob(statement_, index, value, size,
                                         SQLITE_STATIC);
    return Successful();
  }
  bool BindDouble(const int index, const double value) {
    last_error_code_ = sqlite3_bind_double(statement_, index, value);
    return Successful();
  }
  bool BindInt(const int index, const int value) {
    last_error_code_ = sqlite3_bind_int(statement_, index, value);
    return Successful();
  }
  bool BindInt64(const int index, const sqlite3_int64 value) {
    last_error_code_ = sqlite3_bind_int64(statement_, index, value);
    return Successful();
  }
  bool BindNull(const int index) {
    last_error_code_ = sqlite3_bind_null(statement_, index);
    return Successful();
  }
  bool BindTextTransient(const int index, const std::string &value) {
    return BindText(index, value.data(), value.length(), SQLITE_TRANSIENT);
  }
  bool BindText(const int index, const std::string &value) {
    return BindText(index, value.data(), value.length(), SQLITE_STATIC);
  }
  bool BindText(const int   index,
                const char* value,
                const int   size,
                void(*dtor)(void*) = SQLITE_STATIC) {
    last_error_code_ = sqlite3_bind_text(statement_, index, value, size, dtor);
    return Successful();
  }


  int RetrieveType(const int idx_column) const {
    return sqlite3_column_type(statement_, idx_column);
  }
  int RetrieveBytes(const int idx_column) const {
    return sqlite3_column_bytes(statement_, idx_column);
  }
  const void *RetrieveBlob(const int idx_column) const {
    return sqlite3_column_blob(statement_, idx_column);
  }
  double RetrieveDouble(const int idx_column) const {
    return sqlite3_column_double(statement_, idx_column);
  }
  int RetrieveInt(const int idx_column) const {
    return sqlite3_column_int(statement_, idx_column);
  }
  sqlite3_int64 RetrieveInt64(const int idx_column) const {
    return sqlite3_column_int64(statement_, idx_column);
  }
  const unsigned char *RetrieveText(const int idx_column) const {
    return sqlite3_column_text(statement_, idx_column);
  }

  /**
   * Checks if a statement is currently busy with a transaction
   * i.e. Reset() was not yet called on it.
   */
  inline bool IsBusy() const {
    return (bool)sqlite3_stmt_busy(statement_);
  }

 protected:
  Sql() : statement_(NULL), last_error_code_(0) { }
  bool Init(const sqlite3 *database, const std::string &statement);

  /**
   * Checks the last action for success
   * @return true if last action succeeded otherwise false
   */
  inline bool Successful() const {
    return SQLITE_OK   == last_error_code_ ||
    SQLITE_ROW  == last_error_code_ ||
    SQLITE_DONE == last_error_code_;
  }

  sqlite3_stmt *statement_;
  int last_error_code_;
};

}  // namespace sqlite

#include "sql_impl.h"

#endif  // CVMFS_SQL_H_
