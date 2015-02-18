/**
 * This file is part of the CernVM file system.
 */

#include "sql.h"
#include "logging.h"
#include "util.h"

using namespace std;  // NOLINT

namespace sqlite {

Sql::Sql(sqlite3 *sqlite_db, const std::string &statement) {
  Init(sqlite_db, statement);
}


Sql::~Sql() {
  last_error_code_ = sqlite3_finalize(statement_);

  if (!Successful()) {
    LogCvmfs(kLogSql, kLogDebug,
             "failed to finalize statement - error code: %d", last_error_code_);
  }
  LogCvmfs(kLogSql, kLogDebug, "successfully finalized statement");
}


/**
 * Executes the prepared statement.
 * (this method should be used for modifying statements like DELETE or INSERT)
 * @return true on success otherwise false
 */
bool Sql::Execute() {
  last_error_code_ = sqlite3_step(statement_);
#ifdef DEBUGMSG
  if (! Successful()) {
    LogCvmfs(kLogSql, kLogDebug, "SQL query failed - SQLite: %d - %s",
             GetLastError(), GetLastErrorMsg().c_str());
  }
#endif
  return Successful();
}


/**
 * Execute the prepared statement or fetch its next row.
 * This method is intended to step through the result set.
 * If it returns false this does not neccessarily mean, that the actual
 * statement execution failed, but that no row was fetched.
 * @return true if a new row was fetched otherwise false
 */
bool Sql::FetchRow() {
  last_error_code_ = sqlite3_step(statement_);
  return SQLITE_ROW == last_error_code_;
}


std::string Sql::DebugResultTable() {
  std::string line;
  std::string result;
  unsigned int rows = 0;

  // go through all data rows
  while (FetchRow()) {
    // retrieve the table header (once)
    const unsigned int cols = sqlite3_column_count(statement_);
    if (rows == 0) {
      for (unsigned int col = 0; col < cols; ++col) {
        const char *name = sqlite3_column_name(statement_, col);
        line += name;
        if (col + 1 < cols) line += " | ";
      }
      result += line + "\n";
      line.clear();
    }

    // retrieve the data fields for each row
    for (unsigned int col = 0; col < cols; ++col) {
      const int type = sqlite3_column_type(statement_, col);
      switch(type) {
        case SQLITE_INTEGER:
          line += StringifyInt(RetrieveInt64(col));
          break;
        case SQLITE_FLOAT:
          line += StringifyDouble(RetrieveDouble(col));
          break;
        case SQLITE_TEXT:
          line += (char*)RetrieveText(col);
          break;
        case SQLITE_BLOB:
          line += "[BLOB data]";
          break;
        case SQLITE_NULL:
          line += "[NULL]";
          break;
      }
      if (col + 1 < cols) line += " | ";
    }

    result += line + "\n";
    line.clear();
    ++rows;
  }

  // print the result
  result += "Retrieved Rows: " + StringifyInt(rows);
  return result;
}


/**
 * Reset a prepared statement to make it reusable.
 * @return true on success otherwise false
 */
bool Sql::Reset() {
  last_error_code_ = sqlite3_reset(statement_);
  return Successful();
}


bool Sql::Init(const sqlite3 *database, const std::string &statement) {
  last_error_code_ = sqlite3_prepare_v2((sqlite3*)database,
                                        statement.c_str(),
                                        -1, // parse until null termination
                                        &statement_,
                                        NULL);

  if (!Successful()) {
    LogCvmfs(kLogSql, kLogDebug, "failed to prepare statement '%s' (%d: %s)",
             statement.c_str(), GetLastError(),
             sqlite3_errmsg((sqlite3*)database));
    return false;
  }

  LogCvmfs(kLogSql, kLogDebug, "successfully prepared statement '%s'",
           statement.c_str());
  return true;
}

std::string Sql::GetLastErrorMsg() const {
  sqlite3* db     = sqlite3_db_handle(statement_);
  std::string msg = sqlite3_errmsg(db);
  return msg;
}

}  // namespace sqlite
