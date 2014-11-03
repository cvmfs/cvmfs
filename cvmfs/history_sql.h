/**
 * This file is part of the CernVM File System.
 */

#include <string>

#include "sql.h"
#include "history.h"

namespace history {

class HistoryDatabase : public sqlite::Database<HistoryDatabase> {
 public:
  static const float kLatestSchema;
  static const float kLatestSupportedSchema;
  // backwards-compatible schema changes
  static const unsigned kLatestSchemaRevision;

  static const std::string kFqrnKey;

  bool CreateEmptyDatabase();
  bool InsertInitialValues(const std::string &repository_name);

  bool CheckSchemaCompatibility();
  bool LiveSchemaUpgradeIfNecessary();
  bool CompactizeDatabase() const;

 protected:
  // TODO: C++11 - constructor inheritance
  friend class sqlite::Database<HistoryDatabase>;
  HistoryDatabase(const std::string  &filename,
                  const OpenMode      open_mode) :
    sqlite::Database<HistoryDatabase>(filename, open_mode) {}
};


//------------------------------------------------------------------------------


class SqlRetrieveTag : public sqlite::Sql {
 protected:
  static const std::string tag_database_fields;

 public:
  History::Tag RetrieveTag() const;

 protected:
  const std::string& GetDatabaseFields() const; // this is done to allow for
                                                // future schema flexibility
};


class SqlInsertTag : public SqlRetrieveTag {
 protected:
  static const std::string tag_database_placeholders;

 public:
  SqlInsertTag(const HistoryDatabase *database);
  bool BindTag(const History::Tag &tag);

 protected:
  const std::string& GetDatabasePlaceholders() const; // same reason as for
                                                      // SqlRetrieveTag::...()
};


class SqlRemoveTag : public sqlite::Sql {
 public:
  SqlRemoveTag(const HistoryDatabase *database);
  bool BindName(const std::string &name);
};


class SqlFindTag : public SqlRetrieveTag {
 public:
  SqlFindTag(const HistoryDatabase *database);
  bool BindName(const std::string &name);
};


class SqlFindTagByDate : public SqlRetrieveTag {
 public:
  SqlFindTagByDate(const HistoryDatabase *database);
  bool BindTimestamp(const time_t timestamp);
};


class SqlCountTags : public sqlite::Sql {
 public:
  SqlCountTags(const HistoryDatabase *database);
  int RetrieveCount() const;
};


class SqlListTags : public SqlRetrieveTag {
 public:
  SqlListTags(const HistoryDatabase *database);
};


class SqlGetChannelTips : public SqlRetrieveTag {
 public:
  SqlGetChannelTips(const HistoryDatabase *database);
};


class SqlGetHashes : public sqlite::Sql {
 public:
  SqlGetHashes(const HistoryDatabase *database);
  shash::Any RetrieveHash() const;
};









class SqlTag : public sqlite::Sql {
 public:
  SqlTag(const HistoryDatabase &database, const std::string &statement) {
    Init(database.sqlite_db(), statement);
  }

  bool BindTag(const History::Tag &tag);
  History::Tag RetrieveTag();
};




} /* namespace history */
