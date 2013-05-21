/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_HISTORY_H_
#define CVMFS_HISTORY_H_

#include <string>
#include <vector>
#include <set>
#include <map>

#include "sql.h"
#include "hash.h"

namespace history {

enum UpdateChannel {
  kChannelTrunk = 0,
  kChannelDevel = 0x10,
  kChannelTest = 0x20,
  kChannelProd = 0x30,
};


struct Tag {
  Tag() {
    revision = 0;
    timestamp = 0;
    channel = kChannelTrunk;
  }

  Tag(const std::string &n, const hash::Any &h, const unsigned r,
      const time_t t, const UpdateChannel c, const std::string &d)
  {
    name = n;
    root_hash = h;
    revision = r;
    timestamp = t;
    channel = c;
    description = d;
  }

  std::string name;
  hash::Any root_hash;
  unsigned revision;
  time_t timestamp;
  UpdateChannel channel;
  std::string description;
};


class Database {
 public:
  static const float kLatestSchema;
  static const float kLatestSupportedSchema;
  static const float kSchemaEpsilon;  // floats get imprecise in SQlite

  Database() {
    sqlite_db_ = NULL;
    schema_version_ = 0.0;
    read_write_ = false;
    ready_ = false;
  }
  ~Database();
  static bool Create(const std::string &filename,
                     const std::string &repository_name);
  bool Open(const std::string filename, const sqlite::DbOpenMode open_mode);

  sqlite3 *sqlite_db() const { return sqlite_db_; }
  std::string filename() const { return filename_; }
  float schema_version() const { return schema_version_; }
  bool ready() const { return ready_; }

  std::string GetLastErrorMsg() const;
 private:
  Database(sqlite3 *sqlite_db, const float schema, const bool rw);

  sqlite3 *sqlite_db_;
  std::string filename_;
  float schema_version_;
  bool read_write_;
  bool ready_;
};


class SqlTag : public sqlite::Sql {
 public:
  SqlTag(const Database &database, const std::string &statement) {
    Init(database.sqlite_db(), statement);
  }
  virtual ~SqlTag() { /* Done by super class */ }

  bool BindTag(const Tag &tag);
  Tag RetrieveTag();
};


class TagList {
 public:
  enum Failures {
    kFailOk = 0,
    kFailTagExists,
  };

  bool FindTag(const std::string &name, Tag *tag);
  bool FindRevision(const unsigned revision, Tag *tag);
  Failures Insert(const Tag &tag);
  void Remove(const std::string &name);
  std::set<hash::Any> GetAllHashes();
  std::map<UpdateChannel, hash::Any> GetChannelTops();
  std::string List();

  bool Load(Database *database);
  bool Store(Database *database);
 private:
  std::vector<Tag> list_;
};

}  // namespace hsitory

#endif  // CVMFS_HISTORY_H_
