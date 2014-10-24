/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_HISTORY_H_
#define CVMFS_HISTORY_H_

#include <stdint.h>
#include <time.h>

#include <string>
#include <vector>
#include <set>
#include <map>

#include "sql.h"
#include "hash.h"

namespace history {

enum UpdateChannel {
  kChannelTrunk = 0,
  kChannelDevel = 4,
  kChannelTest = 16,
  kChannelProd = 64,
};


struct Tag {
  Tag() {
    size = 0;
    revision = 0;
    timestamp = 0;
    channel = kChannelTrunk;
  }

  Tag(const std::string &n, const shash::Any &h, const uint64_t s,
      const unsigned r, const time_t t, const UpdateChannel c,
      const std::string &d)
  {
    name = n;
    root_hash = h;
    size = s;
    revision = r;
    timestamp = t;
    channel = c;
    description = d;
  }

  bool operator ==(const Tag &other) const {
    return this->revision == other.revision;
  }

  bool operator <(const Tag &other) const {
    return this->revision < other.revision;
  }

  std::string name;
  shash::Any root_hash;
  uint64_t size;
  unsigned revision;
  time_t timestamp;
  UpdateChannel channel;
  std::string description;
};


class HistoryDatabase : public sqlite::Database<HistoryDatabase> {
 public:
  static const float kLatestSchema;
  static const float kLatestSupportedSchema;
  // backwards-compatible schema changes
  static const unsigned kLatestSchemaRevision;

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


class SqlTag : public sqlite::Sql {
 public:
  SqlTag(const HistoryDatabase &database, const std::string &statement) {
    Init(database.sqlite_db(), statement);
  }
  virtual ~SqlTag() { /* Done by super class */ }

  bool BindTag(const Tag &tag);
  Tag RetrieveTag();
};


class TagList {
 public:
  struct ChannelTag {
    ChannelTag(const UpdateChannel c, const shash::Any &h) :
      channel(c), root_hash(h) { }
    UpdateChannel channel;
    shash::Any root_hash;
  };

  enum Failures {
    kFailOk = 0,
    kFailTagExists,
  };

  bool FindTag(const std::string &name, Tag *tag);
  bool FindTagByDate(const time_t seconds_utc, Tag *tag);
  bool FindRevision(const unsigned revision, Tag *tag);
  bool FindHash(const shash::Any &hash, Tag *tag);
  Failures Insert(const Tag &tag);
  void Remove(const std::string &name);
  void Rollback(const unsigned until_revision);
  // Ordered list, newest releases first
  std::vector<ChannelTag> GetChannelTops();
  std::string List();
  std::map<std::string, shash::Any> GetAllHashes();

  /**
   * This returns a list of referenced catalog root hashes sorted by revision
   * from HEAD to tail.
   * @return  a sorted list of all referenced root catalog hashes.
   */
  std::vector<shash::Any> GetReferencedHashes() const;

  bool Load(HistoryDatabase *database);
  bool Store(HistoryDatabase *database);
 private:
  std::vector<Tag> list_;
};

}  // namespace hsitory

#endif  // CVMFS_HISTORY_H_
