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

#include "hash.h"
#include "util.h"

namespace history {

class HistoryDatabase;
class SqlInsertTag;
class SqlFindTag;
class SqlCountTags;
class SqlListTags;

class History {
 public:
  enum UpdateChannel {
    kChannelTrunk = 0,
    kChannelDevel = 4,
    kChannelTest = 16,
    kChannelProd = 64,
  };

  struct Tag {
    Tag() :
      size(0), revision(0), timestamp(0), channel(kChannelTrunk) {}

    Tag(const std::string &n, const shash::Any &h, const uint64_t s,
        const unsigned r, const time_t t, const UpdateChannel c,
        const std::string &d) :
      name(n), root_hash(h), size(s), revision(r), timestamp(t), channel(c),
      description(d) {}

    bool operator ==(const Tag &other) const {
      return this->revision == other.revision;
    }

    bool operator <(const Tag &other) const {
      return this->revision < other.revision;
    }

    std::string    name;
    shash::Any     root_hash;
    uint64_t       size;
    unsigned       revision;
    time_t         timestamp;
    UpdateChannel  channel;
    std::string    description;
  };

 public:
  ~History();

  static History* Open(const std::string &file_name);
  static History* Create(const std::string &file_name, const std::string &fqrn);

  int GetNumberOfTags() const;
  bool BeginTransaction()  const;
  bool CommitTransaction() const;

  bool Insert(const Tag &tag);
  bool Find(const std::string &name, Tag *tag) const;
  bool List(std::vector<Tag> *tags) const;

  const std::string& fqrn() const { return fqrn_; }


 protected:
  bool OpenDatabase(const std::string &file_name);
  bool CreateDatabase(const std::string &file_name, const std::string &fqrn);

  bool Initialize();
  bool PrepareQueries();

 private:
  UniquePtr<HistoryDatabase>   database_;
  std::string                  fqrn_;

  UniquePtr<SqlInsertTag>      insert_tag_;
  UniquePtr<SqlFindTag>        find_tag_;
  UniquePtr<SqlCountTags>      count_tags_;
  UniquePtr<SqlListTags>       list_tags_;
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
