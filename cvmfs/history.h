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
class SqlRemoveTag;
class SqlFindTag;
class SqlFindTagByDate;
class SqlCountTags;
class SqlListTags;
class SqlGetChannelTips;
class SqlGetHashes;

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

    inline const char* GetChannelName() const {
      switch(channel) {
        case kChannelTrunk: return "trunk";
        case kChannelDevel: return "development";
        case kChannelTest:  return "testing";
        case kChannelProd:  return "production";
        default: assert (false && "unknown channel id");
      }
    }

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

 protected:
  static const std::string kPreviousRevisionKey;

 public:
  ~History();

  static History* Open(const std::string &file_name);
  static History* OpenWritable(const std::string &file_name);
  static History* Create(const std::string &file_name, const std::string &fqrn);

  bool IsWritable() const;
  int GetNumberOfTags() const;

  bool BeginTransaction()  const;
  bool CommitTransaction() const;
  bool SetPreviousRevision(const shash::Any &history_hash);

  bool Insert(const Tag &tag);
  bool Remove(const std::string &name);
  bool Exists(const std::string &name) const;
  bool Get(const std::string &name, Tag *tag) const;
  bool Get(const time_t timestamp, Tag *tag) const;
  bool List(std::vector<Tag> *tags) const;
  bool Tips(std::vector<Tag> *channel_tips) const;

  bool GetHashes(std::vector<shash::Any> *hashes) const;

  const std::string& fqrn() const { return fqrn_; }

 protected:
  static History* Open(const std::string &file_name, const bool read_write);
  bool OpenDatabase(const std::string &file_name, const bool read_write);
  bool CreateDatabase(const std::string &file_name, const std::string &fqrn);

  bool Initialize();
  bool PrepareQueries();

 private:
  template <class SqlListingT>
  bool RunListing(std::vector<Tag> *list, SqlListingT *sql) const;

 private:
  UniquePtr<HistoryDatabase>      database_;
  std::string                     fqrn_;

  UniquePtr<SqlInsertTag>         insert_tag_;
  UniquePtr<SqlRemoveTag>         remove_tag_;
  UniquePtr<SqlFindTag>           find_tag_;
  UniquePtr<SqlFindTagByDate>     find_tag_by_date_;
  UniquePtr<SqlCountTags>         count_tags_;
  UniquePtr<SqlListTags>          list_tags_;
  UniquePtr<SqlGetChannelTips>    channel_tips_;
  UniquePtr<SqlGetHashes>         get_hashes_;
};

}  // namespace hsitory

#endif  // CVMFS_HISTORY_H_
