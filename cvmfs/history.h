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
class SqlRollbackTag;

/**
 * This class wraps the history of a repository, i.e. it contains a database
 * of named snapshots or tags. Internally it uses the HistoryDatabase class
 * to store those tags in an SQLite file.
 *
 * Each tag contains meta information (i.e. description, date) and points to
 * one specific catalog revision (revision, root catalog hash). Furthermore
 * tags are associated with _one_ update channel. This can be used in clients
 * to selectively apply file system snapshots of a specific update channel.
 */
class History {
 public:
  /**
   * Available update channels
   *   o Trunk (the default)
   *   o Development
   *   o Testing
   *   o Production
   */
  enum UpdateChannel {
    kChannelTrunk = 0,
    kChannelDevel = 4,
    kChannelTest = 16,
    kChannelProd = 64,
  };

  /**
   * The Tag structure contains information about one specific named snap-
   * shot stored in the history database. Tags can be retrieved from this
   * history class both by 'name' and by 'date'. Naturally, tags can also
   * be saved into the History using this struct as a container.
   */
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

  /**
   * Opens an available history database file in read-only mode and returns
   * a pointer to a History object wrapping this database.
   * Note: The caller is assumed to retain ownership of the pointer and the
   *       history database is closed on deletion of the History object.
   *
   * @param file_name  the path to the history SQLite file to be opened
   * @return           pointer to History object or NULL on error
   */
  static History* Open(const std::string &file_name);

  /**
   * Same as History::Open(), but opens the history database file in read/write
   * mode. This allows to use the modifying methods of the History object.
   *
   * @param file_name  the path to the history SQLite file to be opened
   * @return           pointer to History object or NULL on error
   */
  static History* OpenWritable(const std::string &file_name);

  /**
   * Creates an empty History database. Since a History object is always
   * associated to a specific repository, one needs to specify the fully
   * qualified repository name (FQRN) on creation of the History database.
   * Note: pointer ownership is assumed to be retained by the caller.
   *
   * @param file_name  the path of the new history file.
   * @param fqrn       the FQRN of the repository containing this History
   * @return           pointer to empty History object or NULL on error
   */
  static History* Create(const std::string &file_name, const std::string &fqrn);

  bool IsWritable() const;
  int GetNumberOfTags() const;

  /**
   * Opens a new database transaction in the underlying SQLite database
   * This can greatly improve performance when used before inserting or
   * removing multiple tags.
   */
  bool BeginTransaction()  const;

  /**
   * Closes a transaction (see BeginTransaction())
   */
  bool CommitTransaction() const;

  /**
   * Sets the internal pointer to the previous revision of this History file.
   * Note: This must be handled by the user code.
   *
   * @param history_hash  the content hash of the previous revision
   */
  bool SetPreviousRevision(const shash::Any &history_hash);

  bool Insert(const Tag &tag);
  bool Remove(const std::string &name);
  bool Exists(const std::string &name) const;
  bool GetByName(const std::string &name, Tag *tag) const;
  bool GetByDate(const time_t timestamp, Tag *tag) const;
  bool List(std::vector<Tag> *tags) const;
  bool Tips(std::vector<Tag> *channel_tips) const;

  /**
   * Rolls back the history to the provided target tag and deletes all tags
   * of the containing channel in between.
   *
   * Note: this assumes that the provided target tag was already updated with
   *       the republished root catalog information.
   *
   * @param updated_target_tag  the tag to be rolled back to (updated: see Note)
   * @return                    true on success
   */
  bool Rollback(const Tag &updated_target_tag);

  /**
   * Provides a list of all referenced catalog hashes in this History.
   * The hashes will be ordered by their associated revision number in
   * acending order.
   *
   * @param hashes  pointer to the result vector to be filled
   */
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
  UniquePtr<SqlRollbackTag>       rollback_tag_;
};

}  // namespace hsitory

#endif  // CVMFS_HISTORY_H_
