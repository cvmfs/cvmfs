/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_HISTORY_H_
#define CVMFS_HISTORY_H_

#include <stdint.h>
#include <time.h>

#include <string>
#include <vector>

#include "hash.h"

namespace history {

/**
 * This is the abstract base class for repository history. It maintains a list
 * of named snapshots in a Tag structure.
 *
 * Each tag contains meta information (i.e. description, date) and points to
 * one specific catalog revision (revision, root catalog hash). Furthermore
 * tags are associated with _one_ update channel. This can be used in clients
 * to selectively apply file system snapshots of a specific update channel.
 *
 * Note: The public interface of the History class is virtual, in order to over-
 *       write it in a testing environment. As we are dealing with an SQLite
 *       database anyway, the overhead of this should not matter.
 *       It could be implemented using CRTP if necessary, but would require com-
 *       plex code to do so.
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

    bool operator >(const Tag &other) const {
      return this->revision > other.revision;
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
  virtual ~History() {};

  virtual bool IsWritable() const     = 0;
  virtual unsigned GetNumberOfTags() const = 0;

  /**
   * Opens a new database transaction in the underlying SQLite database
   * This can greatly improve performance when used before inserting or
   * removing multiple tags.
   */
  virtual bool BeginTransaction()  const = 0;

  /**
   * Closes a transaction (see BeginTransaction())
   */
  virtual bool CommitTransaction() const = 0;

  /**
   * Sets the internal pointer to the previous revision of this History file.
   * Note: This must be handled by the user code.
   *
   * @param history_hash  the content hash of the previous revision
   */
  virtual bool SetPreviousRevision(const shash::Any &history_hash)   = 0;
  virtual shash::Any previous_revision() const                       = 0;

  virtual bool Insert(const Tag &tag)                                = 0;
  virtual bool Remove(const std::string &name)                       = 0;
  virtual bool Exists(const std::string &name) const                 = 0;
  virtual bool GetByName(const std::string &name, Tag *tag) const    = 0;
  virtual bool GetByDate(const time_t timestamp, Tag *tag) const     = 0;
  virtual bool List(std::vector<Tag> *tags) const                    = 0;
  virtual bool Tips(std::vector<Tag> *channel_tips) const            = 0;

  virtual bool ListRecycleBin(std::vector<shash::Any> *hashes) const = 0;
  virtual bool EmptyRecycleBin()                                     = 0;

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
  virtual bool Rollback(const Tag &updated_target_tag) = 0;

  /**
   * Lists the tags that would be deleted by a rollback to the tag specified.
   *
   * Note: This doesn't change the database but is mainly used for sanity checks
   *       and user output.
   *
   * @param target_tag_name  the tag name for the planned rollback
   * @param tags             pointer to the result tag list to be filled
   * @return                 true on success
   */
  virtual bool ListTagsAffectedByRollback(const std::string  &target_tag_name,
                                          std::vector<Tag>   *tags) const = 0;

  /**
   * Provides a list of all referenced catalog hashes in this History.
   * The hashes will be ordered by their associated revision number in
   * acending order.
   *
   * @param hashes  pointer to the result vector to be filled
   */
  virtual bool GetHashes(std::vector<shash::Any> *hashes) const = 0;

  const std::string& fqrn() const { return fqrn_; }

 protected:
  void set_fqrn(const std::string &fqrn) { fqrn_ = fqrn; }

 private:
  std::string   fqrn_;
};

}  // namespace hsitory

#endif  // CVMFS_HISTORY_H_
