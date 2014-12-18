/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_HISTORY_SQLITE_H_
#define CVMFS_HISTORY_SQLITE_H_

#include <stdint.h>
#include <time.h>

#include <string>
#include <vector>

#include "hash.h"
#include "util.h"
#include "history_sql.h"
#include "history.h"

namespace history {

/**
 * This class wraps the history of a repository, i.e. it contains a database
 * of named snapshots or tags. Internally it uses the HistoryDatabase class
 * to store those tags in an SQLite file.
 */
class SqliteHistory : public History {

 protected:
  static const std::string kPreviousRevisionKey;

 public:
  virtual ~SqliteHistory() {};

  /**
   * Opens an available history database file in read-only mode and returns
   * a pointer to a History object wrapping this database.
   * Note: The caller is assumed to retain ownership of the pointer and the
   *       history database is closed on deletion of the History object.
   *
   * @param file_name  the path to the history SQLite file to be opened
   * @return           pointer to History object or NULL on error
   */
  static SqliteHistory* Open(const std::string &file_name);

  /**
   * Same as SqliteHistory::Open(), but opens the history database file in
   * read/write mode. This allows to use the modifying methods of the History
   * object.
   *
   * @param file_name  the path to the history SQLite file to be opened
   * @return           pointer to History object or NULL on error
   */
  static SqliteHistory* OpenWritable(const std::string &file_name);

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
  static SqliteHistory* Create(const std::string &file_name,
                               const std::string &fqrn);

  bool IsWritable() const;
  unsigned GetNumberOfTags() const;

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
  shash::Any previous_revision() const;

  bool Insert(const Tag &tag);
  bool Remove(const std::string &name);
  bool Exists(const std::string &name) const;
  bool GetByName(const std::string &name, Tag *tag) const;
  bool GetByDate(const time_t timestamp, Tag *tag) const;
  bool List(std::vector<Tag> *tags) const;
  bool Tips(std::vector<Tag> *channel_tips) const;

  bool ListRecycleBin(std::vector<shash::Any> *hashes) const;
  bool EmptyRecycleBin();

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
   * Lists the tags that would be deleted by a rollback to the tag specified.
   *
   * Note: This doesn't change the database but is mainly used for sanity checks
   *       and user output.
   *
   * @param target_tag_name  the tag name for the planned rollback
   * @param tags             pointer to the result tag list to be filled
   * @return                 true on success
   */
  bool ListTagsAffectedByRollback(const std::string  &target_tag_name,
                                  std::vector<Tag>   *tags) const;

  /**
   * Provides a list of all referenced catalog hashes in this History.
   * The hashes will be ordered by their associated revision number in
   * acending order.
   *
   * @param hashes  pointer to the result vector to be filled
   */
  bool GetHashes(std::vector<shash::Any> *hashes) const;

 protected:
  static SqliteHistory* Open(const std::string &file_name,
                             const bool read_write);
  bool OpenDatabase(const std::string &file_name, const bool read_write);
  bool CreateDatabase(const std::string &file_name, const std::string &fqrn);
  void PrepareQueries();

 private:
  template <class SqlListingT>
  bool RunListing(std::vector<Tag> *list, SqlListingT *sql) const;

  bool KeepHashReference(const Tag &tag);

 private:
  UniquePtr<HistoryDatabase>        database_;

  UniquePtr<SqlInsertTag>           insert_tag_;
  UniquePtr<SqlRemoveTag>           remove_tag_;
  UniquePtr<SqlFindTag>             find_tag_;
  UniquePtr<SqlFindTagByDate>       find_tag_by_date_;
  UniquePtr<SqlCountTags>           count_tags_;
  UniquePtr<SqlListTags>            list_tags_;
  UniquePtr<SqlGetChannelTips>      channel_tips_;
  UniquePtr<SqlGetHashes>           get_hashes_;
  UniquePtr<SqlRollbackTag>         rollback_tag_;
  UniquePtr<SqlListRollbackTags>    list_rollback_tags_;
  UniquePtr<SqlRecycleBinInsert>    recycle_insert_;
  UniquePtr<SqlRecycleBinList>      recycle_list_;
  UniquePtr<SqlRecycleBinFlush>     recycle_empty_;
  UniquePtr<SqlRecycleBinRollback>  recycle_rollback_;
};

} /* namespace history */

#endif
