/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_HISTORY_SQL_H_
#define CVMFS_HISTORY_SQL_H_

#include <string>

#include "history.h"
#include "sql.h"

namespace history {

/**
 * This class wraps the database structure of the History SQLite database files.
 * For that it inherits from sqlite::Database<>, please look there for further
 * details.
 */
class HistoryDatabase : public sqlite::Database<HistoryDatabase> {
 public:
  static const float kLatestSchema;
  static const float kLatestSupportedSchema;
  // backwards-compatible schema changes
  static const unsigned kLatestSchemaRevision;

  static const std::string kFqrnKey;

  bool CreateEmptyDatabase();
  bool InsertInitialValues(const std::string &repository_name);

  bool ContainsRecycleBin() const;

  bool CheckSchemaCompatibility();
  bool LiveSchemaUpgradeIfNecessary();
  bool CompactDatabase() const { return true; }  // no implementation specific
                                                 // database compaction.
 protected:
  // TODO(rmeusel): C++11 - constructor inheritance
  friend class sqlite::Database<HistoryDatabase>;
  HistoryDatabase(const std::string  &filename,
                  const OpenMode      open_mode) :
    sqlite::Database<HistoryDatabase>(filename, open_mode) {}

 private:
  bool CreateTagsTable();
  bool CreateRecycleBinTable();

  bool UpgradeSchemaRevision_10_1();
  bool UpgradeSchemaRevision_10_2();
};


//------------------------------------------------------------------------------


class SqlHistory : public sqlite::Sql {};


/**
 * A mixin that allows to inject the RetrieveTag() method if it is needed in an
 * SQL query subclass.
 *
 * This allows us to orchestrate specific functionality in those classes as we
 * need it, without relying on multiple inheritance. Otherwise one would inherit
 * from sqlite::Sql more than once when inheriting from more than one of those
 * mixin classes in a query subclass.
 * In contrast, the mixins produce a clear (single) inheritance graph for each
 * of the derived classes by chaining the functionality as seen below.
 *
 * Example:
 * class SqlListRollbackTags : public SqlRetrieveTag<SqlRollback<SqlHistory>> {}
 *
 *    ################     Note: both SqlRetrieveTag and SqlRollback need the
 *    # sqlite::Sql  #           functionality provided in sqlite::Sql, thus
 *    ################           would otherwise be derived from it individually
 *          ´|`
 *           |
 *    ################      ####################      #################
 *    #  SqlHistory  # <--- #  SqlRetrieveTag  # <--- #  SqlRollback  #
 *    ################      ####################      #################
 *                                                           ´|`
 *                                                            |
 *                                                #########################
 *                                                #  SqlListRollbackTags  #
 *                                                #########################
 *
 * Note: MixinT needs to be eventually derived from sqlite::Sql as it uses
 *       Sql::Retrieve...() methods to extract information from SQLite rows.
 *
 * @param MixinT  the class that should gain RetrieveTag()'s functionality
 */
template <class MixinT>
class SqlRetrieveTag : public MixinT {
 public:
  /**
   * Retrieves a tag from a database row
   * See SqlHistory::db_fields for a listing of the used SQLite data fields
   *
   * @return  a tag retrieved from the SQLite database row
   */
  History::Tag RetrieveTag() const {
    History::Tag result;
    result.name        = MixinT::RetrieveString(0);
    result.root_hash   = shash::MkFromHexPtr(
                           shash::HexPtr(MixinT::RetrieveString(1)),
                           shash::kSuffixCatalog);
    result.revision    = MixinT::RetrieveInt64(2);
    result.timestamp   = MixinT::RetrieveInt64(3);
    result.channel     = static_cast<History::UpdateChannel>(
                           MixinT::RetrieveInt64(4));
    result.description = MixinT::RetrieveString(5);
    result.size        = MixinT::RetrieveInt64(6);
    return result;
  }
};


class SqlInsertTag : public SqlHistory {
 private:
  static const std::string db_placeholders;

 public:
  explicit SqlInsertTag(const HistoryDatabase *database);
  bool BindTag(const History::Tag &tag);
};


class SqlRemoveTag : public SqlHistory {
 public:
  explicit SqlRemoveTag(const HistoryDatabase *database);
  bool BindName(const std::string &name);
};


class SqlFindTag : public SqlRetrieveTag<SqlHistory> {
 public:
  explicit SqlFindTag(const HistoryDatabase *database);
  bool BindName(const std::string &name);
};


class SqlFindTagByDate : public SqlRetrieveTag<SqlHistory> {
 public:
  explicit SqlFindTagByDate(const HistoryDatabase *database);
  bool BindTimestamp(const time_t timestamp);
};


class SqlCountTags : public SqlHistory {
 public:
  explicit SqlCountTags(const HistoryDatabase *database);
  unsigned RetrieveCount() const;
};


class SqlListTags : public SqlRetrieveTag<SqlHistory> {
 public:
  explicit SqlListTags(const HistoryDatabase *database);
};


class SqlGetChannelTips : public SqlRetrieveTag<SqlHistory> {
 public:
  explicit SqlGetChannelTips(const HistoryDatabase *database);
};


class SqlGetHashes : public SqlHistory {
 public:
  explicit SqlGetHashes(const HistoryDatabase *database);
  shash::Any RetrieveHash() const;
};


/**
 * Mixin to inject the rollback condition definition and the BindTargetTag()
 * method into other subclasses.
 *
 * Note: See documentation of the SqlRetrieveTag<> for further details.
 *
 * @param MixinT  the class that should gain BindTargetTags()'s functionality
 * @param offset  offset for SQLite placeholders, if used inside other complex
 *                SQL queries with preceeding placeholders
 */
template <class MixinT, int offset = 0>
class SqlRollback : public MixinT {
 protected:
  static const std::string rollback_condition;

 public:
  bool BindTargetTag(const History::Tag &target_tag) {
    return MixinT::BindInt64(offset + 1, target_tag.revision) &&
           MixinT::BindText(offset + 2, target_tag.name) &&
           MixinT::BindInt64(offset + 3, target_tag.channel);
  }
};

template <class MixinT, int offset>
const std::string SqlRollback<MixinT, offset>::rollback_condition =
                                             "(revision > :target_rev  OR "
                                             " name     = :target_name)   "
                                             "AND channel  = :target_chan ";


class SqlRollbackTag : public SqlRollback<SqlHistory> {
 public:
  explicit SqlRollbackTag(const HistoryDatabase *database);
};


class SqlListRollbackTags : public SqlRetrieveTag<SqlRollback<SqlHistory> > {
 public:
  explicit SqlListRollbackTags(const HistoryDatabase *database);
};


class SqlRecycleBin : public SqlHistory {
 protected:
  static const unsigned int kFlagCatalog = 1;

  bool CheckSchema(const HistoryDatabase *database) const;
};


class SqlRecycleBinInsert : public SqlRecycleBin {
 public:
  explicit SqlRecycleBinInsert(const HistoryDatabase *database);
  bool BindTag(const History::Tag &condemned_tag);
};


class SqlRecycleBinList : public SqlRecycleBin {
 public:
  explicit SqlRecycleBinList(const HistoryDatabase *database);
  shash::Any RetrieveHash();
};


class SqlRecycleBinFlush : public SqlRecycleBin {
 public:
  explicit SqlRecycleBinFlush(const HistoryDatabase *database);
};


/**
 * Shadows all hashes that are going to be deleted by the history rollback into
 * the recycle bin.
 * See: SqlRollback::BindTargetTag()
 */
class SqlRecycleBinRollback : public SqlRollback<SqlRecycleBin, 1> {
 public:
  explicit SqlRecycleBinRollback(const HistoryDatabase *database);
  bool BindFlags();
};

}  // namespace history

#endif  // CVMFS_HISTORY_SQL_H_
