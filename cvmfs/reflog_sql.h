/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_REFLOG_SQL_H_
#define CVMFS_REFLOG_SQL_H_

#include <string>

#include "crypto/hash.h"
#include "sql.h"

class ReflogDatabase : public sqlite::Database<ReflogDatabase> {
 public:
  static const float kLatestSchema;
  static const float kLatestSupportedSchema;
  // backwards-compatible schema changes
  static const unsigned kLatestSchemaRevision;

  static const std::string kFqrnKey;

  bool CreateEmptyDatabase();

  bool CheckSchemaCompatibility();
  bool LiveSchemaUpgradeIfNecessary();
  bool CompactDatabase() const {
    return true;
  }  // no implementation specific
     // database compaction.

  bool InsertInitialValues(const std::string &repo_name);

 protected:
  // TODO(rmeusel): C++11 - constructor inheritance
  friend class sqlite::Database<ReflogDatabase>;
  ReflogDatabase(const std::string &filename, const OpenMode open_mode)
      : sqlite::Database<ReflogDatabase>(filename, open_mode) { }
};


//------------------------------------------------------------------------------


class SqlReflog : public sqlite::Sql {
 public:
  enum ReferenceType {
    kRefCatalog,
    kRefCertificate,
    kRefHistory,
    kRefMetainfo
  };

  static shash::Suffix ToSuffix(const ReferenceType type);
};


class SqlInsertReference : public SqlReflog {
 public:
  explicit SqlInsertReference(const ReflogDatabase *database);
  bool BindReference(const shash::Any &reference_hash,
                     const ReferenceType type);
};


class SqlCountReferences : public SqlReflog {
 public:
  explicit SqlCountReferences(const ReflogDatabase *database);
  uint64_t RetrieveCount();
};


class SqlListReferences : public SqlReflog {
 public:
  explicit SqlListReferences(const ReflogDatabase *database);
  bool BindType(const ReferenceType type);
  bool BindOlderThan(const uint64_t timestamp);
  shash::Any RetrieveHash() const;
};


class SqlRemoveReference : public SqlReflog {
 public:
  explicit SqlRemoveReference(const ReflogDatabase *database);
  bool BindReference(const shash::Any &reference_hash,
                     const ReferenceType type);
};


class SqlContainsReference : public SqlReflog {
 public:
  explicit SqlContainsReference(const ReflogDatabase *database);
  bool BindReference(const shash::Any &reference_hash,
                     const ReferenceType type);
  bool RetrieveAnswer();
};


class SqlGetTimestamp : public SqlReflog {
 public:
  explicit SqlGetTimestamp(const ReflogDatabase *database);
  bool BindReference(const shash::Any &reference_hash,
                     const ReferenceType type);
  uint64_t RetrieveTimestamp();
};

#endif  // CVMFS_REFLOG_SQL_H_
