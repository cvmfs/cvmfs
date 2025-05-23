/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_REFLOG_H_
#define CVMFS_REFLOG_H_

#include <string>
#include <vector>

#include "reflog_sql.h"

namespace manifest {


/**
 * This is an object reference log meant to keep track of all "root" objects in
 * the backend storage it is situated in. It's main purpose is to keep track of
 * historic objects for efficient and robust garbage collection.
 *
 * "Root objects" in this sense are backend objects containing references to
 * other backend objects but that are not necessarily referenced by any other
 * object in the backend. For example historic root catalogs, history databases,
 * meta-info objects or certificates.
 *
 * Every time such an object is newly added to a backend storage of CernVM-FS
 * its content hash (and object type) will be added to the Reflog. This ensures
 * an authoritative list of "root objects" in a given backend storage.
 *
 * Reflogs are associated to a specific backend storage of Stratum0 or Stratum1
 * and _not_ to a CernVM-FS repository. Thus, the Reflogs of Stratum0 and its
 * replicas can and probably will be different and are _not_ interchangeable.
 *
 * TODO(rmeusel): this shares a lot of database management code with
 *                SqliteHistory and (potentially) ...Catalog. This might be an
 *                architectural weakness and should be cleaned up.
 */
class Reflog {
 public:
  static Reflog *Open(const std::string &database_path);
  static Reflog *Create(const std::string &database_path,
                        const std::string &repo_name);
  static void HashDatabase(const std::string &database_path,
                           shash::Any *hash_reflog);

  static bool ReadChecksum(const std::string &path, shash::Any *checksum);
  static bool WriteChecksum(const std::string &path, const shash::Any &value);

  bool AddCertificate(const shash::Any &certificate);
  bool AddCatalog(const shash::Any &catalog);
  bool AddHistory(const shash::Any &history);
  bool AddMetainfo(const shash::Any &metainfo);

  uint64_t CountEntries();
  bool List(SqlReflog::ReferenceType type,
            std::vector<shash::Any> *hashes) const;
  bool ListOlderThan(SqlReflog::ReferenceType type,
                     uint64_t timestamp,
                     std::vector<shash::Any> *hashes) const;

  bool Remove(const shash::Any &hash);

  bool ContainsCertificate(const shash::Any &certificate) const;
  bool ContainsCatalog(const shash::Any &catalog) const;
  bool ContainsHistory(const shash::Any &history) const;
  bool ContainsMetainfo(const shash::Any &metainfo) const;

  bool GetCatalogTimestamp(const shash::Any &catalog,
                           uint64_t *timestamp) const;

  void BeginTransaction();
  void CommitTransaction();

  void TakeDatabaseFileOwnership();
  void DropDatabaseFileOwnership();
  bool OwnsDatabaseFile() const {
    return database_.IsValid() && database_->OwnsFile();
  }
  bool Vacuum() { return database_->Vacuum(); }

  std::string fqrn() const;
  std::string database_file() const;

 protected:
  bool AddReference(const shash::Any &hash,
                    const SqlReflog::ReferenceType type);
  bool ContainsReference(const shash::Any &hash,
                         const SqlReflog::ReferenceType type) const;
  bool GetReferenceTimestamp(const shash::Any &hash,
                             const SqlReflog::ReferenceType type,
                             uint64_t *timestamp) const;

 private:
  bool CreateDatabase(const std::string &database_path,
                      const std::string &repo_name);
  bool OpenDatabase(const std::string &database_path);

  void PrepareQueries();

 private:
  UniquePtr<ReflogDatabase> database_;

  UniquePtr<SqlInsertReference> insert_reference_;
  UniquePtr<SqlCountReferences> count_references_;
  UniquePtr<SqlListReferences> list_references_;
  UniquePtr<SqlRemoveReference> remove_reference_;
  UniquePtr<SqlContainsReference> contains_reference_;
  UniquePtr<SqlGetTimestamp> get_timestamp_;
};

}  // namespace manifest

#endif  // CVMFS_REFLOG_H_
