/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_REFLOG_H_
#define CVMFS_REFLOG_H_

#include <string>
#include <vector>

#include "reflog_sql.h"

namespace manifest {

// TODO(rmeusel): this shares a lot of database management code with
//                SqliteHistory and (potentially) ...Catalog. This might be an
//                architectural weakness and should be cleaned up.
class Reflog {
 public:
  static Reflog* Open(const std::string &database_path);
  static Reflog* Create(const std::string &database_path,
                        const std::string &repo_name);

  std::string CloseAndReturnDatabaseFile() {
    return database_->CloseAndReturnDatabaseFile();
  }

  bool AddCertificate(const shash::Any &certificate);
  bool AddCatalog(const shash::Any &catalog);
  bool AddHistory(const shash::Any &history);
  bool AddMetainfo(const shash::Any &metainfo);

  uint64_t CountEntries();
  bool ListCatalogs(std::vector<shash::Any> *hashes) const;

  bool RemoveCatalog(const shash::Any &hash);

  bool ContainsCertificate(const shash::Any &certificate) const;
  bool ContainsCatalog(const shash::Any &catalog) const;
  bool ContainsHistory(const shash::Any &history) const;
  bool ContainsMetainfo(const shash::Any &metainfo) const;

  void BeginTransaction();
  void CommitTransaction();

  void TakeDatabaseFileOwnership();
  void DropDatabaseFileOwnership();
  bool OwnsDatabaseFile() const {
    return database_.IsValid() && database_->OwnsFile();
  }

  std::string fqrn() const;
  std::string database_file() const;

 protected:
  bool AddReference(const shash::Any               &hash,
                    const SqlReflog::ReferenceType  type);
  bool ContainsReference(const shash::Any               &hash,
                         const SqlReflog::ReferenceType  type) const;

 private:
  bool CreateDatabase(const std::string &database_path,
                      const std::string &repo_name);
  bool OpenDatabase(const std::string &database_path);

  void PrepareQueries();

 private:
  UniquePtr<ReflogDatabase>       database_;

  UniquePtr<SqlInsertReference>   insert_reference_;
  UniquePtr<SqlCountReferences>   count_references_;
  UniquePtr<SqlListReferences>    list_references_;
  UniquePtr<SqlRemoveReference>   remove_reference_;
  UniquePtr<SqlContainsReference> contains_reference_;
};

}  // namespace manifest

#endif  // CVMFS_REFLOG_H_
