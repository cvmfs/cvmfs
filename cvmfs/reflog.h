/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_REFLOG_H_
#define CVMFS_REFLOG_H_

#include <string>

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

 public:
  bool AddCertificate(const shash::Any &certificate);
  bool AddCatalog(const shash::Any &catalog);
  bool AddHistory(const shash::Any &history);
  bool AddMetainfo(const shash::Any &metainfo);

  uint64_t CountEntries();

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

 private:
  bool CreateDatabase(const std::string &database_path,
                      const std::string &repo_name);
  bool OpenDatabase(const std::string &database_path);

  void PrepareQueries();

 private:
  UniquePtr<ReflogDatabase>      database_;

  UniquePtr<SqlInsertReference>  insert_reference_;
  UniquePtr<SqlCountReferences>  count_references_;
};

} // namespace manifest

#endif /* CVMFS_REFLOG_H_ */
