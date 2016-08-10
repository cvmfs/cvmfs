/**
 * This file is part of the CernVM File System.
 */

#include "reflog.h"

#include <fcntl.h>
#include <unistd.h>

#include <cassert>

#include "util/posix.h"

namespace manifest {

Reflog* Reflog::Open(const std::string &database_path) {
  Reflog *reflog = new Reflog();
  if (NULL == reflog || !reflog->OpenDatabase(database_path)) {
    delete reflog;
    return NULL;
  }

  LogCvmfs(kLogReflog, kLogDebug,
           "opened Reflog database '%s' for repository '%s'",
           database_path.c_str(), reflog->fqrn().c_str());

  return reflog;
}


Reflog* Reflog::Create(const std::string &database_path,
                       const std::string &repo_name) {
  Reflog *reflog = new Reflog();
  if (NULL == reflog || !reflog->CreateDatabase(database_path, repo_name)) {
    delete reflog;
    return NULL;
  }

  LogCvmfs(kLogReflog, kLogDebug, "created empty reflog database '%s' for "
                                  "repository '%s'",
           database_path.c_str(), repo_name.c_str());
  return reflog;
}


shash::Any Reflog::ReadChecksum(const std::string &path) {
  int fd = open(path.c_str(), O_RDONLY);
  assert(fd >= 0);
  std::string hex_hash;
  bool retval = SafeReadToString(fd, &hex_hash);
  assert(retval);
  close(fd);
  return shash::MkFromHexPtr(shash::HexPtr(hex_hash));
}


void Reflog::WriteChecksum(const std::string &path, const shash::Any &value) {
  int fd = open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, kDefaultFileMode);
  assert(fd >= 0);
  std::string hex_hash = value.ToString();
  bool retval = SafeWrite(fd, hex_hash.data(), hex_hash.length());
  assert(retval);
  close(fd);
}


bool Reflog::CreateDatabase(const std::string &database_path,
                            const std::string &repo_name) {
  assert(!database_);
  database_ = ReflogDatabase::Create(database_path);
  if (!database_ || !database_->InsertInitialValues(repo_name)) {
    LogCvmfs(kLogReflog, kLogDebug,
             "failed to initialize empty database '%s'",
             database_path.c_str());
    return false;
  }

  PrepareQueries();
  return true;
}


bool Reflog::OpenDatabase(const std::string &database_path) {
  assert(!database_);

  ReflogDatabase::OpenMode mode = ReflogDatabase::kOpenReadWrite;
  database_ = ReflogDatabase::Open(database_path, mode);
  if (!database_.IsValid()) {
    return false;
  }

  PrepareQueries();
  return true;
}


void Reflog::PrepareQueries() {
  assert(database_);
  insert_reference_   = new SqlInsertReference(database_.weak_ref());
  count_references_   = new SqlCountReferences(database_.weak_ref());
  list_references_    = new SqlListReferences(database_.weak_ref());
  remove_reference_   = new SqlRemoveReference(database_.weak_ref());
  contains_reference_ = new SqlContainsReference(database_.weak_ref());
}


bool Reflog::AddCertificate(const shash::Any &certificate) {
  assert(certificate.HasSuffix() &&
         certificate.suffix == shash::kSuffixCertificate);
  return AddReference(certificate, SqlReflog::kRefCertificate);
}


bool Reflog::AddCatalog(const shash::Any &catalog) {
  assert(catalog.HasSuffix() && catalog.suffix == shash::kSuffixCatalog);
  return AddReference(catalog, SqlReflog::kRefCatalog);
}


bool Reflog::AddHistory(const shash::Any &history) {
  assert(history.HasSuffix() && history.suffix == shash::kSuffixHistory);
  return AddReference(history, SqlReflog::kRefHistory);
}


bool Reflog::AddMetainfo(const shash::Any &metainfo) {
  assert(metainfo.HasSuffix() && metainfo.suffix == shash::kSuffixMetainfo);
  return AddReference(metainfo, SqlReflog::kRefMetainfo);
}


uint64_t Reflog::CountEntries() {
  assert(database_);
  const bool success_exec = count_references_->Execute();
  assert(success_exec);
  const uint64_t count = count_references_->RetrieveCount();
  const bool success_reset = count_references_->Reset();
  assert(success_reset);
  return count;
}


bool Reflog::ListCatalogs(std::vector<shash::Any> *hashes) const {
  assert(database_);
  assert(NULL != hashes);

  hashes->clear();

  const bool success_bind = list_references_->BindType(SqlReflog::kRefCatalog);
  assert(success_bind);
  while (list_references_->FetchRow()) {
    hashes->push_back(list_references_->RetrieveHash());
  }

  return list_references_->Reset();
}


bool Reflog::RemoveCatalog(const shash::Any &hash) {
  assert(database_);

  return
    remove_reference_->BindReference(hash, SqlReflog::kRefCatalog) &&
    remove_reference_->Execute()                                   &&
    remove_reference_->Reset();
}


bool Reflog::ContainsCertificate(const shash::Any &certificate) const {
  assert(certificate.HasSuffix() &&
         certificate.suffix == shash::kSuffixCertificate);
  return ContainsReference(certificate, SqlReflog::kRefCertificate);
}


bool Reflog::ContainsCatalog(const shash::Any &catalog) const {
  assert(catalog.HasSuffix() && catalog.suffix == shash::kSuffixCatalog);
  return ContainsReference(catalog, SqlReflog::kRefCatalog);
}


bool Reflog::ContainsHistory(const shash::Any &history) const {
  assert(history.HasSuffix() && history.suffix == shash::kSuffixHistory);
  return ContainsReference(history, SqlReflog::kRefHistory);
}


bool Reflog::ContainsMetainfo(const shash::Any &metainfo) const {
  assert(metainfo.HasSuffix() && metainfo.suffix == shash::kSuffixMetainfo);
  return ContainsReference(metainfo, SqlReflog::kRefMetainfo);
}


bool Reflog::AddReference(const shash::Any               &hash,
                          const SqlReflog::ReferenceType  type) {
  return
    insert_reference_->BindReference(hash, type) &&
    insert_reference_->Execute()                 &&
    insert_reference_->Reset();
}


bool Reflog::ContainsReference(const shash::Any               &hash,
                               const SqlReflog::ReferenceType  type) const {
  const bool fetching =
    contains_reference_->BindReference(hash, type) &&
    contains_reference_->FetchRow();
  assert(fetching);

  const bool answer = contains_reference_->RetrieveAnswer();
  const bool reset = contains_reference_->Reset();
  assert(reset);

  return answer;
}


void Reflog::BeginTransaction() {
  assert(database_);
  database_->BeginTransaction();
}


void Reflog::CommitTransaction() {
  assert(database_);
  database_->CommitTransaction();
}


void Reflog::TakeDatabaseFileOwnership() {
  assert(database_);
  database_->TakeFileOwnership();
}


void Reflog::DropDatabaseFileOwnership() {
  assert(database_);
  database_->DropFileOwnership();
}


/**
 * Use only once the database was closed.
 */
void Reflog::HashDatabase(
  const std::string &database_path,
  shash::Any *hash_reflog)
{
  bool retval = HashFile(database_path, hash_reflog);
  assert(retval);
}


std::string Reflog::fqrn() const {
  assert(database_);
  return database_->GetProperty<std::string>(ReflogDatabase::kFqrnKey);
}


std::string Reflog::database_file() const {
  assert(database_);
  return database_->filename();
}

}  // namespace manifest
