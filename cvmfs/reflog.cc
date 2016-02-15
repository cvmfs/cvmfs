/**
 * This file is part of the CernVM File System.
 */

#include "reflog.h"

using namespace manifest;

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
  insert_reference_ = new SqlInsertReference(database_.weak_ref());
  count_references_ = new SqlCountReferences(database_.weak_ref());
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


bool Reflog::AddReference(const shash::Any               &hash,
                          const SqlReflog::ReferenceType  type) {
  return
    insert_reference_->BindReference(hash, type) &&
    insert_reference_->Execute()                 &&
    insert_reference_->Reset();
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


std::string Reflog::fqrn() const {
  assert(database_);
  return database_->GetProperty<std::string>(ReflogDatabase::kFqrnKey);
}


std::string Reflog::database_file() const {
  assert(database_);
  return database_->filename();
}
