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

  // PrepareQueries();
  return true;
}


bool Reflog::OpenDatabase(const std::string &database_path) {
  assert(!database_);

  ReflogDatabase::OpenMode mode = ReflogDatabase::kOpenReadWrite;
  database_ = ReflogDatabase::Open(database_path, mode);
  if (!database_.IsValid()) {
    return false;
  }

  // PrepareQueries();
  return true;
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
