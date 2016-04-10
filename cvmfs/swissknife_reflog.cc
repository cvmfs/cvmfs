/**
 * This file is part of the CernVM File System.
 */

#include "swissknife_reflog.h"

#include <cassert>

#include <string>
#include <vector>

#include "manifest.h"
#include "object_fetcher.h"
#include "upload_facility.h"


namespace swissknife {

typedef HttpObjectFetcher<> ObjectFetcher;

class RootChainWalker {
 public:
  typedef ObjectFetcher::CatalogTN CatalogTN;
  typedef ObjectFetcher::HistoryTN HistoryTN;

 public:
  RootChainWalker(const manifest::Manifest *manifest,
                  ObjectFetcher            *object_fetcher,
                  manifest::Reflog         *reflog)
    : object_fetcher_(object_fetcher)
    , reflog_(reflog)
    , manifest_(manifest) {}

  void FindObjectsAndPopulateReflog();

 protected:
  typedef std::vector<shash::Any> CatalogList;

 protected:
  CatalogTN* FetchCatalog(const shash::Any catalog_hash);
  HistoryTN* FetchHistory(const shash::Any history_hash);

  void WalkRootCatalogs(const shash::Any &root_catalog_hash);
  void WalkHistories(const shash::Any &history_hash);

  void WalkCatalogsInHistory(const HistoryTN *history);
  void WalkListedCatalogs(const CatalogList &catalog_list);

  template <class DatabaseT>
  DatabaseT* ReturnOrAbort(const ObjectFetcherFailures::Failures  failure,
                           const shash::Any                      &content_hash,
                           DatabaseT                             *database);

 private:
  ObjectFetcher            *object_fetcher_;
  manifest::Reflog         *reflog_;
  const manifest::Manifest *manifest_;
};


ParameterList CommandReconstructReflog::GetParams() {
  ParameterList r;
  r.push_back(Parameter::Mandatory('r', "repository url"));
  r.push_back(Parameter::Mandatory('u', "spooler definition string"));
  r.push_back(Parameter::Mandatory('n', "fully qualified repository name"));
  r.push_back(Parameter::Mandatory('t', "temporary directory"));
  r.push_back(Parameter::Mandatory('k', "repository keychain"));
  return r;
}


int CommandReconstructReflog::Main(const ArgumentList &args) {
  const std::string &repo_url  = *args.find('r')->second;
  const std::string &spooler   = *args.find('u')->second;
  const std::string &repo_name = *args.find('n')->second;
  const std::string &tmp_dir   = *args.find('t')->second;
  const std::string &repo_keys = *args.find('k')->second;

  const bool follow_redirects = false;
  if (!this->InitDownloadManager(follow_redirects) ||
      !this->InitVerifyingSignatureManager(repo_keys)) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to init repo connection");
    return 1;
  }

  ObjectFetcher object_fetcher(repo_name,
                               repo_url,
                               tmp_dir,
                               download_manager(),
                               signature_manager());

  UniquePtr<manifest::Manifest> manifest;
  ObjectFetcher::Failures retval = object_fetcher.FetchManifest(&manifest);
  if (retval != ObjectFetcher::kFailOk) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to load repository manifest "
                                    "(%d - %s)",
                                    retval, Code2Ascii(retval));
    return 1;
  }

  const upload::SpoolerDefinition spooler_definition(spooler, shash::kAny);
  UniquePtr<upload::AbstractUploader> uploader(
                       upload::AbstractUploader::Construct(spooler_definition));

  if (!uploader.IsValid()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to initialize spooler for '%s'",
             spooler.c_str());
    return 1;
  }

  UniquePtr<manifest::Reflog> reflog(CreateEmptyReflog(tmp_dir, repo_name));
  reflog->TakeDatabaseFileOwnership();

  reflog->BeginTransaction();
  AddStaticManifestObjects(reflog.weak_ref(), manifest.weak_ref());
  RootChainWalker walker(manifest.weak_ref(),
                         &object_fetcher,
                         reflog.weak_ref());
  walker.FindObjectsAndPopulateReflog();
  reflog->CommitTransaction();

  LogCvmfs(kLogCvmfs, kLogStdout, "found %d entries", reflog->CountEntries());

  const std::string reflog_db = reflog->CloseAndReturnDatabaseFile();
  uploader->Upload(reflog_db, ".cvmfsreflog");
  uploader->WaitForUpload();
  unlink(reflog_db.c_str());

  const int errors = uploader->GetNumberOfErrors();
  if (errors > 0) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to upload generated Reflog");
  }

  uploader->TearDown();

  return (errors == 0) ? 0 : 1;
}


void CommandReconstructReflog::AddStaticManifestObjects(
                                          manifest::Reflog    *reflog,
                                          manifest::Manifest  *manifest) const {
  const shash::Any certificate = manifest->certificate();
  const shash::Any meta_info   = manifest->meta_info();
  assert(!certificate.IsNull());

  bool success = reflog->AddCertificate(certificate);
  assert(success);
  LogCvmfs(kLogCvmfs, kLogStdout, "Certificate: %s",
           certificate.ToString().c_str());

  if (!meta_info.IsNull()) {
    success = reflog->AddMetainfo(meta_info);
    assert(success);
    LogCvmfs(kLogCvmfs, kLogStdout, "Metainfo: %s",
             meta_info.ToString().c_str());
  }
}


void RootChainWalker::FindObjectsAndPopulateReflog() {
  const shash::Any root_catalog = manifest_->catalog_hash();
  const shash::Any history      = manifest_->history();

  assert(!root_catalog.IsNull());
  WalkRootCatalogs(root_catalog);

  if (!history.IsNull()) {
    WalkHistories(history);
  }
}


void RootChainWalker::WalkRootCatalogs(const shash::Any &root_catalog_hash) {
  shash::Any           current_hash = root_catalog_hash;
  UniquePtr<CatalogTN> current_catalog;

  while (!current_hash.IsNull()                  &&
         !reflog_->ContainsCatalog(current_hash) &&
         (current_catalog = FetchCatalog(current_hash)).IsValid()) {
    LogCvmfs(kLogCvmfs, kLogStdout, "Catalog: %s Revision: %d",
             current_hash.ToString().c_str(), current_catalog->GetRevision());

    const bool success = reflog_->AddCatalog(current_hash);
    assert(success);

    current_hash = current_catalog->GetPreviousRevision();
  }
}


void RootChainWalker::WalkHistories(const shash::Any &history_hash) {
  shash::Any           current_hash = history_hash;
  UniquePtr<HistoryTN> current_history;

  while (!current_hash.IsNull()                  &&
         !reflog_->ContainsHistory(current_hash) &&
         (current_history = FetchHistory(current_hash)).IsValid()) {
    LogCvmfs(kLogCvmfs, kLogStdout, "History: %s",
             current_hash.ToString().c_str());

    WalkCatalogsInHistory(current_history);
    const bool success = reflog_->AddHistory(current_hash);
    assert(success);

    current_hash = current_history->previous_revision();
  }
}


void RootChainWalker::WalkCatalogsInHistory(const HistoryTN *history) {
  CatalogList tag_hashes;
  const bool list_success = history->GetHashes(&tag_hashes);
  assert(list_success);

  CatalogList bin_hashes;
  const bool bin_success = history->ListRecycleBin(&bin_hashes);
  assert(bin_success);

  WalkListedCatalogs(tag_hashes);
  WalkListedCatalogs(bin_hashes);
}


void RootChainWalker::WalkListedCatalogs(
                             const RootChainWalker::CatalogList &catalog_list) {
  CatalogList::const_iterator i    = catalog_list.begin();
  CatalogList::const_iterator iend = catalog_list.end();
  for (; i != iend; ++i) {
    WalkRootCatalogs(*i);
  }
}


RootChainWalker::CatalogTN* RootChainWalker::FetchCatalog(
                                                const shash::Any catalog_hash) {
  CatalogTN *catalog;
  const char *root_path = "";
  ObjectFetcherFailures::Failures failure =
    object_fetcher_->FetchCatalog(catalog_hash, root_path, &catalog);

  return ReturnOrAbort(failure, catalog_hash, catalog);
}


RootChainWalker::HistoryTN* RootChainWalker::FetchHistory(
                                                const shash::Any history_hash) {
  HistoryTN *history;
  ObjectFetcherFailures::Failures failure =
    object_fetcher_->FetchHistory(&history, history_hash);

  return ReturnOrAbort(failure, history_hash, history);
}


template <class DatabaseT>
DatabaseT* RootChainWalker::ReturnOrAbort(
                            const ObjectFetcherFailures::Failures  failure,
                            const shash::Any                      &content_hash,
                            DatabaseT                             *database) {
  switch (failure) {
    case ObjectFetcherFailures::kFailOk:
      return database;
    case ObjectFetcherFailures::kFailNotFound:
      return NULL;
    default:
      LogCvmfs(kLogCvmfs, kLogStderr, "Failed to load object '%s' (%d - %s)",
                                      content_hash.ToStringWithSuffix().c_str(),
                                      failure, Code2Ascii(failure));
      abort();
  }
}

}  // namespace swissknife
