/**
 * This file is part of the CernVM File System.
 *
 * This command processes a repository's catalog structure to detect and remove
 * outdated and/or unneeded data objects.
 */

#include "cvmfs_config.h"
#include "swissknife_list_reflog.h"

#include "manifest.h"
#include "object_fetcher.h"
#include "reflog.h"
#include "util/posix.h"
#include "util/string.h"

using namespace std;  // NOLINT

namespace swissknife {

ParameterList CommandListReflog::GetParams() const {
  ParameterList r;
  r.push_back(Parameter::Mandatory('r', "repository url / local storage path"));
  r.push_back(Parameter::Mandatory('n', "fully qualified repository name"));
  r.push_back(Parameter::Optional('R', "path to reflog.chksum file"));
  r.push_back(Parameter::Optional('k', "repository master key(s) / dir"));
  r.push_back(Parameter::Optional('t', "temporary directory"));
  r.push_back(Parameter::Optional('o', "output file"));
  r.push_back(Parameter::Optional('@', "proxy url"));
  return r;
}

int CommandListReflog::Main(const ArgumentList &args) {
  const string &repo_url = *args.find('r')->second;
  const string &repo_name = *args.find('n')->second;
  const std::string &reflog_chksum_path = (args.count('R') > 0) ?
    *args.find('R')->second : "";
  string repo_keys = (args.count('k') > 0) ?
    *args.find('k')->second : "";
  if (DirectoryExists(repo_keys))
    repo_keys = JoinStrings(FindFilesBySuffix(repo_keys, ".pub"), ":");
  const string temp_directory = (args.count('t') > 0) ?
    *args.find('t')->second : "/tmp";
  const string output_path = (args.count('o') > 0) ?
    *args.find('o')->second : "";

  shash::Any reflog_hash;
  if (reflog_chksum_path != "") {
    if (!manifest::Reflog::ReadChecksum(reflog_chksum_path, &reflog_hash)) {
      LogCvmfs(kLogCvmfs, kLogStderr, "Could not read reflog checksum");
      return 1;
    }
  }

  const bool follow_redirects = false;
  const string proxy = (args.count('@') > 0) ? *args.find('@')->second : "";
  if (!this->InitDownloadManager(follow_redirects, proxy) ||
      !this->InitSignatureManager(repo_keys)) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to init repo connection");
    return 1;
  }

  bool success;
  if (IsHttpUrl(repo_url)) {
    HttpObjectFetcher<> object_fetcher(repo_name,
                               repo_url,
                               temp_directory,
                               download_manager(),
                               signature_manager());
    if (reflog_hash.IsNull()) {
      manifest::Manifest *manifest = NULL;
      ObjectFetcherFailures::Failures failure;
      switch (failure = object_fetcher.FetchManifest(&manifest)) {
        case ObjectFetcherFailures::kFailOk:
          reflog_hash = manifest->reflog_hash();
          break;
        default:
          LogCvmfs(kLogCvmfs, kLogStderr, "Failed to fetch manifest: %s",
                    Code2Ascii(failure));
          return 1;
      }
      delete manifest;
    }
    success = Run(&object_fetcher, repo_name, output_path, reflog_hash);
  } else {
    LocalObjectFetcher<> object_fetcher(repo_url, temp_directory);
    success = Run(&object_fetcher, repo_name, output_path, reflog_hash);
  }

  return (success) ? 0 : 1;
}

template <class ObjectFetcherT>
bool CommandListReflog::Run(ObjectFetcherT *object_fetcher, string repo_name,
                            string output_path, shash::Any reflog_hash)
{
  typename ObjectFetcherT::ReflogTN *reflog;
  reflog = FetchReflog(object_fetcher, repo_name, reflog_hash);

  shash::Any null_hash = shash::Any(reflog_hash.algorithm);
  objects_ = new SmallHashDynamic<shash::Any, bool>;
  objects_->Init(1024, null_hash, hasher);

  // Traverse through catalogs and regular objects
  vector<shash::Any> catalogs;
  if (NULL == reflog || !reflog->List(SqlReflog::kRefCatalog, &catalogs)) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Failed to list catalog reference log");
    return false;
  }
  typename CatalogTraversal<ObjectFetcherT>::Parameters traversal_params;
  traversal_params.object_fetcher = object_fetcher;
  CatalogTraversal<ObjectFetcherT> traversal(traversal_params);
  traversal.RegisterListener(
    &swissknife::CommandListReflog::CatalogCallback,
    this);
  bool success = true;
  vector<shash::Any>::iterator i = catalogs.begin();
  const vector<shash::Any>::const_iterator iend = catalogs.end();
  for (; i != iend && success; i++) {
    success &= traversal.TraverseRevision(*i,
                CatalogTraversal<ObjectFetcherT>::kBreadthFirst);
  }

  if (!success) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "Catalog traversal aborted due to an error");
    return false;
  }

  // Add history, certificate, metainfo objects from reflog
  vector<shash::Any> histories, certificates, metainfos;
  if (!reflog->List(SqlReflog::kRefHistory, &histories)) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "Failed to fetch history objects from reflog");
    return false;
  }
  if (!reflog->List(SqlReflog::kRefCertificate, &certificates)) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "Failed to fetch certificate objects from reflog");
    return false;
  }
  if (!reflog->List(SqlReflog::kRefMetainfo, &metainfos)) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "Failed to fetch metainfo objects from reflog");
    return false;
  }
  InsertObjects(histories);
  InsertObjects(certificates);
  InsertObjects(metainfos);

  // Clean up reflog file
  delete reflog;

  LogCvmfs(kLogCvmfs, kLogStderr, "Number of objects: %u", objects_->size());

  if (output_path == "") {
    DumpObjects(stdout);
  } else {
    int fd = open(output_path.c_str(), O_WRONLY | O_CREAT, 0644);
    assert(fd);
    FILE *stream = fdopen(fd, "w");
    DumpObjects(stream);
    fclose(stream);  // no need to call close after fclose
  }

  return success;
}

void CommandListReflog::CatalogCallback(
  const CatalogTraversalData<catalog::Catalog> &data)
{
  LogCvmfs(kLogCvmfs, kLogStderr, "Processing catalog \"%s\"",
           data.catalog->mountpoint().c_str());
  const catalog::Catalog::HashVector &referenced_hashes =
    data.catalog->GetReferencedObjects();
  InsertObjects(referenced_hashes);
  if (data.catalog->hash() != objects_->empty_key())
    objects_->Insert(data.catalog->hash(), true);
}

void CommandListReflog::InsertObjects(const vector<shash::Any> &list) {
  vector<shash::Any>::const_iterator i = list.begin();
  const vector<shash::Any>::const_iterator iend = list.end();
  for (; i != iend; ++i) {
    if ((*i) != objects_->empty_key())
      objects_->Insert(*i, true);
  }
}

void CommandListReflog::DumpObjects(FILE *stream)
{
  shash::Any empty_key = objects_->empty_key();
  shash::Any *hashes = objects_->keys();
  for (uint32_t i = 0; i < objects_->capacity(); ++i) {
    if (hashes[i] != empty_key) {
      fprintf(stream, "%s\n", hashes[i].ToString().c_str());
    }
  }
}

}  // namespace swissknife
