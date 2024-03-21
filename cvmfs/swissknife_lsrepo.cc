/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "swissknife_lsrepo.h"

#include <string>

#include "util/logging.h"
#include "util/posix.h"
#include "util/string.h"

namespace swissknife {

CommandListCatalogs::CommandListCatalogs() :
  print_tree_(false), print_hash_(false), print_size_(false),
  print_entries_(false) {}


ParameterList CommandListCatalogs::GetParams() const {
  ParameterList r;
  r.push_back(Parameter::Mandatory(
              'r', "repository URL (absolute local path or remote URL)"));
  r.push_back(Parameter::Optional('n', "fully qualified repository name"));
  r.push_back(Parameter::Optional('k', "repository master key(s) / dir"));
  r.push_back(Parameter::Optional('l', "temporary directory"));
  r.push_back(Parameter::Optional('h', "root hash (other than trunk)"));
  r.push_back(Parameter::Optional('@', "proxy url"));
  r.push_back(Parameter::Switch('t', "print tree structure of catalogs"));
  r.push_back(Parameter::Switch('d', "print digest for each catalog"));
  r.push_back(Parameter::Switch('s', "print catalog file sizes"));
  r.push_back(Parameter::Switch('e', "print number of catalog entries"));
  return r;
}


int CommandListCatalogs::Main(const ArgumentList &args) {
  print_tree_    = (args.count('t') > 0);
  print_hash_    = (args.count('d') > 0);
  print_size_    = (args.count('s') > 0);
  print_entries_ = (args.count('e') > 0);

  shash::Any manual_root_hash;
  const std::string &repo_url  = *args.find('r')->second;
  const std::string &repo_name =
    (args.count('n') > 0) ? *args.find('n')->second : "";
  std::string repo_keys =
    (args.count('k') > 0) ? *args.find('k')->second : "";
  if (DirectoryExists(repo_keys))
    repo_keys = JoinStrings(FindFilesBySuffix(repo_keys, ".pub"), ":");
  const std::string &tmp_dir   =
    (args.count('l') > 0) ? *args.find('l')->second : "/tmp";
  if (args.count('h') > 0) {
    manual_root_hash = shash::MkFromHexPtr(shash::HexPtr(
      *args.find('h')->second), shash::kSuffixCatalog);
  }

  bool success = false;
  if (IsHttpUrl(repo_url)) {
    const bool follow_redirects = false;
    const std::string proxy = ((args.count('@') > 0) ?
                               *args.find('@')->second : "");
    if (!this->InitDownloadManager(follow_redirects, proxy) ||
        !this->InitSignatureManager(repo_keys)) {
      LogCvmfs(kLogCatalog, kLogStderr, "Failed to init remote connection");
      return 1;
    }

    HttpObjectFetcher<catalog::Catalog,
                      history::SqliteHistory> fetcher(repo_name,
                                                      repo_url,
                                                      tmp_dir,
                                                      download_manager(),
                                                      signature_manager());
    success = Run(manual_root_hash, &fetcher);
  } else {
    LocalObjectFetcher<> fetcher(repo_url, tmp_dir);
    success = Run(manual_root_hash, &fetcher);
  }

  return (success) ? 0 : 1;
}


void CommandListCatalogs::CatalogCallback(
                           const CatalogTraversalData<catalog::Catalog> &data) {
  std::string tree_indent;
  std::string hash_string;
  std::string clg_size;
  std::string clg_entries;
  std::string path;

  if (print_tree_) {
    for (unsigned int i = 1; i < data.tree_level; ++i) {
      tree_indent += "\u2502  ";
    }

    if (data.tree_level > 0)
      tree_indent += "\u251C\u2500 ";
  }

  if (print_hash_) {
    hash_string = data.catalog_hash.ToString() + " ";
  }

  if (print_size_) {
    clg_size = StringifyInt(data.file_size) + "B ";
  }

  if (print_entries_) {
    clg_entries = StringifyInt(data.catalog->GetNumEntries()) + " ";
  }

  path = data.catalog->mountpoint().ToString();
  if (path.empty())
    path = "/";

  LogCvmfs(kLogCatalog, kLogStdout, "%s%s%s%s%s",
    tree_indent.c_str(), hash_string.c_str(), clg_size.c_str(),
    clg_entries.c_str(), path.c_str());
}

}  // namespace swissknife
