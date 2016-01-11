/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SWISSKNIFE_SYNC_H_
#define CVMFS_SWISSKNIFE_SYNC_H_

#include <string>
#include <vector>

#include "compression.h"
#include "swissknife.h"
#include "upload.h"

struct SyncParameters {
  static const unsigned kDefaultMaxWeight = 100000;
  static const unsigned kDefaultMinWeight = 1000;
  static const uint64_t kDefaultEntryWarnThreshold = 500000;
  static const size_t kDefaultMinFileChunkSize = 4*1024*1024;
  static const size_t kDefaultAvgFileChunkSize = 8*1024*1024;
  static const size_t kDefaultMaxFileChunkSize = 16*1024*1024;

  SyncParameters() :
    spooler(NULL),
    union_fs_type("aufs"),
    print_changeset(false),
    dry_run(false),
    mucatalogs(false),
    use_file_chunking(false),
    ignore_xdir_hardlinks(false),
    stop_for_catalog_tweaks(false),
    include_xattrs(false),
    external_data(false),
    voms_authz(false),
    compression_alg(zlib::kZlibDefault),
    catalog_entry_warn_threshold(kDefaultEntryWarnThreshold),
    min_file_chunk_size(kDefaultMinFileChunkSize),
    avg_file_chunk_size(kDefaultAvgFileChunkSize),
    max_file_chunk_size(kDefaultMaxFileChunkSize),
    manual_revision(0),
    ttl_seconds(0),
    max_concurrent_write_jobs(0),
    is_balanced(false),
    max_weight(kDefaultMaxWeight),
    min_weight(kDefaultMinWeight) {}

  upload::Spooler *spooler;
  std::string      repo_name;
  std::string      dir_union;
  std::string      dir_scratch;
  std::string      dir_rdonly;
  std::string      dir_temp;
  shash::Any       base_hash;
  std::string      stratum0;
  std::string      manifest_path;
  std::string      spooler_definition;
  std::string      union_fs_type;
  std::string      public_keys;
  std::string      trusted_certs;
  std::string      authz_file;
  bool             print_changeset;
  bool             dry_run;
  bool             mucatalogs;
  bool             use_file_chunking;
  bool             ignore_xdir_hardlinks;
  bool             stop_for_catalog_tweaks;
  bool             include_xattrs;
  bool             external_data;
  bool             voms_authz;
  zlib::Algorithms compression_alg;
  uint64_t         catalog_entry_warn_threshold;
  size_t           min_file_chunk_size;
  size_t           avg_file_chunk_size;
  size_t           max_file_chunk_size;
  uint64_t         manual_revision;
  uint64_t         ttl_seconds;
  uint64_t         max_concurrent_write_jobs;
  bool             is_balanced;
  unsigned         max_weight;
  unsigned         min_weight;
};

namespace catalog {
class Dirtab;
class SimpleCatalogManager;
}

namespace swissknife {

class CommandCreate : public Command {
 public:
  ~CommandCreate() { }
  std::string GetName() { return "create"; }
  std::string GetDescription() {
    return "Bootstraps a fresh repository.";
  }
  ParameterList GetParams() {
    ParameterList r;
    r.push_back(Parameter::Mandatory('o', "manifest output file"));
    r.push_back(Parameter::Mandatory('t', "directory for temporary storage"));
    r.push_back(Parameter::Mandatory('r', "spooler definition"));
    r.push_back(Parameter::Optional('l', "log level (0-4, default: 2)"));
    r.push_back(Parameter::Optional('a', "hash algorithm (default: SHA-1)"));
    r.push_back(Parameter::Optional('V', "VOMS authz requirement "
                                         "(default: none)"));
    r.push_back(Parameter::Switch('v', "repository containing volatile files"));
    r.push_back(Parameter::Switch(
      'z', "mark new repository as garbage collectable"));
    r.push_back(Parameter::Optional('V', "VOMS authz requirement "
                                         "(default: none)"));
    return r;
  }
  int Main(const ArgumentList &args);
};


class CommandUpload : public Command {
 public:
  ~CommandUpload() { }
  std::string GetName() { return "upload"; }
  std::string GetDescription() {
    return "Uploads a local file to the repository.";
  }
  ParameterList GetParams() {
    ParameterList r;
    r.push_back(Parameter::Mandatory('i', "local file"));
    r.push_back(Parameter::Mandatory('o', "destination path"));
    r.push_back(Parameter::Mandatory('r', "spooler definition"));
    r.push_back(Parameter::Optional('a', "hash algorithm (default: SHA-1)"));
    return r;
  }
  int Main(const ArgumentList &args);
};


class CommandPeek : public Command {
 public:
  ~CommandPeek() { }
  std::string GetName() { return "peek"; }
  std::string GetDescription() {
    return "Checks whether a file exists in the repository.";
  }
  ParameterList GetParams() {
    ParameterList r;
    r.push_back(Parameter::Mandatory('d', "destination path"));
    r.push_back(Parameter::Mandatory('r', "spooler definition"));
    return r;
  }
  int Main(const ArgumentList &args);
};


class CommandRemove : public Command {
 public:
  ~CommandRemove() { }
  std::string GetName() { return "remove"; }
  std::string GetDescription() {
    return "Removes a file in the repository storage.";
  }
  ParameterList GetParams() {
    ParameterList r;
    r.push_back(Parameter::Mandatory('o', "path to file"));
    r.push_back(Parameter::Mandatory('r', "spooler definition"));
    return r;
  }
  int Main(const ArgumentList &args);
};


class CommandApplyDirtab : public Command {
 public:
  CommandApplyDirtab() : verbose_(false) { }
  ~CommandApplyDirtab() { }
  std::string GetName() { return "dirtab"; }
  std::string GetDescription() {
    return "Parses the dirtab file and produces nested catalog markers.";
  }
  ParameterList GetParams() {
    ParameterList r;
    r.push_back(Parameter::Mandatory('d', "path to dirtab file"));
    r.push_back(Parameter::Mandatory('u', "union volume"));
    r.push_back(Parameter::Mandatory('s', "scratch directory"));
    r.push_back(Parameter::Mandatory('b', "base hash"));
    r.push_back(Parameter::Mandatory('w', "stratum 0 base url"));
    r.push_back(Parameter::Mandatory('t', "directory for temporary storage"));
    r.push_back(Parameter::Switch('x', "verbose mode"));
    return r;
  }
  int Main(const ArgumentList &args);

 protected:
  void DetermineNestedCatalogCandidates(
    const catalog::Dirtab &dirtab,
    catalog::SimpleCatalogManager *catalog_manager,
    std::vector<std::string> *nested_catalog_candidates);
  void FilterCandidatesFromGlobResult(
                     const catalog::Dirtab &dirtab,
                     char **paths, const size_t npaths,
                     catalog::SimpleCatalogManager  *catalog_manager,
                     std::vector<std::string>       *nested_catalog_candidates);
  bool CreateCatalogMarkers(
    const std::vector<std::string> &new_nested_catalogs);

 private:
  std::string union_dir_;
  std::string scratch_dir_;
  bool        verbose_;
};


class CommandSync : public Command {
 public:
  ~CommandSync() { }
  std::string GetName() { return "sync"; }
  std::string GetDescription() {
    return "Pushes changes from scratch area back to the repository.";
  }
  ParameterList GetParams() {
    ParameterList r;
    r.push_back(Parameter::Mandatory('b', "base hash"));
    r.push_back(Parameter::Mandatory('c', "r/o volume"));
    r.push_back(Parameter::Mandatory('o', "manifest output file"));
    r.push_back(Parameter::Mandatory('r', "spooler definition"));
    r.push_back(Parameter::Mandatory('s', "scratch directory"));
    r.push_back(Parameter::Mandatory('t', "directory for tee"));
    r.push_back(Parameter::Mandatory('u', "union volume"));
    r.push_back(Parameter::Mandatory('w', "stratum 0 base url"));
    r.push_back(Parameter::Mandatory('K', "public key(s) for repo"));
    r.push_back(Parameter::Mandatory('N', "fully qualified repository name"));

    r.push_back(Parameter::Optional('a', "desired average chunk size (bytes)"));
    r.push_back(Parameter::Optional('e', "hash algorithm (default: SHA-1)"));
    r.push_back(Parameter::Optional('f', "union filesystem type"));
    r.push_back(Parameter::Optional('h', "maximal file chunk size in bytes"));
    r.push_back(Parameter::Optional('j', "catalog entry warning threshold"));
    r.push_back(Parameter::Optional('l', "minimal file chunk size in bytes"));
    r.push_back(Parameter::Optional('q', "number of concurrent write jobs"));
    r.push_back(Parameter::Optional('v', "manual revision number"));
    r.push_back(Parameter::Optional('z', "log level (0-4, default: 2)"));
    r.push_back(Parameter::Optional('C', "trusted certificates"));
    r.push_back(Parameter::Optional('F', "Authz file listing (default: none)"));
    r.push_back(Parameter::Optional('M', "minimum weight of the autocatalogs"));
    r.push_back(Parameter::Optional('T', "Root catalog TTL in seconds"));
    r.push_back(Parameter::Optional('X', "maximum weight of the autocatalogs"));
    r.push_back(Parameter::Optional('Z', "compression algorithm "
                                         "(default: zlib)"));

    r.push_back(Parameter::Switch('d', "pause publishing to allow for catalog "
                                       "tweaks"));
    r.push_back(Parameter::Switch('i', "ignore x-directory hardlinks"));
    r.push_back(Parameter::Switch('k', "include extended attributes"));
    r.push_back(Parameter::Switch('m', "create micro catalogs"));
    r.push_back(Parameter::Switch('n', "create new repository"));
    r.push_back(Parameter::Switch('p', "enable file chunking"));
    r.push_back(Parameter::Switch('x', "print change set"));
    r.push_back(Parameter::Switch('y', "dry run"));
    r.push_back(Parameter::Switch('A', "autocatalog enabled/disabled"));
    r.push_back(Parameter::Switch('L', "enable HTTP redirects"));
    r.push_back(Parameter::Switch('V', "Publish format compatible with "
                                       "authenticated repos"));
    r.push_back(Parameter::Switch('Y', "enable external data"));
    return r;
  }
  int Main(const ArgumentList &args);

 protected:
  bool ReadFileChunkingArgs(const swissknife::ArgumentList &args,
                            SyncParameters *params);
  bool CheckParams(const SyncParameters &p);
};

}  // namespace swissknife

#endif  // CVMFS_SWISSKNIFE_SYNC_H_
