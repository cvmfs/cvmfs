/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SWISSKNIFE_SYNC_H_
#define CVMFS_SWISSKNIFE_SYNC_H_

#include <string>
#include <vector>

#include "compression.h"
#include "repository_tag.h"
#include "swissknife.h"
#include "upload.h"

struct SyncParameters {
  static const unsigned kDefaultMaxWeight = 100000;
  static const unsigned kDefaultMinWeight = 1000;
  static const size_t kDefaultMinFileChunkSize = 4 * 1024 * 1024;
  static const size_t kDefaultAvgFileChunkSize = 8 * 1024 * 1024;
  static const size_t kDefaultMaxFileChunkSize = 16 * 1024 * 1024;
  static const unsigned kDefaultNestedKcatalogLimit = 500;
  static const unsigned kDefaultRootKcatalogLimit = 200;
  static const unsigned kDefaultFileMbyteLimit = 1024;

  SyncParameters()
      : spooler(NULL),
        union_fs_type("aufs"),
        to_delete(""),
        print_changeset(false),
        dry_run(false),
        mucatalogs(false),
        use_file_chunking(false),
        generate_legacy_bulk_chunks(false),
        ignore_xdir_hardlinks(false),
        stop_for_catalog_tweaks(false),
        include_xattrs(false),
        external_data(false),
        direct_io(false),
        voms_authz(false),
        virtual_dir_actions(0),
        ignore_special_files(false),
        branched_catalog(false),
        compression_alg(zlib::kZlibDefault),
        enforce_limits(false),
        nested_kcatalog_limit(0),
        root_kcatalog_limit(0),
        file_mbyte_limit(0),
        min_file_chunk_size(kDefaultMinFileChunkSize),
        avg_file_chunk_size(kDefaultAvgFileChunkSize),
        max_file_chunk_size(kDefaultMaxFileChunkSize),
        manual_revision(0),
        ttl_seconds(0),
        max_concurrent_write_jobs(0),
        num_upload_tasks(1),
        is_balanced(false),
        max_weight(kDefaultMaxWeight),
        min_weight(kDefaultMinWeight),
        session_token_file(),
        key_file(),
        repo_tag() {}

  upload::Spooler *spooler;
  std::string repo_name;
  std::string dir_union;
  std::string dir_scratch;
  std::string dir_rdonly;
  std::string dir_temp;
  shash::Any base_hash;
  std::string stratum0;
  std::string manifest_path;
  std::string spooler_definition;
  std::string union_fs_type;
  std::string public_keys;
  std::string trusted_certs;
  std::string authz_file;
  std::string tar_file;
  std::string base_directory;
  std::string to_delete;
  bool print_changeset;
  bool dry_run;
  bool mucatalogs;
  bool use_file_chunking;
  bool generate_legacy_bulk_chunks;
  bool ignore_xdir_hardlinks;
  bool stop_for_catalog_tweaks;
  bool include_xattrs;
  bool external_data;
  bool direct_io;
  bool voms_authz;
  unsigned virtual_dir_actions;  // bit field
  bool ignore_special_files;
  bool branched_catalog;
  zlib::Algorithms compression_alg;
  bool enforce_limits;
  unsigned nested_kcatalog_limit;
  unsigned root_kcatalog_limit;
  unsigned file_mbyte_limit;
  size_t min_file_chunk_size;
  size_t avg_file_chunk_size;
  size_t max_file_chunk_size;
  uint64_t manual_revision;
  uint64_t ttl_seconds;
  uint64_t max_concurrent_write_jobs;
  unsigned num_upload_tasks;
  bool is_balanced;
  unsigned max_weight;
  unsigned min_weight;

  // Parameters for when upstream type is HTTP
  std::string session_token_file;
  std::string key_file;
  RepositoryTag repo_tag;
};

namespace catalog {
class Dirtab;
class SimpleCatalogManager;
}  // namespace catalog

namespace swissknife {

class CommandCreate : public Command {
 public:
  ~CommandCreate() {}
  virtual std::string GetName() const { return "create"; }
  virtual std::string GetDescription() const {
    return "Bootstraps a fresh repository.";
  }
  virtual ParameterList GetParams() const {
    ParameterList r;
    r.push_back(Parameter::Mandatory('o', "manifest output file"));
    r.push_back(Parameter::Mandatory('t', "directory for temporary storage"));
    r.push_back(Parameter::Mandatory('r', "spooler definition"));
    r.push_back(Parameter::Mandatory('n', "repository name"));
    r.push_back(Parameter::Mandatory('R', "path to reflog.chksum file"));
    r.push_back(Parameter::Optional('l', "log level (0-4, default: 2)"));
    r.push_back(Parameter::Optional('a', "hash algorithm (default: SHA-1)"));
    r.push_back(Parameter::Optional('V',
                                    "VOMS authz requirement "
                                    "(default: none)"));
    r.push_back(Parameter::Switch('v', "repository containing volatile files"));
    r.push_back(
        Parameter::Switch('z', "mark new repository as garbage collectable"));
    r.push_back(Parameter::Optional('V',
                                    "VOMS authz requirement "
                                    "(default: none)"));
    return r;
  }
  int Main(const ArgumentList &args);
};

class CommandUpload : public Command {
 public:
  ~CommandUpload() {}
  virtual std::string GetName() const { return "upload"; }
  virtual std::string GetDescription() const {
    return "Uploads a local file to the repository.";
  }
  virtual ParameterList GetParams() const {
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
  ~CommandPeek() {}
  virtual std::string GetName() const { return "peek"; }
  virtual std::string GetDescription() const {
    return "Checks whether a file exists in the repository.";
  }
  virtual ParameterList GetParams() const {
    ParameterList r;
    r.push_back(Parameter::Mandatory('d', "destination path"));
    r.push_back(Parameter::Mandatory('r', "spooler definition"));
    return r;
  }
  int Main(const ArgumentList &args);
};

class CommandRemove : public Command {
 public:
  ~CommandRemove() {}
  virtual std::string GetName() const { return "remove"; }
  virtual std::string GetDescription() const {
    return "Removes a file in the repository storage.";
  }
  virtual ParameterList GetParams() const {
    ParameterList r;
    r.push_back(Parameter::Mandatory('o', "path to file"));
    r.push_back(Parameter::Mandatory('r', "spooler definition"));
    return r;
  }
  int Main(const ArgumentList &args);
};

class CommandApplyDirtab : public Command {
 public:
  CommandApplyDirtab() : verbose_(false) {}
  ~CommandApplyDirtab() {}
  virtual std::string GetName() const { return "dirtab"; }
  virtual std::string GetDescription() const {
    return "Parses the dirtab file and produces nested catalog markers.";
  }
  virtual ParameterList GetParams() const {
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
      const catalog::Dirtab &dirtab, char **paths, const size_t npaths,
      catalog::SimpleCatalogManager *catalog_manager,
      std::vector<std::string> *nested_catalog_candidates);
  bool CreateCatalogMarkers(
      const std::vector<std::string> &new_nested_catalogs);

 private:
  std::string union_dir_;
  std::string scratch_dir_;
  bool verbose_;
};

class CommandSync : public Command {
 public:
  ~CommandSync() {}
  virtual std::string GetName() const { return "sync"; }
  virtual std::string GetDescription() const {
    return "Pushes changes from scratch area back to the repository.";
  }
  virtual ParameterList GetParams() const {
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
    r.push_back(Parameter::Optional('l', "minimal file chunk size in bytes"));
    r.push_back(Parameter::Optional('q', "number of concurrent write jobs"));
    r.push_back(Parameter::Optional('0', "number of upload tasks"));
    r.push_back(Parameter::Optional('v', "manual revision number"));
    r.push_back(Parameter::Optional('z', "log level (0-4, default: 2)"));
    r.push_back(Parameter::Optional('C', "trusted certificates"));
    r.push_back(Parameter::Optional('F', "Authz file listing (default: none)"));
    r.push_back(Parameter::Optional('M', "minimum weight of the autocatalogs"));
    r.push_back(
        Parameter::Optional('Q', "nested catalog limit in kilo-entries"));
    r.push_back(Parameter::Optional('R', "root catalog limit in kilo-entries"));
    r.push_back(Parameter::Optional('T', "Root catalog TTL in seconds"));
    r.push_back(Parameter::Optional('U', "file size limit in megabytes"));
    r.push_back(
        Parameter::Optional('D', "tag name (only used when upstream is GW)"));
    r.push_back(Parameter::Optional(
        'G', "tag channel (only used when upstream is GW)"));
    r.push_back(Parameter::Optional(
        'J', "tag description (only used when upstream is GW)"));
    r.push_back(Parameter::Optional('X', "maximum weight of the autocatalogs"));
    r.push_back(Parameter::Optional('Z',
                                    "compression algorithm "
                                    "(default: zlib)"));
    r.push_back(Parameter::Optional('S',
                                    "virtual directory options "
                                    "[snapshots, remove]"));

    r.push_back(Parameter::Switch('d',
                                  "pause publishing to allow for catalog "
                                  "tweaks"));
    r.push_back(Parameter::Switch('i', "ignore x-directory hardlinks"));
    r.push_back(Parameter::Switch('g', "ignore special files"));
    r.push_back(Parameter::Switch('k', "include extended attributes"));
    r.push_back(Parameter::Switch('m', "create micro catalogs"));
    r.push_back(Parameter::Switch('n', "create new repository"));
    r.push_back(Parameter::Switch('p', "enable file chunking"));
    r.push_back(Parameter::Switch('O', "generate legacy bulk chunks"));
    r.push_back(Parameter::Switch('x', "print change set"));
    r.push_back(Parameter::Switch('y', "dry run"));
    r.push_back(Parameter::Switch('A', "autocatalog enabled/disabled"));
    r.push_back(Parameter::Switch('E', "enforce limits instead of warning"));
    r.push_back(Parameter::Switch('L', "enable HTTP redirects"));
    r.push_back(Parameter::Switch('V',
                                  "Publish format compatible with "
                                  "authenticated repos"));
    r.push_back(Parameter::Switch('Y', "enable external data"));
    r.push_back(Parameter::Switch('W', "set direct I/O for regular files"));
    r.push_back(Parameter::Switch('B', "branched catalog (no manifest)"));
    r.push_back(Parameter::Switch('I', "upload updated statistics DB file"));

    r.push_back(Parameter::Optional('P', "session_token_file"));
    r.push_back(Parameter::Optional('H', "key file for HTTP API"));

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
