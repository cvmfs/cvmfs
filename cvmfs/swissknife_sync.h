/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SWISSKNIFE_SYNC_H_
#define CVMFS_SWISSKNIFE_SYNC_H_

#include <string>
#include <vector>

#include "swissknife.h"
#include "upload.h"

struct SyncParameters {
  SyncParameters() :
    spooler(NULL),
    union_fs_type("aufs"),
    print_changeset(false),
    dry_run(false),
    mucatalogs(false),
    use_file_chunking(false),
    ignore_xdir_hardlinks(false),
    stop_for_catalog_tweaks(false),
    garbage_collectable(false),
    include_xattrs(false),
    catalog_entry_warn_threshold(500000),
    min_file_chunk_size(4*1024*1024),
    avg_file_chunk_size(8*1024*1024),
    max_file_chunk_size(16*1024*1024),
    manual_revision(0),
    max_concurrent_write_jobs(0) {}

  upload::Spooler *spooler;
  std::string      dir_union;
  std::string      dir_scratch;
  std::string      dir_rdonly;
  std::string      dir_temp;
  shash::Any       base_hash;
  std::string      stratum0;
  std::string      manifest_path;
  std::string      spooler_definition;
  std::string      union_fs_type;
  bool             print_changeset;
  bool             dry_run;
  bool             mucatalogs;
  bool             use_file_chunking;
  bool             ignore_xdir_hardlinks;
  bool             stop_for_catalog_tweaks;
  bool             garbage_collectable;
  bool             include_xattrs;
  uint64_t         catalog_entry_warn_threshold;
  size_t           min_file_chunk_size;
  size_t           avg_file_chunk_size;
  size_t           max_file_chunk_size;
  uint64_t         manual_revision;
  uint64_t         max_concurrent_write_jobs;
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
    r.push_back(Parameter::Switch('v', "repository containing volatile files"));
    r.push_back(Parameter::Switch(
      'z', "mark new repository as garbage collectable"));
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
    r.push_back(Parameter::Mandatory('u', "union volume"));
    r.push_back(Parameter::Mandatory('s', "scratch directory"));
    r.push_back(Parameter::Mandatory('c', "r/o volume"));
    r.push_back(Parameter::Mandatory('t', "directory for tee"));
    r.push_back(Parameter::Mandatory('b', "base hash"));
    r.push_back(Parameter::Mandatory('w', "stratum 0 base url"));
    r.push_back(Parameter::Mandatory('o', "manifest output file"));
    r.push_back(Parameter::Mandatory('r', "spooler definition"));
    r.push_back(Parameter::Switch('n', "create new repository"));
    r.push_back(Parameter::Switch('x', "print change set"));
    r.push_back(Parameter::Switch('y', "dry run"));
    r.push_back(Parameter::Switch('L', "enable HTTP redirects"));
    r.push_back(Parameter::Switch('m', "create micro catalogs"));
    r.push_back(Parameter::Switch('i', "ignore x-directory hardlinks"));
    r.push_back(Parameter::Switch('d', "pause publishing to allow for "
                                          "catalog tweaks"));
    r.push_back(Parameter::Switch('g', "repo is garbage collectable"));
    r.push_back(Parameter::Switch('p', "enable file chunking"));
    r.push_back(Parameter::Switch('k', "include extended attributes"));
    r.push_back(Parameter::Switch('A', "acquire CAP_SYS_ADMIN on start up"));
    r.push_back(Parameter::Optional('z', "log level (0-4, default: 2)"));
    r.push_back(Parameter::Optional('a',
      "desired average chunk size in bytes"));
    r.push_back(Parameter::Optional('l', "minimal file chunk size in bytes"));
    r.push_back(Parameter::Optional('h', "maximal file chunk size in bytes"));
    r.push_back(Parameter::Optional('f', "union filesystem type"));
    r.push_back(Parameter::Optional('e', "hash algorithm (default: SHA-1)"));
    r.push_back(Parameter::Optional('j', "catalog entry warning threshold"));
    r.push_back(Parameter::Optional('v', "manual revision number"));
    r.push_back(Parameter::Optional('q', "number of concurrent write jobs"));
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
