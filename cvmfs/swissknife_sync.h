/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SWISSKNIFE_SYNC_H_
#define CVMFS_SWISSKNIFE_SYNC_H_

#include <string>
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
    catalog_entry_warn_threshold(500000),
    min_file_chunk_size(4*1024*1024),
    avg_file_chunk_size(8*1024*1024),
    max_file_chunk_size(16*1024*1024) {}

  upload::Spooler *spooler;
  std::string      dir_union;
  std::string      dir_scratch;
  std::string      dir_rdonly;
  std::string      dir_temp;
  std::string      base_hash;
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
  uint64_t         catalog_entry_warn_threshold;
  size_t           min_file_chunk_size;
  size_t           avg_file_chunk_size;
  size_t           max_file_chunk_size;
};


namespace swissknife {

class CommandCreate : public Command {
 public:
  ~CommandCreate() { };
  std::string GetName() { return "create"; };
  std::string GetDescription() {
    return "Bootstraps a fresh repository.";
  };
  ParameterList GetParams() {
    ParameterList result;
    result.push_back(Parameter('o', "manifest output file", false, false));
    result.push_back(Parameter('t', "directory for temporary storage",
                               false, false));
    result.push_back(Parameter('r', "spooler definition", false, false));
    result.push_back(Parameter('l', "log level (0-4, default: 2)",
                               true, false));
    result.push_back(Parameter('a', "hash algorithm (default: SHA-1)",
                               true, false));
    result.push_back(Parameter('v', "repository containing volatile files",
                               true, true));
    return result;
  }
  int Main(const ArgumentList &args);
};


class CommandUpload : public Command {
 public:
  ~CommandUpload() { };
  std::string GetName() { return "upload"; };
  std::string GetDescription() {
    return "Uploads a local file to the repository.";
  };
  ParameterList GetParams() {
    ParameterList result;
    result.push_back(Parameter('i', "local file", false, false));
    result.push_back(Parameter('o', "destination path", false, false));
    result.push_back(Parameter('r', "spooler definition", false, false));
    result.push_back(Parameter('a', "hash algorithm (default: SHA-1)",
                               true, false));
    return result;
  }
  int Main(const ArgumentList &args);
};


class CommandPeek : public Command {
public:
  ~CommandPeek() { };
  std::string GetName() { return "peek"; };
  std::string GetDescription() {
    return "Checks whether a file exists in the repository.";
  };
  ParameterList GetParams() {
    ParameterList result;
    result.push_back(Parameter('d', "destination path", false, false));
    result.push_back(Parameter('r', "spooler definition", false, false));
    return result;
  }
  int Main(const ArgumentList &args);
};


class CommandRemove : public Command {
 public:
  ~CommandRemove() { };
  std::string GetName() { return "remove"; };
  std::string GetDescription() {
    return "Removes a file in the repository storage.";
  };
  ParameterList GetParams() {
    ParameterList result;
    result.push_back(Parameter('o', "path to file", false, false));
    result.push_back(Parameter('r', "spooler definition", false, false));
    return result;
  }
  int Main(const ArgumentList &args);
};


class CommandApplyDirtab : public Command {
 public:
  ~CommandApplyDirtab() {};
  std::string GetName() { return "dirtab"; }
  std::string GetDescription() {
    return "Parses the dirtab file and produces nested catalog markers.";
  }
  ParameterList GetParams() {
    ParameterList result;
    result.push_back(Parameter('k', "repository master key(s)", false, false));
    result.push_back(Parameter('y', "trusted certificate directories",
                               true, false));
    result.push_back(Parameter('d', "path to dirtab file", false, false));
    result.push_back(Parameter('b', "base hash", false, false));
    result.push_back(Parameter('w', "stratum 0 base url", false, false));
    result.push_back(Parameter('t', "directory for temporary storage",
                               false, false));
    return result;
  }
  int Main(const ArgumentList &args);
};


class CommandSync : public Command {
 public:
  ~CommandSync() { };
  std::string GetName() { return "sync"; };
  std::string GetDescription() {
    return "Pushes changes from scratch area back to the repository.";
  };
  ParameterList GetParams() {
    ParameterList result;
    result.push_back(Parameter('u', "union volume", false, false));
    result.push_back(Parameter('s', "scratch directory", false, false));
    result.push_back(Parameter('c', "r/o volume", false, false));
    result.push_back(Parameter('t', "directory for temporary storage",
                               false, false));
    result.push_back(Parameter('b', "base hash", false, false));
    result.push_back(Parameter('w', "stratum 0 base url", false, false));
    result.push_back(Parameter('o', "manifest output file", false, false));
    result.push_back(Parameter('r', "spooler definition", false, false));

    result.push_back(Parameter('n', "create new repository", true, true));
    result.push_back(Parameter('x', "print change set", true, true));
    result.push_back(Parameter('y', "dry run", true, true));
    result.push_back(Parameter('m', "create micro catalogs", true, true));
    result.push_back(Parameter('i', "ignore x-directory hardlinks",
                               true, true));
    result.push_back(Parameter('d', "pause publishing to allow for catalog tweaks",
                               true, true));
    result.push_back(Parameter('z', "log level (0-4, default: 2)",
                               true, false));

    result.push_back(Parameter('p', "enable file chunking", true, true));
    result.push_back(Parameter('a', "desired average chunk size in bytes", true,
                               false));
    result.push_back(Parameter('l', "minimal file chunk size in bytes", true,
                               false));
    result.push_back(Parameter('h', "maximal file chunk size in bytes", true,
                               false));
    result.push_back(Parameter('f', "union filesystem type", true, false));
    result.push_back(Parameter('e', "hash algorithm (default: SHA-1)",
                               true, false));
    result.push_back(Parameter('j', "catalog entry warning threshold",
                               true, false));
    return result;
  }
  int Main(const ArgumentList &args);

 protected:
  bool ReadFileChunkingArgs(const swissknife::ArgumentList &args,
                            SyncParameters &params);
  bool CheckParams(const SyncParameters &p);
};

}  // namespace swissknife

#endif  // CVMFS_SWISSKNIFE_SYNC_H_
