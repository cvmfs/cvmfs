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
  size_t           min_file_chunk_size;
  size_t           avg_file_chunk_size;
  size_t           max_file_chunk_size;
};


namespace swissknife {

class CommandCreate : public Command<CommandCreate> {
 public:
  CommandCreate(const std::string &param) : Command(param) {}
  ~CommandCreate() { };
  static std::string GetName() { return "create"; };
  static std::string GetDescription() {
    return "Bootstraps a fresh repository.";
  };
  static ParameterList GetParameters() {
    ParameterList r;
    r.push_back(Parameter::Mandatory('o', "manifest output file"));
    r.push_back(Parameter::Mandatory('t', "directory for temporary storage"));
    r.push_back(Parameter::Mandatory('r', "spooler definition"));
    r.push_back(Parameter::Optional ('l', "log level (0-4, default: 2)"));
    r.push_back(Parameter::Optional ('a', "hash algorithm (default: SHA-1)"));
    r.push_back(Parameter::Switch   ('v', "repository containing volatile files"));
    return r;
  }
  int Run(const ArgumentList &args);
};


class CommandUpload : public Command<CommandUpload> {
 public:
  CommandUpload(const std::string &param) : Command(param) {}
  ~CommandUpload() { };
  static std::string GetName() { return "upload"; };
  static std::string GetDescription() {
    return "Uploads a local file to the repository.";
  };
  static ParameterList GetParameters() {
    ParameterList r;
    r.push_back(Parameter::Mandatory('i', "local file"));
    r.push_back(Parameter::Mandatory('o', "destination path"));
    r.push_back(Parameter::Mandatory('r', "spooler definition"));
    r.push_back(Parameter::Optional ('a', "hash algorithm (default: SHA-1)"));
    return r;
  }
  int Run(const ArgumentList &args);
};


class CommandPeek : public Command<CommandPeek> {
public:
  CommandPeek(const std::string &param) : Command(param) {}
  ~CommandPeek() { };
  static std::string GetName() { return "peek"; };
  static std::string GetDescription() {
    return "Checks whether a file exists in the repository.";
  };
  static ParameterList GetParameters() {
    ParameterList r;
    r.push_back(Parameter::Mandatory('d', "destination path"));
    r.push_back(Parameter::Mandatory('r', "spooler definition"));
    return r;
  }
  int Run(const ArgumentList &args);
};


class CommandRemove : public Command<CommandRemove> {
 public:
  CommandRemove(const std::string &param) : Command(param) {}
  ~CommandRemove() { };
  static std::string GetName() { return "remove"; };
  static std::string GetDescription() {
    return "Removes a file in the repository storage.";
  };
  static ParameterList GetParameters() {
    ParameterList r;
    r.push_back(Parameter::Mandatory('o', "path to file"));
    r.push_back(Parameter::Mandatory('r', "spooler definition"));
    return r;
  }
  int Run(const ArgumentList &args);
};


class CommandSync : public Command<CommandSync> {
 public:
  CommandSync(const std::string &param) : Command(param) {}
  ~CommandSync() { };
  static std::string GetName() { return "sync"; };
  static std::string GetDescription() {
    return "Pushes changes from scratch area back to the repository.";
  };
  static ParameterList GetParameters() {
    ParameterList r;
    r.push_back(Parameter::Mandatory('u', "union volume"));
    r.push_back(Parameter::Mandatory('s', "scratch directory"));
    r.push_back(Parameter::Mandatory('c', "r/o volume"));
    r.push_back(Parameter::Mandatory('t', "directory for tee"));
    r.push_back(Parameter::Mandatory('b', "base hash"));
    r.push_back(Parameter::Mandatory('w', "stratum 0 base url"));
    r.push_back(Parameter::Mandatory('o', "manifest output file"));
    r.push_back(Parameter::Mandatory('r', "spooler definition"));
    r.push_back(Parameter::Switch   ('n', "create new repository"));
    r.push_back(Parameter::Switch   ('x', "print change set"));
    r.push_back(Parameter::Switch   ('y', "dry run"));
    r.push_back(Parameter::Switch   ('m', "create micro catalogs"));
    r.push_back(Parameter::Switch   ('i', "ignore x-directory hardlinks"));
    r.push_back(Parameter::Switch   ('p', "enable file chunking"));
    r.push_back(Parameter::Optional ('z', "log level (0-4, default: 2)"));
    r.push_back(Parameter::Optional ('a', "desired average chunk size in bytes"));
    r.push_back(Parameter::Optional ('l', "minimal file chunk size in bytes"));
    r.push_back(Parameter::Optional ('h', "maximal file chunk size in bytes"));
    r.push_back(Parameter::Optional ('f', "union filesystem type"));
    r.push_back(Parameter::Optional ('e', "hash algorithm (default: SHA-1)"));
    return r;
  }
  int Run(const ArgumentList &args);

 protected:
  bool ReadFileChunkingArgs(const swissknife::ArgumentList &args,
                            SyncParameters &params);
  bool CheckParams(const SyncParameters &p);
};

}  // namespace swissknife

#endif  // CVMFS_SWISSKNIFE_SYNC_H_
