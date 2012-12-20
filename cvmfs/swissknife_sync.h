/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SWISSKNIFE_SYNC_H_
#define CVMFS_SWISSKNIFE_SYNC_H_

#include <string>
#include "swissknife.h"
#include "upload.h"

struct SyncParameters {
  SyncParameters() {
    print_changeset = false;
    dry_run = false;
    mucatalogs = false;
    spooler = NULL;
  }

  upload::AbstractSpooler *spooler;
	std::string              dir_union;
  std::string              dir_scratch;
	std::string              dir_rdonly;
  std::string              dir_temp;
  std::string              base_hash;
  std::string              stratum0;
  std::string              manifest_path;
  std::string              spooler_definition;
	bool                     print_changeset;
	bool                     dry_run;
	bool                     mucatalogs;
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
    result.push_back(Parameter('z', "log level (0-4, default: 2)",
                               true, false));
    return result;
  }
  int Main(const ArgumentList &args);
};

}  // namespace swissknife

#endif  // CVMFS_SWISSKNIFE_SYNC_H_
