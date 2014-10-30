/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SWISSKNIFE_HISTORY_H_
#define CVMFS_SWISSKNIFE_HISTORY_H_

#include <string>
#include "swissknife.h"
#include "hash.h"
#include "util_concurrency.h"

namespace manifest {
  class Manifest;
}

namespace history {
  class History;
}

namespace catalog {
  class Catalog;
  class WritableCatalog;
}

namespace upload {
  class SpoolerDefinition;
  class SpoolerResult;
  class Spooler;
}

namespace swissknife {


class CommandTag_ : public Command {
 protected:
  struct Environment {
    Environment(const std::string &manifest_path,
                const std::string &repository_url,
                const std::string &tmp_path) :
      manifest_path(manifest_path), repository_url(repository_url),
      tmp_path(tmp_path), push_happened_(false) {}
    void PushHistoryCallback(const upload::SpoolerResult &result);

    const std::string              manifest_path;
    const std::string              repository_url;
    const std::string              tmp_path;

    UniquePtr<manifest::Manifest>  manifest;
    UniquePtr<history::History>    history;
    UniquePtr<upload::Spooler>     spooler;
    UnlinkGuard                    history_path;

    Future<shash::Any>             pushed_history_hash_;
    bool                           push_happened_;
  };

 public:
  CommandTag_() {};

 protected:
  void InsertCommonParameters(ParameterList &parameters);

  Environment* InitializeEnvironment(const ArgumentList &args,
                                     const bool read_write);
  bool CloseAndPublishHistory(Environment *environment);

  manifest::Manifest* FetchManifest(
                                const std::string &repository_url,
                                const std::string &repository_name,
                                const shash::Any  &expected_root_catalog) const;
  bool FetchObject(const std::string  &repository_url,
                   const shash::Any   &object_hash,
                   const std::string  &hash_suffix,
                   const std::string   destination_path) const;
  history::History* GetHistory(const manifest::Manifest  *manifest,
                               const std::string         &repository_url,
                               const std::string         &history_path,
                               const bool                 read_write) const;

  catalog::Catalog* GetCatalog(const std::string  &repository_url,
                               const shash::Any   &catalog_hash,
                               const std::string   catalog_path,
                               const bool          read_write) const;

  shash::Any PushHistory(const upload::SpoolerDefinition &spooler_definition,
                         const std::string &history_path);
};


class CommandCreateTag : public CommandTag_ {
 public:
  std::string GetName() { return "tag_create"; }
  std::string GetDescription() {
    return "Create a tag for a specific snapshot.";
  }

  ParameterList GetParams();
  int Main(const ArgumentList &args);
};


class CommandRemoveTag : public Command {
 public:
  std::string GetName() { return "tag_remove"; }
  std::string GetDescription() {
    return "Remove a specific tag.";
  }

  ParameterList GetParams();
  int Main(const ArgumentList &args);
};


class CommandListTags : public Command {
 public:
  std::string GetName() { return "tags_list"; }
  std::string GetDescription() {
    return "Remove a specific tag.";
  }

  ParameterList GetParams();
  int Main(const ArgumentList &args);
};






class CommandTag : public Command {
 public:
  ~CommandTag() { };
  std::string GetName() { return "tag"; };
  std::string GetDescription() {
    return "Tags a snapshot.";
  };
  ParameterList GetParams() {
    ParameterList r;
    r.push_back(Parameter::Mandatory('r', "repository directory / url"));
    r.push_back(Parameter::Mandatory('b', "base hash"));
    r.push_back(Parameter::Mandatory('t', "trunk hash"));
    r.push_back(Parameter::Mandatory('s', "trunk catalog size"));
    r.push_back(Parameter::Mandatory('i', "trunk revision"));
    r.push_back(Parameter::Mandatory('n', "repository name"));
    r.push_back(Parameter::Mandatory('k', "repository public key"));
    r.push_back(Parameter::Mandatory('o', "history db output file"));
    r.push_back(Parameter::Optional ('d', "delete a tag"));
    r.push_back(Parameter::Optional ('a', "add a tag (format: \"name@channel@desc\")"));
    r.push_back(Parameter::Optional ('h', "tag hash (if different from trunk)"));
    r.push_back(Parameter::Switch   ('l', "list tags"));
    r.push_back(Parameter::Optional ('z', "trusted certificate dir(s)"));

    return r;
  }
  int Main(const ArgumentList &args);
};


class CommandRollback : public Command {
 public:
  ~CommandRollback() { };
  std::string GetName() { return "rollback"; };
  std::string GetDescription() {
    return "Re-publishes a previous tagged snapshot.  All intermediate "
           "snapshots become inaccessible.";
  };
  ParameterList GetParams() {
    ParameterList r;
    r.push_back(Parameter::Mandatory('r', "spooler definition"));
    r.push_back(Parameter::Mandatory('u', "repository directory / url"));
    r.push_back(Parameter::Mandatory('b', "base hash"));
    r.push_back(Parameter::Mandatory('n', "repository name"));
    r.push_back(Parameter::Mandatory('k', "repository public key"));
    r.push_back(Parameter::Mandatory('o', "history db output file"));
    r.push_back(Parameter::Mandatory('m', "manifest output file"));
    r.push_back(Parameter::Mandatory('t', "revert to this tag"));
    r.push_back(Parameter::Mandatory('d', "temp directory"));
    r.push_back(Parameter::Optional ('z', "trusted certificate dir(s)"));
    return r;
  }
  int Main(const ArgumentList &args);
};

}  // namespace swissknife

#endif  // CVMFS_SWISSKNIFE_HISTORY_H_
