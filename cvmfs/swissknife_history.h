/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SWISSKNIFE_HISTORY_H_
#define CVMFS_SWISSKNIFE_HISTORY_H_

#include <string>
#include "swissknife.h"
#include "hash.h"
#include "util_concurrency.h"
#include "history.h"

namespace manifest {
  class Manifest;
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


class CommandTag : public Command {
 protected:
  typedef std::vector<history::History::Tag> TagList;

 protected:
  struct Environment {
    Environment(const std::string &repository_url,
                const std::string &tmp_path) :
      repository_url(repository_url), tmp_path(tmp_path),
      push_happened_(false) {}
    void PushHistoryCallback(const upload::SpoolerResult &result);

    const std::string              repository_url;
    const std::string              tmp_path;

    UnlinkGuard                    manifest_path;
    UniquePtr<manifest::Manifest>  manifest;
    UniquePtr<manifest::Manifest>  previous_manifest;
    UniquePtr<history::History>    history;
    UniquePtr<upload::Spooler>     spooler;
    UnlinkGuard                    history_path;

    Future<shash::Any>             pushed_history_hash_;
    bool                           push_happened_;
  };

 public:
  CommandTag() {};

 protected:
  void InsertCommonParameters(ParameterList &parameters);

  Environment* InitializeEnvironment(const ArgumentList &args,
                                     const bool read_write);
  bool CloseAndPublishHistory(Environment *environment);

  manifest::Manifest* FetchManifest(const std::string  &repository_url,
                                    const std::string  &repository_name,
                                    const std::string  &pubkey_path,
                                    const std::string  &trusted_certs,
                                    const shash::Any   &base_hash) const;
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


class CommandCreateTag : public CommandTag {
 public:
  std::string GetName() { return "tag_create"; }
  std::string GetDescription() {
    return "Create a tag for a specific snapshot.";
  }

  ParameterList GetParams();
  int Main(const ArgumentList &args);
};


class CommandRemoveTag : public CommandTag {
 public:
  std::string GetName() { return "tag_remove"; }
  std::string GetDescription() {
    return "Remove one or more tags.";
  }

  ParameterList GetParams();
  int Main(const ArgumentList &args);
};


class CommandListTags : public CommandTag {
 public:
  std::string GetName() { return "tag_list"; }
  std::string GetDescription() {
    return "List tags in the tag database.";
  }

  ParameterList GetParams();
  int Main(const ArgumentList &args);

 protected:
  std::string AddPadding(const std::string  &str,
                         const size_t        padding,
                         const bool          align_right = false,
                         const std::string  &fill_char = " ") const;

  void PrintHumanReadableList(const TagList &tags) const;
};


class CommandInfoTag : public CommandTag {
 public:
  std::string GetName() { return "tag_info"; }
  std::string GetDescription() {
    return "Obtain detailed information about a tag.";
  }

  ParameterList GetParams();
  int Main(const ArgumentList &args);

 protected:
  std::string HumanReadableFilesize(const size_t filesize) const;
  void PrintHumanReadableInfo(const history::History::Tag &tag) const;
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
