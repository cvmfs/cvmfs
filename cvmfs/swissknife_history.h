/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SWISSKNIFE_HISTORY_H_
#define CVMFS_SWISSKNIFE_HISTORY_H_

#include <string>
#include <vector>

#include "crypto/hash.h"
#include "history_sqlite.h"
#include "swissknife.h"
#include "util/future.h"

namespace manifest {
class Manifest;
}

namespace catalog {
class Catalog;
class WritableCatalog;
}  // namespace catalog

namespace upload {
struct SpoolerDefinition;
struct SpoolerResult;
class Spooler;
}  // namespace upload

namespace swissknife {

class CommandTag : public Command {
 public:
  static const std::string kHeadTag;
  static const std::string kHeadTagDescription;
  static const std::string kPreviousHeadTag;
  static const std::string kPreviousHeadTagDescription;

  CommandTag() { }

 protected:
  typedef std::vector<history::History::Tag> TagList;
  typedef std::vector<history::History::Branch> BranchList;

  struct Environment {
    Environment(const std::string &repository_url, const std::string &tmp_path)
        : repository_url(repository_url), tmp_path(tmp_path) { }

    const std::string repository_url;
    const std::string tmp_path;

    UnlinkGuard manifest_path;
    UniquePtr<manifest::Manifest> manifest;
    UniquePtr<manifest::Manifest> previous_manifest;
    UniquePtr<history::History> history;
    UniquePtr<upload::Spooler> spooler;
    UnlinkGuard history_path;
  };


  Environment *InitializeEnvironment(const ArgumentList &args,
                                     const bool read_write);
  bool CloseAndPublishHistory(Environment *environment);
  bool UploadCatalogAndUpdateManifest(Environment *env,
                                      catalog::WritableCatalog *catalog);
  void UploadClosure(const upload::SpoolerResult &result,
                     Future<shash::Any> *hash);

  bool UpdateUndoTags(Environment *env,
                      const history::History::Tag &current_head_template,
                      const bool undo_rollback = false);

  // TODO(jblomer): replace by swissknife::Assistant
  bool FetchObject(const std::string &repository_url,
                   const shash::Any &object_hash,
                   const std::string &destination_path) const;
  history::History *GetHistory(const manifest::Manifest *manifest,
                               const std::string &repository_url,
                               const std::string &history_path,
                               const bool read_write) const;

  catalog::Catalog *GetCatalog(const std::string &repository_url,
                               const shash::Any &catalog_hash,
                               const std::string catalog_path,
                               const bool read_write) const;

  void PrintTagMachineReadable(const history::History::Tag &tag) const;

  std::string AddPadding(const std::string &str,
                         const size_t padding,
                         const bool align_right = false,
                         const std::string &fill_char = " ") const;

  bool IsUndoTagName(const std::string &tag_name) const;
};


//------------------------------------------------------------------------------


/**
 * If -a and -d are specified, removal of tags takes place before the new tag is
 * added.
 */
class CommandEditTag : public CommandTag {
 public:
  virtual std::string GetName() const { return "tag_edit"; }
  virtual std::string GetDescription() const {
    return "Create a tag and/or remove tags.";
  }

  virtual ParameterList GetParams() const;
  int Main(const ArgumentList &args);

 protected:
  int RemoveTags(const ArgumentList &args, Environment *env);
  int AddNewTag(const ArgumentList &args, Environment *env);

  shash::Any GetTagRootHash(Environment *env,
                            const std::string &root_hash_string) const;
  bool ManipulateTag(Environment *env,
                     const history::History::Tag &tag_template,
                     const bool user_provided_hash);
  bool MoveTag(Environment *env, const history::History::Tag &tag_template);
  bool CreateTag(Environment *env, const history::History::Tag &new_tag);
};


//------------------------------------------------------------------------------


class CommandListTags : public CommandTag {
 public:
  virtual std::string GetName() const { return "tag_list"; }
  virtual std::string GetDescription() const {
    return "List tags in the tag database.";
  }

  virtual ParameterList GetParams() const;
  int Main(const ArgumentList &args);

 protected:
  struct BranchLevel {
    BranchLevel() : branch(), level(0) { }
    BranchLevel(const history::History::Branch &b, unsigned l)
        : branch(b), level(l) { }
    history::History::Branch branch;
    unsigned level;
  };
  typedef std::vector<BranchLevel> BranchHierarchy;

  void SortBranchesRecursively(unsigned level,
                               const std::string &parent_branch,
                               const BranchList &branches,
                               BranchHierarchy *hierarchy) const;
  BranchHierarchy SortBranches(const BranchList &branches) const;

  void PrintHumanReadableTagList(const TagList &tags) const;
  void PrintMachineReadableTagList(const TagList &tags) const;
  void PrintHumanReadableBranchList(const BranchHierarchy &branches) const;
  void PrintMachineReadableBranchList(const BranchHierarchy &branches) const;
};


//------------------------------------------------------------------------------


class CommandInfoTag : public CommandTag {
 public:
  virtual std::string GetName() const { return "tag_info"; }
  virtual std::string GetDescription() const {
    return "Obtain detailed information about a tag.";
  }

  virtual ParameterList GetParams() const;
  int Main(const ArgumentList &args);

 protected:
  std::string HumanReadableFilesize(const size_t filesize) const;
  void PrintHumanReadableInfo(const history::History::Tag &tag) const;
};


//------------------------------------------------------------------------------


class CommandRollbackTag : public CommandTag {
 public:
  virtual std::string GetName() const { return "tag_rollback"; }
  virtual std::string GetDescription() const {
    return "Rollback repository to a given tag.";
  }

  virtual ParameterList GetParams() const;
  int Main(const ArgumentList &args);

 protected:
  void PrintDeletedTagList(const TagList &tags) const;
};


//------------------------------------------------------------------------------


class CommandEmptyRecycleBin : public CommandTag {
 public:
  virtual std::string GetName() const { return "tag_empty_bin"; }
  virtual std::string GetDescription() const {
    return "Empty the internal recycle bin of the history database.";
  }

  virtual ParameterList GetParams() const;
  int Main(const ArgumentList &args);
};

}  // namespace swissknife

#endif  // CVMFS_SWISSKNIFE_HISTORY_H_
