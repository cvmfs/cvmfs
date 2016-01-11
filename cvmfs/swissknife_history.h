/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SWISSKNIFE_HISTORY_H_
#define CVMFS_SWISSKNIFE_HISTORY_H_

#include <string>
#include <vector>

#include "hash.h"
#include "history_sqlite.h"
#include "swissknife.h"
#include "util_concurrency.h"

namespace manifest {
class Manifest;
}

namespace catalog {
class Catalog;
class WritableCatalog;
}

namespace upload {
struct SpoolerDefinition;
struct SpoolerResult;
class Spooler;
}

namespace swissknife {

class CommandTag : public Command {
 public:
  static const std::string kHeadTag;
  static const std::string kHeadTagDescription;
  static const std::string kPreviousHeadTag;
  static const std::string kPreviousHeadTagDescription;

 protected:
  typedef std::vector<history::History::Tag> TagList;

 protected:
  struct Environment {
    Environment(const std::string &repository_url,
                const std::string &tmp_path) :
      repository_url(repository_url), tmp_path(tmp_path) {}

    const std::string              repository_url;
    const std::string              tmp_path;

    UnlinkGuard                    manifest_path;
    UniquePtr<manifest::Manifest>  manifest;
    UniquePtr<manifest::Manifest>  previous_manifest;
    UniquePtr<history::History>    history;
    UniquePtr<upload::Spooler>     spooler;
    UnlinkGuard                    history_path;
  };

 public:
  CommandTag() { }

 protected:
  void InsertCommonParameters(ParameterList *parameters);

  Environment* InitializeEnvironment(const ArgumentList &args,
                                     const bool read_write);
  bool CloseAndPublishHistory(Environment *environment);
  bool UploadCatalogAndUpdateManifest(Environment               *env,
                                      catalog::WritableCatalog  *catalog);
  void UploadClosure(const upload::SpoolerResult  &result,
                           Future<shash::Any>     *hash);

  bool UpdateUndoTags(Environment                  *env,
                      const history::History::Tag  &current_head_template,
                      const bool                    undo_rollback = false);

  bool FetchObject(const std::string    &repository_url,
                   const shash::Any     &object_hash,
                   const std::string    &destination_path) const;
  history::History* GetHistory(const manifest::Manifest  *manifest,
                               const std::string         &repository_url,
                               const std::string         &history_path,
                               const bool                 read_write) const;

  catalog::Catalog* GetCatalog(const std::string  &repository_url,
                               const shash::Any   &catalog_hash,
                               const std::string   catalog_path,
                               const bool          read_write) const;

  void PrintTagMachineReadable(const history::History::Tag &tag) const;

  std::string AddPadding(const std::string  &str,
                         const size_t        padding,
                         const bool          align_right = false,
                         const std::string  &fill_char = " ") const;

  bool IsUndoTagName(const std::string &tag_name) const;
};


//------------------------------------------------------------------------------


class CommandCreateTag : public CommandTag {
 public:
  std::string GetName() { return "tag_create"; }
  std::string GetDescription() {
    return "Create a tag for a specific snapshot.";
  }

  ParameterList GetParams();
  int Main(const ArgumentList &args);

 protected:
  shash::Any GetTagRootHash(Environment *env,
                            const std::string &root_hash_string) const;
  bool ManipulateTag(Environment                  *env,
                     const history::History::Tag  &tag_template,
                     const bool                    user_provided_hash);
  bool MoveTag(Environment                  *env,
               const history::History::Tag  &tag_template);
  bool CreateTag(Environment                  *env,
                 const history::History::Tag  &new_tag);
};


//------------------------------------------------------------------------------


class CommandRemoveTag : public CommandTag {
 public:
  std::string GetName() { return "tag_remove"; }
  std::string GetDescription() {
    return "Remove one or more tags.";
  }

  ParameterList GetParams();
  int Main(const ArgumentList &args);
};


//------------------------------------------------------------------------------


class CommandListTags : public CommandTag {
 public:
  std::string GetName() { return "tag_list"; }
  std::string GetDescription() {
    return "List tags in the tag database.";
  }

  ParameterList GetParams();
  int Main(const ArgumentList &args);

 protected:
  void PrintHumanReadableList(const TagList &tags) const;
  void PrintMachineReadableList(const TagList &tags) const;
};


//------------------------------------------------------------------------------


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


//------------------------------------------------------------------------------


class CommandRollbackTag : public CommandTag {
 public:
  std::string GetName() { return "tag_rollback"; }
  std::string GetDescription() {
    return "Rollback repository to a given tag.";
  }

  ParameterList GetParams();
  int Main(const ArgumentList &args);

 protected:
  void PrintDeletedTagList(const TagList &tags) const;
};


//------------------------------------------------------------------------------


class CommandEmptyRecycleBin : public CommandTag {
 public:
  std::string GetName() { return "tag_empty_bin"; }
  std::string GetDescription() {
    return "Empty the internal recycle bin of the history database.";
  }

  ParameterList GetParams();
  int Main(const ArgumentList &args);
};

}  // namespace swissknife

#endif  // CVMFS_SWISSKNIFE_HISTORY_H_
