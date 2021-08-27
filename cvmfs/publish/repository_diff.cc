/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "publish/repository.h"

#include <cassert>
#include <string>

#include "catalog_counters.h"
#include "catalog_diff_tool.h"
#include "catalog_mgr_ro.h"
#include "file_chunk.h"
#include "hash.h"
#include "history_sqlite.h"
#include "publish/except.h"
#include "shortstring.h"
#include "statistics.h"
#include "xattr.h"

namespace {

static history::History::Tag GetTag(const std::string &tag_name,
                                    const history::History &history)
{
  assert(!tag_name.empty());

  history::History::Tag tag;

  if (tag_name[0] == publish::Repository::kRawHashSymbol) {
    tag.name = tag_name.substr(1);
    tag.root_hash =
      shash::MkFromHexPtr(shash::HexPtr(tag.name), shash::kSuffixCatalog);
  } else {
    bool retval = history.GetByName(tag_name, &tag);
    if (!retval)
      throw publish::EPublish("unknown repository tag name: " + tag_name);
  }

  return tag;
}


class DiffForwarder : public CatalogDiffTool<catalog::SimpleCatalogManager> {
 private:
  publish::DiffListener *listener_;

 public:
  DiffForwarder(catalog::SimpleCatalogManager *old_mgr,
                catalog::SimpleCatalogManager *new_mgr,
                publish::DiffListener *listener)
    : CatalogDiffTool<catalog::SimpleCatalogManager>(old_mgr, new_mgr)
    , listener_(listener)
  {
  }
  virtual ~DiffForwarder() {}

  virtual void ReportAddition(const PathString& path,
                              const catalog::DirectoryEntry& entry,
                              const XattrList& /* xattrs */,
                              const FileChunkList& /* chunks */)
  {
    listener_->OnAdd(path.ToString(), entry);
  }

  virtual void ReportRemoval(const PathString& path,
                             const catalog::DirectoryEntry& entry)
  {
    listener_->OnRemove(path.ToString(), entry);
  }

  virtual bool ReportModification(const PathString& path,
                                  const catalog::DirectoryEntry& old_entry,
                                  const catalog::DirectoryEntry& new_entry,
                                  const XattrList& /*xattrs */,
                                  const FileChunkList& /* chunks */)
  {
    listener_->OnModify(path.ToString(), old_entry, new_entry);
    return true;
  }
};  // class DiffForwarder

}  // anonymous namespace

namespace publish {

void Repository::Diff(const std::string &from, const std::string &to,
                      DiffListener *diff_listener)
{
  history::History::Tag from_tag = GetTag(from, *history_);
  history::History::Tag to_tag = GetTag(to, *history_);
  diff_listener->OnInit(from_tag, to_tag);

  perf::Statistics stats_from;
  catalog::SimpleCatalogManager *mgr_from = new catalog::SimpleCatalogManager(
    from_tag.root_hash,
    settings_.url(),
    settings_.tmp_dir(),
    download_mgr_,
    &stats_from,
    true /* manage_catalog_files */);
  mgr_from->Init();

  perf::Statistics stats_to;
  catalog::SimpleCatalogManager *mgr_to = new catalog::SimpleCatalogManager(
    to_tag.root_hash,
    settings_.url(),
    settings_.tmp_dir(),
    download_mgr_,
    &stats_to,
    true /* manage_catalog_files */);
  mgr_to->Init();

  catalog::Counters counters_from = mgr_from->GetRootCatalog()->GetCounters();
  catalog::Counters counters_to = mgr_to->GetRootCatalog()->GetCounters();
  diff_listener->OnStats(catalog::Counters::Diff(counters_from, counters_to));

  // DiffTool takes ownership of the catalog managers
  DiffForwarder diff_forwarder(mgr_from, mgr_to, diff_listener);
  if (!diff_forwarder.Init())
    throw EPublish("cannot initialize difference engine");
  diff_forwarder.Run(PathString());
}

}  // namespace publish
