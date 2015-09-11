/**
 * This file is part of the CernVM file system.
 */


#ifndef CVMFS_CATALOG_MGR_RW_IMPL_H_
#define CVMFS_CATALOG_MGR_RW_IMPL_H_

#include "catalog_mgr_rw.h"

#include <inttypes.h>
#include <unistd.h>

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <string>
#include <vector>

#include "catalog_rw.h"
#include "directory_entry.h"
#include "hash.h"
#include "logging.h"
#include "manifest.h"
#include "smalloc.h"
#include "statistics.h"
#include "upload.h"
#include "util.h"



using namespace std;  // NOLINT

namespace catalog {

template <class CatalogMgrT>
DirectoryEntryBase
CatalogBalancer<CatalogMgrT>::CreateEmptyContentDirectoryEntryBase(
    string name, uid_t uid, gid_t gid) {
  shash::Algorithms algorithm = catalog_mgr_->spooler_->GetHashAlgorithm();
  shash::Any file_hash(algorithm);
  shash::HashString("", &file_hash);

  DirectoryEntryBase deb;
  deb.name_ = NameString(name);
  deb.mode_ = S_IFREG | S_IRWXU;
  deb.size_ = 0;
  deb.linkcount_ = 1;
  deb.checksum_ = file_hash;
  deb.has_xattrs_ = false;
  deb.mtime_ = time(NULL);
  deb.inode_ = catalog::DirectoryEntry::kInvalidInode;
  deb.parent_inode_ = catalog::DirectoryEntry::kInvalidInode;
  deb.uid_ = uid;
  deb.gid_ = gid;
  return deb;
}

template <class CatalogMgrT>
void CatalogBalancer<CatalogMgrT>::AddCvmfsCatalogFile(string path) {
  XattrList xattr;
  DirectoryEntry parent;
  catalog_mgr_->LookupPath(PathString(path), kLookupSole, &parent);
  DirectoryEntryBase cvmfscatalog =
      CreateEmptyContentDirectoryEntryBase(".cvmfscatalog", parent.uid(),
                                           parent.gid());
  DirectoryEntryBase cvmfsautocatalog =
      CreateEmptyContentDirectoryEntryBase(".cvmfsautocatalog", parent.uid(),
                                           parent.gid());
  catalog_mgr_->AddFile(cvmfscatalog, xattr, path);
  catalog_mgr_->AddFile(cvmfsautocatalog, xattr, path);
}

template <class CatalogMgrT>
void CatalogBalancer<CatalogMgrT>::Balance(catalog_t *catalog) {
  if (catalog == NULL) {
    vector<catalog_t*> catalogs = catalog_mgr_->GetCatalogs();
    for (unsigned i = 0; i < catalogs.size(); ++i)
      Balance(catalogs[i]);
    return;
  }
  string catalog_path = catalog->path().ToString();
  virtual_node_t root_node(catalog_path, catalog_mgr_);
  root_node.ExtractChildren(catalog_mgr_);
  // we have just recursively loaded the entire virtual tree!
  OptimalPartition(&root_node);
}

template <class CatalogMgrT>
void CatalogBalancer<CatalogMgrT>::OptimalPartition(
    virtual_node_t *virtual_node) {
  // postorder track of the fs-tree
  for (unsigned i = 0; i < virtual_node->children.size(); ++i) {
    virtual_node_t *virtual_child =
        &virtual_node->children[i];
    if (virtual_child->IsDirectory() && !virtual_child->IsCatalog())
      OptimalPartition(virtual_child);
  }
  virtual_node->CaltulateWeight();
  while (virtual_node->weight > catalog_mgr_->balance_weight_) {
    typename CatalogBalancer<CatalogMgrT>::VirtualNode *heaviest_node =
        MaxChild(virtual_node);
    // we directly add a catalog in this node
    // TODO(molina) apply heuristics here
    if (heaviest_node != NULL &&
        heaviest_node->weight >= catalog_mgr_->min_weight_) {
      AddCvmfsCatalogFile(heaviest_node->path + "/.cvmfscatalog");
      AddCatalog(heaviest_node);
      virtual_node->CaltulateWeight();
    } else {
      // there is no possibility for this directory to be a catalog
      break;
    }
  }
}

template <class CatalogMgrT>
typename CatalogBalancer<CatalogMgrT>::VirtualNode*
CatalogBalancer<CatalogMgrT>::MaxChild(
    virtual_node_t *virtual_node)
{
  virtual_node_t *max_child = NULL;
  unsigned max_weight = 0;
  if (virtual_node->IsDirectory() && !virtual_node->IsCatalog()) {
    for (unsigned i = 0; i < virtual_node->children.size(); ++i) {
      virtual_node_t *child = &virtual_node->children[i];
      if (child->IsDirectory() &&
          !child->IsCatalog() &&
          max_weight < child->weight) {
        max_weight = child->weight;
        max_child = child;
      }
    }
  }
  return max_child;
}

template <class CatalogMgrT>
void CatalogBalancer<CatalogMgrT>::AddCatalog(virtual_node_t *child_node) {
  if (child_node != NULL) {
    catalog_mgr_->CreateNestedCatalog(child_node->path);
    child_node->weight = 1;
    child_node->is_new_nested_catalog = true;
    LogCvmfs(kLogPublish, kLogStdout, "automatic creation of nested"
        " catalog in '%s'", child_node->path.c_str());
  }
}

template <class CatalogMgrT>
void CatalogBalancer<CatalogMgrT>::VirtualNode::ExtractChildren(
    CatalogMgrT *catalog_mgr) {
  DirectoryEntryList direntlist;
  catalog_mgr->Listing(path, &direntlist);
  for (unsigned i = 0; i < direntlist.size(); ++i) {
    string children_path = path + "/" + direntlist[i].name().ToString();
    children.push_back(virtual_node_t(
        children_path, direntlist[i], catalog_mgr));
    weight += children[i].weight;
  }
}

template <class CatalogMgrT>
void CatalogBalancer<CatalogMgrT>::VirtualNode::CaltulateWeight() {
  weight = 1;
  if (!IsCatalog() && IsDirectory()) {
    for (unsigned i = 0; i < children.size(); ++i) {
      weight += children[i].weight;
    }
  }
}

}  // namespace catalog

#endif  // CVMFS_CATALOG_MGR_RW_IMPL_H_
