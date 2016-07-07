/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_CATALOG_BALANCER_IMPL_H_
#define CVMFS_CATALOG_BALANCER_IMPL_H_


#include <inttypes.h>

#include <cassert>
#include <string>
#include <vector>

#include "catalog_mgr.h"
#include "directory_entry.h"
#include "hash.h"
#include "logging.h"


using namespace std;  // NOLINT

namespace catalog {

template <class CatalogMgrT>
DirectoryEntryBase
CatalogBalancer<CatalogMgrT>::MakeEmptyDirectoryEntryBase(
    string name, uid_t uid, gid_t gid) {
  shash::Algorithms algorithm = catalog_mgr_->spooler_->GetHashAlgorithm();
  shash::Any file_hash(algorithm);
  shash::HashString("x\x9c\x03\x00\x00\x00\x00\x01", &file_hash);

  DirectoryEntryBase deb;
  deb.name_ = NameString(name);
  deb.mode_ = S_IFREG | S_IRUSR | S_IWUSR;
  deb.checksum_ = file_hash;
  deb.mtime_ = time(NULL);
  deb.uid_ = uid;
  deb.gid_ = gid;
  return deb;
}

template <class CatalogMgrT>
void CatalogBalancer<CatalogMgrT>::AddCatalogMarker(string path) {
  XattrList xattr;
  DirectoryEntry parent;
  bool retval;
  retval = catalog_mgr_->LookupPath(PathString(path), kLookupSole, &parent);
  assert(retval);
  DirectoryEntryBase cvmfscatalog =
      MakeEmptyDirectoryEntryBase(".cvmfscatalog", parent.uid(),
                                           parent.gid());
  DirectoryEntryBase cvmfsautocatalog =
      MakeEmptyDirectoryEntryBase(".cvmfsautocatalog", parent.uid(),
                                           parent.gid());
  string relative_path = path.substr(1);
  catalog_mgr_->AddFile(cvmfscatalog, xattr, relative_path);
  catalog_mgr_->AddFile(cvmfsautocatalog, xattr, relative_path);
}

template <class CatalogMgrT>
void CatalogBalancer<CatalogMgrT>::Balance(catalog_t *catalog) {
  if (catalog == NULL) {
    // obtain a copy of the catalogs
    vector<catalog_t*> catalogs = catalog_mgr_->GetCatalogs();
    // we need to reverse the catalog list in order to analyze the
    // last added ones first. This is necessary in the weird case the child
    // catalog's underflow provokes an overflow in the father
    reverse(catalogs.begin(), catalogs.end());
    for (unsigned i = 0; i < catalogs.size(); ++i)
      Balance(catalogs[i]);
    return;
  }
  string catalog_path = catalog->path().ToString();
  virtual_node_t root_node(catalog_path, catalog_mgr_);
  root_node.ExtractChildren(catalog_mgr_);
  // we have just recursively loaded the entire virtual tree!
  PartitionOptimally(&root_node);
}

template <class CatalogMgrT>
void CatalogBalancer<CatalogMgrT>::PartitionOptimally(
    virtual_node_t *virtual_node) {
  // postorder track of the fs-tree
  for (unsigned i = 0; i < virtual_node->children.size(); ++i) {
    virtual_node_t *virtual_child =
        &virtual_node->children[i];
    if (virtual_child->IsDirectory() && !virtual_child->IsCatalog())
      PartitionOptimally(virtual_child);
  }
  virtual_node->FixWeight();
  while (virtual_node->weight > catalog_mgr_->balance_weight_) {
    virtual_node_t *heaviest_node =
        MaxChild(virtual_node);
    // we directly add a catalog in this node
    // TODO(molina) apply heuristics here
    if (heaviest_node != NULL &&
        heaviest_node->weight >= catalog_mgr_->min_weight_) {
      // the catalog now generated _cannot_ be overflowed because the tree is
      // being traversed in postorder, handling the lightest nodes first
      unsigned max_weight = heaviest_node->weight;
      AddCatalogMarker(heaviest_node->path);
      AddCatalog(heaviest_node);
      virtual_node->weight -= (max_weight - 1);
    } else {
      // there is no possibility for any this directory's children
      // to be a catalog
      LogCvmfs(kLogPublish, kLogStdout, "Couldn't create a new nested catalog"
        " in any subdirectory of '%s' even though"
        " currently it is overflowed", virtual_node->path.c_str());
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
  if (virtual_node->IsDirectory() &&
      !virtual_node->IsCatalog() &&
      !virtual_node->is_new_nested_catalog) {
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
  assert(child_node != NULL);
  string new_catalog_path = child_node->path.substr(1);
  catalog_mgr_->CreateNestedCatalog(new_catalog_path);
  child_node->weight = 1;
  child_node->is_new_nested_catalog = true;
  LogCvmfs(kLogPublish, kLogStdout, "Automatic creation of nested"
      " catalog in '%s'", child_node->path.c_str());
}

template <class CatalogMgrT>
void CatalogBalancer<CatalogMgrT>::VirtualNode::ExtractChildren(
    CatalogMgrT *catalog_mgr) {
  DirectoryEntryList direntlist;
  catalog_mgr->Listing(path, &direntlist);
  for (unsigned i = 0; i < direntlist.size(); ++i) {
    string child_path = path + "/" + direntlist[i].name().ToString();
    children.push_back(virtual_node_t(
        child_path, direntlist[i], catalog_mgr));
    weight += children[i].weight;
  }
}

/**
 * This function is called in the father node when one of its children has
 * changed its weight. This phenomenon only occurs when one of its children has
 * become a new autogenerated nested catalog, and its weight is now 1 (which
 * represents the sole DirectoryEntry of that directory).
 * However this is not propagated to the top or the bottom of the tree, but each
 * VirtualNode that represents a directory is responsible for calling it when
 * previous operations might have changed the weight of its children (and
 * consequently its own weight).
 * This function is also called the first time this VirtualNodeit is used to
 * set its weight to the actual value. Initially the weight of any VirtualNode
 * is always 1.
 */
template <class CatalogMgrT>
void CatalogBalancer<CatalogMgrT>::VirtualNode::FixWeight() {
  weight = 1;
  if (!IsCatalog() && IsDirectory()) {
    for (unsigned i = 0; i < children.size(); ++i) {
      weight += children[i].weight;
    }
  }
}

}  // namespace catalog



#endif  // CVMFS_CATALOG_BALANCER_IMPL_H_

