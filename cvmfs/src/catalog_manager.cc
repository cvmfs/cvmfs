#include "catalog_manager.h"

#include <map>
#include <errno.h>
#include <assert.h>
#include <iostream>
#include <fstream>

#include "util.h"
#include "cache.h"
#include "signature.h"
#include "lru.h"

extern "C" {
  #include "debug.h"
  #include "log.h"
  #include "compression.h"
  #include "http_curl.h"
}

using namespace std;

namespace cvmfs {
  
CatalogManager::CatalogManager(const string &root_url, const string &repo_name, const std::string &whitelist, const std::string &blacklist, const bool force_signing) {
  root_url_ = root_url;
  repo_name_ = repo_name;
  whitelist_ = whitelist;
  blacklist_ = blacklist;
  force_signing_ = force_signing;
  current_inode_offset_ = CatalogManager::kInitialInodeOffset;

  atomic_init(&certificate_hits_);
  atomic_init(&certificate_misses_);
}

CatalogManager::~CatalogManager() {
  
}

bool CatalogManager::LoadAndAttachCatalog(const string &mountpoint, Catalog *parent_catalog, Catalog **attached_catalog) {  
  string new_catalog_file;
  if (0 != LoadCatalogFile(mountpoint, hash::t_md5(mountpoint), &new_catalog_file)) {
    pmesg(D_CATALOG, "failed to load catalog %s", mountpoint.c_str());
    return false;
  }
  
  if (not AttachCatalog(new_catalog_file, mountpoint, parent_catalog, false, attached_catalog)) {
    pmesg(D_CATALOG, "failed to attach catalog %s", mountpoint.c_str());
    return false;
  }
  
  return true;
}

bool CatalogManager::LoadAndAttachRootCatalog() {
  return LoadAndAttachCatalog("", NULL);
}

bool CatalogManager::AttachCatalog(const std::string &db_file, const std::string &url, Catalog *parent, const bool open_transaction, Catalog **attached_catalog) {
  pmesg(D_CATALOG, "attaching catalog file %s", db_file.c_str());
  
  Catalog *new_catalog = new Catalog(url, parent);
  if (not new_catalog->Init(db_file, this)) {
    pmesg(D_CATALOG, "initialization of catalog %s failed", db_file.c_str());
    return false;
  }
  
  catalogs_.push_back(new_catalog);
  if (NULL != attached_catalog) *attached_catalog = new_catalog;
  return true;
}

bool CatalogManager::RefreshCatalog() {
  
  return true;
}

bool CatalogManager::DetachCatalog() {
  
  return true;
}

/**
 *  currently this is a very simple approach
 *  just assign the next free numbers in this 64 bit space
 *  TODO: think about other allocation methods, this may run out
 *        of free inodes some time (in the late future admittedly)
 */
uint64_t CatalogManager::GetInodeChunkOfSize(uint64_t size) {
  uint64_t result = current_inode_offset_;
  current_inode_offset_ = current_inode_offset_ + size;
  pmesg(D_CATALOG, "allocating inodes from %d to %d.", result, current_inode_offset_);
  
  return result;
}

bool CatalogManager::Lookup(const inode_t inode, DirectoryEntry *entry, const bool with_parent) const {
  // get appropriate catalog
  Catalog *catalog;
  bool found_catalog = GetCatalogByInode(inode, &catalog);
  if (not found_catalog) {
    pmesg(D_CATALOG, "cannot find catalog for inode %d", inode);
    return false;
  }
  
  // if we are not asked to lookup the parent inode or if we are
  // asked for the root inode (which of course has no parent)
  // we simply look in the best suited catalog and are done
  if (not with_parent || inode == GetRootInode()) {
    return catalog->Lookup(inode, entry);
  }
  
  // to lookup the parent of this entry, we obtain the parent
  // md5 hash and make potentially two lookups, first in the
  // previously found catalog and second its parent catalog
  else {
    hash::t_md5 parent_hash;
    DirectoryEntry parent;
    bool found_entry = catalog->Lookup(inode, entry, &parent_hash);
    
    // entry was not found in the first place... no parent lookup
    if (not found_entry) {
      return false;
    }
    
    // look for the parent entry in the same catalog
    bool found_parent_entry = false;
    found_parent_entry = catalog->Lookup(parent_hash, &parent);
    
    // if the entry was not found and there is a parent catalog
    // we also check this one
    if (not found_parent_entry && not catalog->IsRoot()) {
      Catalog *parent_catalog = catalog->parent();
      found_parent_entry = parent_catalog->Lookup(parent_hash, &parent);
    }
    
    // if we still lack a parent entry, there may be some data corruption!
    if (not found_parent_entry) {
      pmesg(D_CATALOG, "cannot find parent entry for inode %d --> data corrupt?", inode);
      return false;
    }
    
    // all set
    entry->set_parent_inode(parent.inode());
    return true;
  }
}

bool CatalogManager::Lookup(const string &path, DirectoryEntry *entry, const bool with_parent) {
  bool found = false;
  found = GetCatalogByPath(path, false, NULL, entry);
  
  if (not found) {
    return false;
  }
  
  if (with_parent) {
    string parent_path = get_parent_path(path);
    DirectoryEntry parent;
    found = LookupWithoutParent(parent_path, &parent);
    if (not found) {
      pmesg(D_CATALOG, "cannot find parent '%s' for entry '%s' --> data corrupt?", parent_path.c_str(), path.c_str());
      return false;
    }
    
    entry->set_parent_inode(parent.inode());
  }
  
  return true;
}

bool CatalogManager::Listing(const string &path, DirectoryEntryList *result) {
  Catalog *catalog;
  bool found_catalog = GetCatalogByPath(path, true, &catalog);
  
  if (not found_catalog) {
    return false;
  }
  
  return catalog->Listing(path, result);
}

bool CatalogManager::GetCatalogByPath(const string &path, const bool load_final_catalog, Catalog **catalog, DirectoryEntry *entry) {
  // find the best fitting loaded catalog for this path
  Catalog *best_fitting_catalog = FindBestFittingCatalogForPath(path);
  assert (best_fitting_catalog != NULL);

  // path lookup in this catalog
  pmesg(D_CATALOG, "looking up %s in catalog: %s", path.c_str(), best_fitting_catalog->path().c_str());
  DirectoryEntry d;
  bool entry_found = best_fitting_catalog->Lookup(path, &d);
  
  // if we did not find the entry, there are two possible reasons:
  //    1. the entry in question resides in a not yet loaded nested catalog
  //    2. the entry does not exist at all
  if (not entry_found) {
    pmesg(D_CATALOG, "entry not found, we may have to load nested catalogs");
    
    // try to load the nested catalogs for this path
    Catalog *nested_catalog;
    entry_found = LoadNestedCatalogForPath(path, best_fitting_catalog, load_final_catalog, &nested_catalog);
    if (not entry_found) {
      pmesg(D_CATALOG, "nested catalog for %s could not be found", path.c_str());
      return false;
    
    // retry the lookup with the nested catalog
    } else {
      entry_found = nested_catalog->Lookup(path, &d);
      if (not entry_found) {
        pmesg(D_CATALOG, "nested catalogs loaded but entry %s was still not found", path.c_str());
        return false;
      } else {
        best_fitting_catalog = nested_catalog;
      }
    }
  }

  // if the found entry is a nested catalog mount point we have to load in on request
  else if (load_final_catalog && d.IsNestedCatalogMountpoint()) {
    Catalog *new_catalog;
    bool attached_successfully = LoadAndAttachCatalog(path, best_fitting_catalog, &new_catalog);
    if (not attached_successfully) {
      return false;
    }
    best_fitting_catalog = new_catalog;
  }
  
  pmesg(D_CATALOG, "found entry %s in catalog %s", path.c_str(), best_fitting_catalog->path().c_str());
  if (NULL != catalog) *catalog = best_fitting_catalog;
  if (NULL != entry)   *entry = d;
  return true;
}

bool CatalogManager::GetCatalogByInode(const uint64_t inode, Catalog **catalog) const {
  // TODO: replace this with a more clever algorithm
  //       maybe exploit the ordering in the vector
  CatalogVector::const_iterator i,end;
  for (i = catalogs_.begin(), end = catalogs_.end(); i != end; ++i) {
    if ((*i)->ContainsInode(inode)) {
      *catalog = *i;
      return true;
    }
  }
  
  // inode was not found... might be data corruption
  return false;
}

Catalog* CatalogManager::FindBestFittingCatalogForPath(const string &path) const {
  // go and find the best fit in open catalogs for this path
  Catalog *best_fit = GetRootCatalog();
  while (best_fit->path() != path) {
    // now go through all children of the current best fit and look for better fits
    unsigned int longest_hit = 0;
    Catalog *next_best_fit = NULL;
    CatalogVector children = best_fit->children();
    CatalogVector::const_iterator i,end;
    string child_path;
    for (i = children.begin(), end = children.end(); i != end; ++i) {
      child_path = (*i)->path();
      
      // sort out the best fitting child and continue
      if (path.find(child_path) == 0 && longest_hit < child_path.length()) {
        next_best_fit = *i;
        longest_hit = child_path.length();
        
        // quick way out if we found the right catalog
        if (longest_hit == path.length()) {
          break;
        }
      }
    }

    // continue with the best fitting child or break
    if (next_best_fit != NULL) {
      best_fit = next_best_fit;
    } else {
      break;
    }
  }
  
  return best_fit;
}

bool CatalogManager::LoadNestedCatalogForPath(const string &path, const Catalog *entry_point, const bool load_final_catalog, Catalog **final_catalog) {
  std::vector<string> path_elements;
  string sub_path, relative_path;
  Catalog *containing_catalog = (entry_point == NULL) ? GetRootCatalog() : (Catalog *)entry_point;
  
  // do all processing relative to the entry_point catalog
  assert (path.find(containing_catalog->path()) == 0);
  relative_path = path.substr(containing_catalog->path().length() + 1); // +1 --> remove slash '/' from beginning relative path
  sub_path = containing_catalog->path();
  
  path_elements = split_string(relative_path, "/");
  bool entry_found;
  DirectoryEntry entry;

  // step through the path and attach nested catalogs on the way
  // TODO: this might be faster with the 'nested catalog table'
  std::vector<string>::const_iterator i,end;
  for (i = path_elements.begin(), end = path_elements.end(); i != end; ++i) {
    sub_path += "/" + *i;
    
    entry_found = containing_catalog->Lookup(sub_path, &entry);
    if (not entry_found) {
      return false;
    }
    
    if (entry.IsNestedCatalogMountpoint()) {
      // nested catalogs on the way are downloaded
      // if load_final_catalog is false we do not download a nested
      // catalog pointed to by the whole path
      if (sub_path.length() < path.length() || load_final_catalog) {
        Catalog *new_catalog;
        bool attached_successfully = LoadAndAttachCatalog(sub_path, containing_catalog, &new_catalog);
        if (not attached_successfully) {
          return false;
        }
        containing_catalog = new_catalog;
      }
    }
  }
  
  *final_catalog = containing_catalog;
  return true;
}

int CatalogManager::LoadCatalogFile(const string &url_path, const hash::t_md5 &mount_point, 
                                    const int existing_cat_id, const bool no_cache,
                                    const hash::t_sha1 expected_clg, std::string *catalog_file)
{
  string old_file;
  hash::t_sha1 sha1_old;
  hash::t_sha1 sha1_cat;
  bool cached_copy;
  int cat_id = existing_cat_id;
  
  int result = FetchCatalog(url_path, no_cache, mount_point, 
                            *catalog_file, sha1_cat, old_file, sha1_old, cached_copy, expected_clg);
  if (((result == -EPERM) || (result == -EAGAIN) || (result == -EINVAL)) && !no_cache) {
     /* retry with no-cache pragma */
     pmesg(D_CVMFS, "could not load catalog, trying again with pragma: no-cache");
     logmsg("possible data corruption while trying to retrieve catalog from %s, trying with no-cache",
            (root_url_ + url_path).c_str());
     result = FetchCatalog(url_path, true, mount_point, 
                           *catalog_file, sha1_cat, old_file, sha1_old, cached_copy, expected_clg);
  }
  /* log certain failures */
  if (result == -EPERM) {
     logmsg("signature verification failure while trying to retrieve catalog from %s", 
            (root_url_ + url_path).c_str());
  }
  else if ((result == -EINVAL) || (result == -EAGAIN)) {
     logmsg("data corruption while trying to retrieve catalog from %s",
            (root_url_ + url_path).c_str());
  }
  else if (result < 0) {
     logmsg("catalog load failure while try to retrieve catalog from %s", 
            (root_url_ + url_path).c_str());
  }
  
  /* LRU handling, could still fail due to cache size restrictions */
  if (((result == 0) && !cached_copy) ||
      ((existing_cat_id < 0) && ((result == 0) || cached_copy)))
  {
     PortableStat64 info;
     if (portableFileStat64(catalog_file->c_str(), &info) != 0) {
        /* should never happen */
        lru::remove(sha1_cat);
        cached_copy = false;
        result = -EIO;
        pmesg(D_CVMFS, "failed to access new catalog");
        logmsg("catalog access failure for %s", catalog_file->c_str());
     } else {
        if (((uint64_t)info.st_size > lru::max_file_size()) ||
            (!lru::pin(sha1_cat, info.st_size, root_url_ + url_path)))
        {
           pmesg(D_CVMFS, "failed to store %s in LRU cache (no space)", catalog_file->c_str());
           logmsg("catalog load failure for %s (no space)", catalog_file->c_str());
           lru::remove(sha1_cat);
           unlink(catalog_file->c_str());
           cached_copy = false;
           result = -ENOSPC;
        } else {
           /* From now on we have to go with the new catalog */
           if (!sha1_old.is_null() && (sha1_old != sha1_cat)) {
              lru::remove(sha1_old);
              unlink(old_file.c_str());
           }
        }
     }
  }
  
  return result;
}

/**
* Uses fetch catalog to get a possibly new catalog version.
* Old catalog has to be detached afterwards.
* Updates LRU database and TTL list.
* \return 0 on success (also cached copy is success), standard error code else
*/
// int CatalogManager::LoadAndAttachCatalog(const string &url_path, const hash::t_md5 &mount_point, 
//                                          const string &mount_path, const int existing_cat_id, const bool no_cache,
//                                          const hash::t_sha1 expected_clg)
// {
// //  LoadCatalog(url_path)
//   
//   time_t now = time(NULL);
//   
//   /* Now we have the right catalog in cat_file, which might be
//         already loaded (cache_copy and existing_cat_id > 0) */
//   if (((result == 0) && !cached_copy) ||
//       ((existing_cat_id < 0) && ((result == 0) || cached_copy))) 
//   {
//      bool attach_result;
//      if (existing_cat_id >= 0) {
//        attach_result = Reattach(existing_cat_id, cat_file, url_path, NULL);
//         /*
//         
//         TODO: reimplement this
//         
//         catalog::detach_intermediate(existing_cat_id);
//         attach_result = catalog::reattach(existing_cat_id, cat_file, url_path);
//         catalog_tree::get_catalog(existing_cat_id)->last_changed = now;
//         catalog_tree::get_catalog(existing_cat_id)->snapshot = sha1_cat;
//         */
//      } else {
//        attach_result = Attach(cat_file, url_path, NULL, false, NULL);
//      }
//      
//      /* Also for existing_cat_id < 0 to remove the "nested" flags from cache */
// // TODO: reimplement this
// //     invalidate_cache(existing_cat_id);
//         
//      if (!attach_result) {
//         /* should never happen, no reasonable continuation */
//         pmesg(D_CVMFS, "failed to attach new catalog");
//         logmsg("catalog attach failure for %s", cat_file.c_str());
//         abort();
//      } else {
//         if (existing_cat_id < 0) {
//            cat_id = catalog::get_num_catalogs()-1;
//         }
//      }
//   }
//   
//   /* Back-rename if we have a catalog at all.  No race condition with LRU
//      because file is pinned. */
//   if ((result == 0) || cached_copy) {
//      const string sha1_cat_str = sha1_cat.to_string();
//      const string final_file = "./" + sha1_cat_str.substr(0, 2) + "/" + 
//                                sha1_cat_str.substr(2);
//      (void)rename(cat_file.c_str(), final_file.c_str());
//   }
//   
//   if (cat_id >= 0) {
//      return 0;
//   } else {
//      return result;
//   }
// }

string CatalogManager::MakeFilesystemKey(string url) const {
  string::size_type pos;
  while ((pos = url.find(':', 0)) != string::npos) {
    url[pos] = '-';
  }
  while ((pos = url.find('/', 0)) != string::npos){
    url[pos] = '-';
  }
  return url;
}

// TODO: code from here on DOES NOT belong here
//       should be hidden in a FileManager class or something
//       currently this is here for convenience!!
//       see: https://cernvm.cern.ch/project/trac/cernvm/wiki/private/evolving-cvmfs


/**
* Loads a catalog from an url into local cache if there is a newer version.
* Catalogs are stored like data chunks.
* This funktions returns a temporary file that is not tampered with by LRU.
*
* We first download the checksum of the catalog to quickly see if anyting changed.
*
* The checksum can be signed by an X.509 certificate.  If so, we only load succeed
* only with a valid signature and a valid certificate. 
*
* @param[in] url, relative directory path starting from root_url
* @param[in] no_proxy, if true, fetch checksum and signature/whitelist with pragma: no-cache
* @param[in] mount_point, expected mount path (required for sanity check)
* @param[out] cat_file, file name of the catalog cache copy or the new catalog on success.
* @param[out] cat_sha1, sha1 value of the catalog returned by cat_file.
* @param[out] old_file, file name of the old catalog cache copy if new catalog is loaded.
* @param[out] old_sha1, sha1 value of the old catalog cache copy if new catalog is loaded.
* @param[out] cached_copy, indicates if a new catalog version was loaded.
* \return 0 on success, a standard error code else
*/
int CatalogManager::FetchCatalog(const string &url_path, const bool no_proxy, const hash::t_md5 &mount_point,
                                 string &cat_file, hash::t_sha1 &cat_sha1, string &old_file, hash::t_sha1 &old_sha1, 
                                 bool &cached_copy, const hash::t_sha1 &sha1_expected, const bool dry_run) {
  const string fskey = (repo_name_ == "") ? root_url_ : repo_name_;
  const string lpath_chksum = "./cvmfs.checksum." + MakeFilesystemKey(fskey + url_path);
  const string rpath_chksum = url_path + "/.cvmfspublished";
  bool have_cached = false;
  bool signature_ok = false;
  hash::t_sha1 sha1_download;
  hash::t_sha1 sha1_local;
  hash::t_sha1 sha1_chksum; /* required for signature verification */
  struct mem_url mem_url_chksum;
  struct mem_url mem_url_cert;
  map<char, string> chksum_keyval;
  int curl_result;
  int64_t local_modified;
  char *checksum = NULL;
  
  pmesg(D_CVMFS, "searching for filesystem at %s", (root_url_ + url_path).c_str());
  
  cached_copy = false;
  cat_file = old_file = "";
  old_sha1 = cat_sha1 = hash::t_sha1();
  local_modified = 0;
  
  /* load local checksum */
  pmesg(D_CVMFS, "local checksum file is %s", lpath_chksum.c_str());   
  FILE *fchksum = fopen(lpath_chksum.c_str(), "r");
  char tmp[40];
  if (fchksum && (fread(tmp, 1, 40, fchksum) == 40)) 
  {
     sha1_local.from_hash_str(string(tmp, 40));
     cat_file = "./" + string(tmp, 2) + "/" + string(tmp+2, 38);
     
     /* try to get local last modified time */
     char buf_modified;
     string str_modified;
     if ((fread(&buf_modified, 1, 1, fchksum) == 1) && (buf_modified == 'T')) {
        while (fread(&buf_modified, 1, 1, fchksum) == 1)
           str_modified += string(&buf_modified, 1);
        local_modified = atoll(str_modified.c_str());
        pmesg(D_CVMFS, "cached copy publish date %s", localtime_ascii(local_modified, true).c_str());
     } 

     /* Sanity check, do we have the catalog? If yes, save it to temporary file. */
     if (!dry_run) {
        if (rename(cat_file.c_str(), (cat_file + "T").c_str()) != 0) { 
           cat_file = "";
           unlink(lpath_chksum.c_str());
           pmesg(D_CVMFS, "checksum existed but no catalog with it");
        } else {
           cat_file += "T";
           old_file = cat_file;
           cat_sha1 = old_sha1 = sha1_local;
           have_cached = cached_copy = true;
           pmesg(D_CVMFS, "local checksum is %s", sha1_local.to_string().c_str());
        }
     } else {
        old_file = cat_file;
        cat_sha1 = old_sha1 = sha1_local;
        have_cached = cached_copy = true;
     }
  } else {
     pmesg(D_CVMFS, "unable to read local checksum");
  }
  if (fchksum) fclose(fchksum);
  
  /* load remote checksum */
  int sig_start = 0;
  if (sha1_expected == hash::t_sha1()) { 
     if (no_proxy) curl_result = curl_download_mem_nocache(rpath_chksum.c_str(), &mem_url_chksum, 1, 0);
     else curl_result = curl_download_mem(rpath_chksum.c_str(), &mem_url_chksum, 1, 0);
     if (curl_result != CURLE_OK) {
        if (mem_url_chksum.size > 0) free(mem_url_chksum.data); 
        pmesg(D_CVMFS, "unable to load checksum from %s (%d), going to offline mode", rpath_chksum.c_str(), curl_result);
        logmsg("unable to load checksum from %s (%d), going to offline mode", rpath_chksum.c_str(), curl_result);
        return -EIO;
     }
     checksum = (char *)alloca(mem_url_chksum.size);
     memcpy(checksum, mem_url_chksum.data, mem_url_chksum.size);
     free(mem_url_chksum.data);
  
     /* parse remote checksum */
     parse_keyval(checksum, mem_url_chksum.size, sig_start, sha1_chksum, chksum_keyval);

     map<char, string>::const_iterator clg_key = chksum_keyval.find('C');
     if (clg_key == chksum_keyval.end()) {
        pmesg(D_CVMFS, "failed to find catalog key in checksum");
        return -EINVAL;
     }
     sha1_download.from_hash_str(clg_key->second);
     pmesg(D_CVMFS, "remote checksum is %s", sha1_download.to_string().c_str());
  } else {
     sha1_download = sha1_expected;
  }
  
  /* short way out, use cached copy */
  if (have_cached) {
     if (sha1_download == sha1_local)
        return 0;
     
     /* Sanity check, last modified (if available, i.e. if signed) */
     map<char, string>::const_iterator published = chksum_keyval.find('T');
     if (published != chksum_keyval.end()) {
        if (local_modified > atoll(published->second.c_str())) {
           pmesg(D_CVMFS, "cached checksum newer than loaded checksum");
           logmsg("Cached copy of %s newer than remote copy", rpath_chksum.c_str());
           return 0;
        }
     }
  }
     
  if (sha1_expected == hash::t_sha1()) {
     /* Sanity check: repository name */
     if (repo_name_ != "") {
        map<char, string>::const_iterator name = chksum_keyval.find('N');
        if (name == chksum_keyval.end()) {
           pmesg(D_CVMFS, "failed to find repository name in checksum");
           return -EINVAL;
        }
        if (name->second != repo_name_) {
           pmesg(D_CVMFS, "expected repository name does not match");
           logmsg("Expected repository name does not match in %s", rpath_chksum.c_str());
           return -EINVAL;
        }
     }
  
  
     /* Sanity check: root prefix */
     map<char, string>::const_iterator root_prefix = chksum_keyval.find('R');
     if (root_prefix == chksum_keyval.end()) {
        pmesg(D_CVMFS, "failed to find root prefix in checksum");
        return -EINVAL;
     }
     if (root_prefix->second != mount_point.to_string()) {
        pmesg(D_CVMFS, "expected mount point does not match");
        logmsg("Expected mount point does not match in %s", rpath_chksum.c_str());
        return -EINVAL;
     }
  
     /* verify remote checksum signature, failure is handled like checksum could not be downloaded,
        except for error code -2 instead of -1. */
     void *sig_buf_heap;
     unsigned sig_buf_size;
     if ((sig_start > 0) &&
         read_sig_tail(checksum, mem_url_chksum.size, sig_start, 
                       &sig_buf_heap, &sig_buf_size)) 
     {
        void *sig_buf = alloca(sig_buf_size);
        memcpy(sig_buf, sig_buf_heap, sig_buf_size);
        free(sig_buf_heap);
     
        /* retrieve certificate */
        map<char, string>::const_iterator key_cert = chksum_keyval.find('X');
        if ((key_cert == chksum_keyval.end()) || (key_cert->second.length() < 40)) {
           pmesg(D_CVMFS, "invalid certificate in checksum");
           return -EINVAL;
        }
     
        bool cached_cert = false;
        hash::t_sha1 cert_sha1;
        cert_sha1.from_hash_str(key_cert->second.substr(0, 40));
     
        if (cache::disk_to_mem(cert_sha1, &mem_url_cert.data, &mem_url_cert.size)) {
           atomic_inc(&certificate_hits_);
           cached_cert = true;
        } else {
           atomic_inc(&certificate_misses_);
           cached_cert = false;

           const string url_cert = "/data/" + key_cert->second.substr(0, 2) + "/" + 
                                   key_cert->second.substr(2) + "X";
           if (no_proxy) curl_result = curl_download_mem_nocache(url_cert.c_str(), &mem_url_cert, 1, 1);
           else curl_result = curl_download_mem(url_cert.c_str(), &mem_url_cert, 1, 1);
           if (curl_result != CURLE_OK) {
              pmesg(D_CVMFS, "unable to load certificate from %s (%d)", url_cert.c_str(), curl_result);
              if (mem_url_cert.size > 0) free(mem_url_cert.data); 
              return -EAGAIN;
           }
        
           /* verify downloaded chunk */
           void *outbuf;
           size_t outsize;
           hash::t_sha1 verify_sha1;
           bool verify_result;
           if (compress_mem(mem_url_cert.data, mem_url_cert.size, &outbuf, &outsize) != 0) {
              verify_result = false;
           } else {
              sha1_mem(outbuf, outsize, verify_sha1.digest);
              free(outbuf);
              verify_result = (verify_sha1 == cert_sha1);
           }
           if (!verify_result) {
              pmesg(D_CVMFS, "data corruption for %s", url_cert.c_str());
              free(mem_url_cert.data);
              return -EAGAIN;
           }
        }
     
        /* read certificate */
        if (!signature::load_certificate(mem_url_cert.data, mem_url_cert.size, false)) {
           pmesg(D_CVMFS, "could not read certificate");
           free(mem_url_cert.data);
           return -EINVAL;
        }
           
        /* verify certificate and signature */
        if (!IsValidCertificate(no_proxy) ||
            !signature::verify(&((sha1_chksum.to_string())[0]), 40, sig_buf, sig_buf_size)) 
        {
           pmesg(D_CVMFS, "signature verification failed against %s", sha1_chksum.to_string().c_str());
           free(mem_url_cert.data);
           return -EPERM;
        }
        pmesg(D_CVMFS, "catalog signed by: %s", signature::whois().c_str());
        signature_ok = true;
     
        if (!cached_cert) {
           cache::mem_to_disk(cert_sha1, mem_url_cert.data, mem_url_cert.size, 
                              "certificate of " + signature::whois());
        }
        free(mem_url_cert.data);
     } else {
        pmesg(D_CVMFS, "remote checksum is not signed");
        if (force_signing_) {
           logmsg("Remote checksum %s is not signed", rpath_chksum.c_str());
           return -EPERM;
        }
     }
  }
  
  if (dry_run) {
     cat_sha1 = sha1_download;
     return 1;
  }
  
  /* load new catalog */
  const string tmp_file_template = "./cvmfs.catalog.XXXXXX";
  char *tmp_file = strdupa(tmp_file_template.c_str());
  int tmp_fd = mkstemp(tmp_file);
  if (tmp_fd < 0) return -EIO;
  FILE *tmp_fp = fdopen(tmp_fd, "w");
  if (!tmp_fp) {
     close(tmp_fd);
     unlink(tmp_file);
     return -EIO;
  }
  int retval;
  char strmbuf[4096];
  retval = setvbuf(tmp_fp, strmbuf, _IOFBF, 4096);
  assert(retval == 0);
  
  const string sha1_clg_str = sha1_download.to_string();
  const string url_clg = "/data/" + sha1_clg_str.substr(0, 2) + "/" +
                         sha1_clg_str.substr(2) + "C";
  if (no_proxy) curl_result = curl_download_stream_nocache(url_clg.c_str(), tmp_fp, sha1_local.digest, 1, 1);
  else curl_result = curl_download_stream(url_clg.c_str(), tmp_fp, sha1_local.digest, 1, 1);
  fclose(tmp_fp);
  if ((curl_result != CURLE_OK) || (sha1_local != sha1_download)) {
     pmesg(D_CVMFS, "unable to load catalog from %s, going to offline mode (%d)", url_clg.c_str(), curl_result);
     logmsg("unable to load catalog from %s, going to offline mode", url_clg.c_str());
     unlink(tmp_file);
     return -EAGAIN;
  }
  
  /* we have all bits and pieces, write checksum and catalog into cache directory */
  const string sha1_download_str = sha1_download.to_string();
  cat_file = tmp_file;
  cat_sha1 = sha1_download;
  cached_copy = false;

  int fdchksum = open(lpath_chksum.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0600);
  if (fdchksum >= 0) {
     string local_chksum = sha1_local.to_string();
     map<char, string>::const_iterator published = chksum_keyval.find('T');
     if (published != chksum_keyval.end())
        local_chksum += "T" + published->second;
        
     fchksum = fdopen(fdchksum, "w");
     if (fchksum) {
        if (fwrite(&(local_chksum[0]), 1, local_chksum.length(), fchksum) != local_chksum.length())
           unlink(lpath_chksum.c_str());
        fclose(fchksum);
     } else {
        unlink(lpath_chksum.c_str());
     }
  } else {
     unlink(lpath_chksum.c_str());
  }
  if ((sha1_expected == hash::t_sha1()) && signature_ok) {
     logmsg("Signed catalog loaded from %s, signed by %s", 
            (root_url_ + url_path).c_str(), signature::whois().c_str());
  }
  return 0;
}

/**
 * Checks, if the SHA1 checksum of a PEM certificate is listed on the
 * whitelist at URL cvmfs::cert_whitelist.
 * With nocache, whitelist is downloaded with pragma:no-cache
 */
bool CatalogManager::IsValidCertificate(bool nocache) {
   const string fingerprint = signature::fingerprint();
   if (fingerprint == "") {
      pmesg(D_CVMFS, "invalid catalog signature");
      return false;
   }
   pmesg(D_CVMFS, "checking certificate with fingerprint %s against whitelist", fingerprint.c_str());
   
   time_t local_timestamp = time(NULL);
   struct mem_url mem_url_wl;
   mem_url_wl.data = NULL;
   string buffer;
   istringstream stream;
   string line;   
   unsigned skip = 0;
   
   /* download whitelist */
   int curl_result;
   if (nocache) curl_result = curl_download_mem_nocache(whitelist_.c_str(), &mem_url_wl, 1, 0);
   else curl_result = curl_download_mem(whitelist_.c_str(), &mem_url_wl, 1, 0);
   if ((curl_result != CURLE_OK) || !mem_url_wl.data) {
      pmesg(D_CVMFS, "whitelist could not be loaded from %s", whitelist_.c_str());
      return false;
   } 
   buffer = string(mem_url_wl.data, mem_url_wl.size);
   
   /* parse whitelist */
   stream.str(buffer);
   
   /* check timestamp (UTC) */
   if (!getline(stream, line) || (line.length() != 14)) {
      pmesg(D_CVMFS, "invalid timestamp format");
      free(mem_url_wl.data);
      return false;
   }
   skip += 15; 
   /* Ignore issue date (legacy) */
   
   /* Now expiry date */
   if (!getline(stream, line) || (line.length() != 15)) {
      pmesg(D_CVMFS, "invalid timestamp format");
      free(mem_url_wl.data);
      return false;
   }
   skip += 16;
   struct tm tm_wl;
   memset(&tm_wl, 0, sizeof(struct tm));
   tm_wl.tm_year = atoi(line.substr(1, 4).c_str())-1900;
   tm_wl.tm_mon = atoi(line.substr(5, 2).c_str()) - 1;
   tm_wl.tm_mday = atoi(line.substr(7, 2).c_str());
   tm_wl.tm_hour = atoi(line.substr(9, 2).c_str());
   tm_wl.tm_min = 0; /* exact on hours level */
   tm_wl.tm_sec = 0;
   time_t timestamp = timegm(&tm_wl);
   pmesg(D_CVMFS, "whitelist UTC expiry timestamp in localtime: %s", localtime_ascii(timestamp, false).c_str());
   if (timestamp < 0) {
      pmesg(D_CVMFS, "invalid timestamp");
      free(mem_url_wl.data);
      return false;
   }
   pmesg(D_CVMFS, "local time: %s", localtime_ascii(local_timestamp, true).c_str());
   if (local_timestamp > timestamp) {
      pmesg(D_CVMFS, "whitelist lifetime verification failed, expired");
      free(mem_url_wl.data);
      return false;
   }
   
   /* Check repository name */
   if (!getline(stream, line)) {
      pmesg(D_CVMFS, "failed to get repository name");
      free(mem_url_wl.data);
      return false;
   }
   skip += line.length() + 1;
   if ((repo_name_ != "") && ("N" + repo_name_ != line)) {
      pmesg(D_CVMFS, "repository name does not match (found %s, expected %s)", 
                     line.c_str(), repo_name_.c_str());
      free(mem_url_wl.data);
      return false;
   }
   
   /* search the fingerprint */
   bool found = false;
   while (getline(stream, line)) {
      skip += line.length() + 1;
      if (line == "--") break;
      if (line.substr(0, 59) == fingerprint)
         found = true;
   }
   if (!found) {
      pmesg(D_CVMFS, "the certificate's fingerprint is not on the whitelist");
      if (mem_url_wl.data)
         free(mem_url_wl.data);
      return false;
   }
   
   /* check whitelist signature */
   if (!getline(stream, line) || (line.length() < 40)) {
      pmesg(D_CVMFS, "no checksum at the end of whitelist found");
      free(mem_url_wl.data);
      return false;
   }
   hash::t_sha1 sha1;
   sha1.from_hash_str(line.substr(0, 40));
   if (sha1 != hash::t_sha1(buffer.substr(0, skip-3))) {
      pmesg(D_CVMFS, "whitelist checksum does not match");
      free(mem_url_wl.data);
      return false;
   }
      
   /* check local blacklist */
   ifstream fblacklist;
   fblacklist.open(blacklist_.c_str());
   if (fblacklist) {
      string blackline;
      while (getline(fblacklist, blackline)) {
         if (blackline.substr(0, 59) == fingerprint) {
            pmesg(D_CVMFS, "this fingerprint is blacklisted");
            logmsg("Blacklisted fingerprint (%s)", fingerprint.c_str());
            fblacklist.close();
            free(mem_url_wl.data);
            return false;
         }
      }
      fblacklist.close();
   }
      
   void *sig_buf;
   unsigned sig_buf_size;
   if (!read_sig_tail(&buffer[0], buffer.length(), skip, 
                      &sig_buf, &sig_buf_size))
   {
      pmesg(D_CVMFS, "no signature at the end of whitelist found");
      free(mem_url_wl.data);
      return false;
   }
   const string sha1str = sha1.to_string();
   bool result = signature::verify_rsa(&sha1str[0], 40, sig_buf, sig_buf_size); 
   free(sig_buf);
   if (!result) pmesg(D_CVMFS, "whitelist signature verification failed, %s", signature::get_crypto_err().c_str());
   else pmesg(D_CVMFS, "whitelist signature verification passed");
      
   if (result) {
      return true;
   } else {
      free(mem_url_wl.data);
      return false;
   }
}

bool CatalogManager::GetCatalogById(const int catalog_id, Catalog **catalog) const {
  if (catalog_id < 0 || catalog_id >= GetNumberOfAttachedCatalogs()) {
    return false;
  }
  
  *catalog = catalogs_[catalog_id];
  return true;
}

}
