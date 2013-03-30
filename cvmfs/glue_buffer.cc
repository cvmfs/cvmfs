/**
 * This file is part of the CernVM File System.
 */

#define __STDC_FORMAT_MACROS

#include "cvmfs_config.h"
#include "glue_buffer.h"

#include <inttypes.h>
#include <dirent.h>
#include <unistd.h>
#include <limits.h>
#include <errno.h>
#ifdef __APPLE__
#include <sys/types.h>
#include <sys/sysctl.h>
#include <libproc.h>
#endif

#include <cstdlib>
#include <cstring>
#include <cassert>

#include <vector>
#include <string>

#include "platform.h"
#include "smalloc.h"
#include "logging.h"
#include "util.h"

using namespace std;  // NOLINT

namespace glue {
  
bool InodeContainer::ConstructPath(const uint64_t inode, PathString *path) {
  InodeMap::const_iterator needle = map_.find(inode);
  if (needle == map_.end())
    return false;
  
  if (needle->second.name.IsEmpty())
    return true;
  
  bool retval = ConstructPath(needle->second.parent_inode, path);
  path->Append("/", 1);
  path->Append(needle->second.name.GetChars(), 
               needle->second.name.GetLength());
  if (!retval) {
    LogCvmfs(kLogGlueBuffer, kLogDebug | kLogSyslog, "internal error: "
            "failed constructing (path so far %s, inode %"PRIu64")", 
            path->c_str(), inode);
  }
  return retval;
}


bool InodeContainer::ConstructChain(const uint64_t inode, 
                                    vector<Dirent> *chain)
{
  uint64_t needle_inode = inode;
  InodeMap::const_iterator needle;
  do {
    needle = map_.find(needle_inode);
    if (needle == map_.end())
      return false;
    
    chain->push_back(needle->second);    
    needle_inode = needle->second.parent_inode;
  } while (!needle->second.name.IsEmpty());

  return true;
}
  
  
uint32_t InodeContainer::TransferAll(const PathString &path, 
                                     InodeContainer *destination) 
{
  uint32_t num_transferred = 0;
  NameString top_name(GetFileName(path));
  for (InodeMap::const_iterator i = map_.begin(), iEnd = map_.end(); 
       i != iEnd; ++i)
  {
    if (i->second.name == top_name) {
      PathString reconstructed_path;
      bool retval = ConstructPath(i->first, &reconstructed_path);
      assert(retval);
      if ((reconstructed_path == path) && !destination->Contains(i->first)) {
        Dirent chain_iter = i->second;
        uint64_t inode_iter = i->first;
        do {
          destination->AddDirent(inode_iter, chain_iter);
          num_transferred++;
          if (chain_iter.name.IsEmpty())
            break;
          inode_iter = chain_iter.parent_inode;
          if (destination->Contains(inode_iter))
            break;
          chain_iter = map_.find(inode_iter)->second;
        } while (true);
      }
    }
  }
  
  return num_transferred;
}


//------------------------------------------------------------------------------


void LookupTracker::InitLock() {
  rwlock_ =
    reinterpret_cast<pthread_rwlock_t *>(smalloc(sizeof(pthread_rwlock_t)));
  int retval = pthread_rwlock_init(rwlock_, NULL);
  assert(retval == 0);
}


LookupTracker::LookupTracker(const unsigned size) {
  assert(size >= 2);
  version_ = kVersion;
  size_ = size;
  buffer_ = new BufferEntry[size_];
  atomic_init64(&buffer_pos_);
  InitLock();
}


void LookupTracker::CopyFrom(const LookupTracker &other) {
  assert(other.version_ == kVersion);
  version_ = kVersion;
  size_ = other.size_;
  buffer_ = new BufferEntry[size_];
  for (unsigned i = 0; i < size_; ++i)
    buffer_[i] = other.buffer_[i];
  buffer_pos_ = other.buffer_pos_;
  ensemble_ = other.ensemble_;
  statistics_ = other.statistics_;
}


LookupTracker::LookupTracker(const LookupTracker &other) {
  CopyFrom(other);
  InitLock();
}


LookupTracker &LookupTracker::operator= (const LookupTracker &other) {
  if (&other == this)
    return *this;
  
  delete[] buffer_;
  CopyFrom(other);
  return *this;
}


LookupTracker::~LookupTracker() {
  delete[] buffer_;
  pthread_rwlock_destroy(rwlock_);
  free(rwlock_);
}


void LookupTracker::Resize(const unsigned new_size) {
  if (size_ == new_size)
    return;
  
  assert(new_size >= 2);
  BufferEntry *new_buffer = new BufferEntry[new_size];
  
  WriteLock();
  unsigned num_entries = size_ > new_size ? new_size : size_;
  for (unsigned i = 0; i < num_entries; ++i) {
    int64_t from_pos = 
    ((int64_t)(buffer_pos_) - (int64_t)(num_entries-i)) % size_;
    if (from_pos < 0)
      from_pos = size_ - (-from_pos);
    new_buffer[i] = buffer_[from_pos];
  }
  delete[] buffer_;
  buffer_ = new_buffer;
  if (buffer_pos_ >= new_size)
    buffer_pos_ = num_entries;
  size_ = new_size;
  Unlock();
}


bool LookupTracker::ConstructPath(const unsigned buffer_idx, PathString *path) {
  // Root inode found?
  if (buffer_[buffer_idx].name.IsEmpty())
    return true;
  
  // Construct path until buffer_idx
  LogCvmfs(kLogGlueBuffer, kLogDebug, "construct inode %u, parent %u, name %s", 
           buffer_[buffer_idx].inode, buffer_[buffer_idx].parent_inode, 
           buffer_[buffer_idx].name.c_str());
  unsigned parent_idx;
  uint64_t needle_inode = buffer_[buffer_idx].parent_inode;
  bool retval = FindIndex(needle_inode, &parent_idx);
  bool result;
  if (retval) {
    result = ConstructPath(parent_idx, path);
  } else {
    LogCvmfs(kLogGlueBuffer, kLogDebug,  "jumping from glue buffer to "
             "active inodes buffer, inode: %u", needle_inode);
    bool retval = ensemble_->open_tracker()->Find(needle_inode, path);
    if (retval) {
      atomic_inc64(&statistics_.num_jmpai_hits);
      result = true;
      goto construct_path_append;
    }
    atomic_inc64(&statistics_.num_jmpai_misses);
    
    LogCvmfs(kLogGlueBuffer, kLogDebug,  "jumping from glue buffer to "
             "cwd buffer, inode: %u", needle_inode);
    retval = ensemble_->cwd_tracker()->Find(needle_inode, path);
    if (retval) {
      atomic_inc64(&statistics_.num_jmpcwd_hits);
      result = true;
      goto construct_path_append;
    }
    atomic_inc64(&statistics_.num_jmpcwd_misses);
    
    result = false;
  }
  
construct_path_append:
  path->Append("/", 1);
  path->Append(buffer_[buffer_idx].name.GetChars(), 
               buffer_[buffer_idx].name.GetLength());  
  return result;
}


bool LookupTracker::ConstructChain(const unsigned buffer_idx, 
                                   std::vector<Dirent> *chain) 
{
  unsigned index = buffer_idx;
  do {
    chain->push_back(Dirent(buffer_[index].parent_inode, buffer_[index].name));
    if (buffer_[index].name.IsEmpty())
      return true;
    
    uint64_t needle_inode = buffer_[index].parent_inode;
    bool retval = FindIndex(needle_inode, &index);
    if (!retval) {
      LogCvmfs(kLogGlueBuffer, kLogDebug,  "jumping from glue buffer to "
               "active inodes buffer, inode: %u", needle_inode);
      bool retval = ensemble_->open_tracker()->FindChain(needle_inode, chain);
      if (retval) {
        atomic_inc64(&statistics_.num_jmpai_hits);
        return true;
      }
      atomic_inc64(&statistics_.num_jmpai_misses);
      
      LogCvmfs(kLogGlueBuffer, kLogDebug,  "jumping from glue buffer to "
               "cwd buffer, inode: %u", needle_inode);
      retval = ensemble_->cwd_tracker()->FindChain(needle_inode, chain);
      if (retval) {
        atomic_inc64(&statistics_.num_jmpcwd_hits);
        return true;
      }
      atomic_inc64(&statistics_.num_jmpcwd_misses);
      return false;
    }
  } while (true);
}
  
  
bool LookupTracker::FindIndex(const uint64_t inode, unsigned *index) {
  bool found = false;
  for (unsigned i = 0; i < size_; ++i) {
    //LogCvmfs(kLogGlueBuffer, kLogDebug, "GLUE: idx %d, inode %u, parent %u, "
    //         "revision %u, name %s",
    //         i, buffer_[i].inode, buffer_[i].parent_inode, buffer_[i].revision, 
    //         buffer_[i].name.c_str());
    if (buffer_[i].inode == inode) {
      *index = i;
      found = true;
      break;
    }
  }
  if (!found) {
    LogCvmfs(kLogGlueBuffer, kLogDebug, "failed to find index for "
             "ancient inode %"PRIu64, inode);
    return false;
  }
  return true;
}


bool LookupTracker::Find(const uint64_t inode, PathString *path) {
  assert(path->IsEmpty());
  WriteLock();
  
  // Find initial buffer index
  unsigned index;
  bool retval = FindIndex(inode, &index);
  if (!retval) {
    Unlock();
    atomic_inc64(&statistics_.num_ancient_misses);
    return false;
  }
    
  // Recursively build path
  retval = ConstructPath(index, path);
  Unlock();
  
  if (retval) {
    atomic_inc64(&statistics_.num_ancient_hits);
    return true;
  }
  atomic_inc64(&statistics_.num_ancient_misses);
  return false;
}
  
  
bool LookupTracker::FindChain(const uint64_t inode, 
                              std::vector<Dirent> *chain) 
{
  WriteLock();
  
  // Find initial buffer index
  unsigned index;
  bool retval = FindIndex(inode, &index);
  if (!retval) {
    Unlock();
    atomic_inc64(&statistics_.num_ancient_misses);
    return false;
  }
  
  // Recursively build path
  retval = ConstructChain(index, chain);
  Unlock();
  
  if (retval) {
    atomic_inc64(&statistics_.num_ancient_hits);
    return true;
  }
  atomic_inc64(&statistics_.num_ancient_misses);
  return false;
  
  Unlock();
}
  

//------------------------------------------------------------------------------


void CwdTracker::InitLock() {
  lock_ =
    reinterpret_cast<pthread_mutex_t *>(smalloc(sizeof(pthread_mutex_t)));
  int retval = pthread_mutex_init(lock_, NULL);
  assert(retval == 0);
}


void CwdTracker::CopyFrom(const CwdTracker &other) {
  assert(other.version_ == kVersion);
  version_ = kVersion;
  inode2cwd_ = other.inode2cwd_;
  mountpoint_ = other.mountpoint_;
  ensemble_ = other.ensemble_;
  statistics_ = other.statistics_;
}


CwdTracker::CwdTracker(const std::string &mountpoint) { 
  version_ = kVersion;
  mountpoint_ = mountpoint;
  InitLock();
}


CwdTracker::CwdTracker(const CwdTracker &other) {
  CopyFrom(other);
  InitLock();
}


CwdTracker &CwdTracker::operator= (const CwdTracker &other) {
  if (&other == this)
    return *this;
  
  CopyFrom(other);
  return *this;
}


CwdTracker::~CwdTracker() {
  pthread_mutex_destroy(lock_);
  free(lock_);
}


vector<PathString> CwdTracker::GatherCwds() {
  pid_t save_uid = geteuid();
  gid_t save_gid = getegid();
  
  int retval = SwitchCredentials(0, save_gid, true);
  if (!retval) {
    LogCvmfs(kLogGlueBuffer, kLogDebug, 
             "failed to switch to root for gathering cwds");
  }
  
  vector<PathString> result;
#ifdef __APPLE__
  // TODO: list cwds without stating them
  // List PIDs
  /*  vector<pid_t> pids;
   int buf_size = proc_listpids(PROC_ALL_PIDS, 0, NULL, 0);
   if (buf_size <= 0) {
   LogCvmfs(kLogGlueBuffer, kLogDebug | kLogSyslog, 
   "failed to gather pid buffer (%d)", errno);
   }
   int *all_pids = static_cast<pid_t *>(smalloc(buf_size));
   buf_size = proc_listpids(PROC_ALL_PIDS, 0, all_pids, buf_size);
   if (buf_size <= 0) {
   LogCvmfs(kLogGlueBuffer, kLogDebug | kLogSyslog, 
   "failed to gather pids (%d)", errno);
   } else {
   int num_procs = buf_size / sizeof(pid_t);
   for (int i = 0; i < num_procs; ++i)
   pids.push_back(all_pids[i]);
   }
   free(all_pids);
   
   // Gather cwd for pids
   // Blocks on cvmfs because it also tries to get the stat information
   for (unsigned i = 0; i < pids.size(); ++i) {
   struct proc_vnodepathinfo vpi;
   buf_size = proc_pidinfo(pids[i], PROC_PIDVNODEPATHINFO, 0, 
   &vpi, sizeof(vpi));
   if (buf_size < (int)sizeof(vpi)) {
   LogCvmfs(kLogGlueBuffer, kLogDebug, "failed to gather cwd for "
   "pid %d (%d)", pids[i], errno);
   } else {
   if (!vpi.pvi_cdir.vip_path[0]) {
   LogCvmfs(kLogGlueBuffer, kLogDebug, "no cwd for pid %d (%d)", pids[i]);
   continue;
   }
   string cwd(vpi.pvi_cdir.vip_path);
   LogCvmfs(kLogGlueBuffer, kLogDebug, "cwd of pid %d is %s", 
   pids[i], cwd.c_str());
   if (HasPrefix(cwd, mountpoint_ + "/", false)) {
   string relative_cwd = cwd.substr(mountpoint_.length());
   result.push_back(PathString(relative_cwd));
   }
   }
   }*/
#else
  DIR *dirp = opendir("/proc");
  if (!dirp) {
    LogCvmfs(kLogGlueBuffer, kLogDebug | kLogSyslog, "failed to open /proc");
    Unlock();
    retval = SwitchCredentials(save_uid, save_gid, true);
    assert(retval);
    return result;
  }
  platform_dirent64 *d;
  while ((d = platform_readdir(dirp)) != NULL) {
    const string pid = d->d_name;
    if (!IsNumeric(pid))
      continue;
    
    const string path = string("/proc/") + pid + string("/cwd");
    char symlink_buf[PATH_MAX];
    ssize_t retval = readlink(path.c_str(), symlink_buf, PATH_MAX);
    if (retval >= 0) {
      const string cwd(symlink_buf, retval);
      LogCvmfs(kLogGlueBuffer, kLogDebug, "cwd of pid %s is %s",
               pid.c_str(), cwd.c_str());
      if (HasPrefix(cwd, mountpoint_ + "/", false)) {
        string relative_cwd = cwd.substr(mountpoint_.length());
        result.push_back(PathString(relative_cwd));
      }
    } else {
      LogCvmfs(kLogGlueBuffer, kLogDebug, "failed to read cwd of pid %s", 
               pid.c_str());
    }
  }
  closedir(dirp);
#endif
  
  if (!result.empty())
    result.push_back(PathString());
  
  retval = SwitchCredentials(save_uid, save_gid, true);
  assert(retval);
  
  return result;
}


bool CwdTracker::Find(const uint64_t inode, PathString *path) {
  bool result = false;
  Lock();
  result = inode2cwd_.ConstructPath(inode, path);
  Unlock();
  
  if (result) {
    atomic_inc64(&statistics_.num_ancient_hits);
    return true;
  }
  atomic_inc64(&statistics_.num_ancient_misses);
  return false;
}

  
uint64_t CwdTracker::Path2Chain(const PathString &path,
                                catalog::AbstractCatalogManager *source) 
{
  uint64_t inode;
  bool retval = source->Path2InodeUnprotected(path, &inode);
  if (!retval)
    return 0;
  
  if (inode2cwd_.Contains(inode))
    return inode;
  
  uint64_t parent_inode = 0;
  if (!path.IsEmpty()) {
    parent_inode = Path2Chain(GetParentPath(path), source);
    if (parent_inode == 0)
      return 0;
  }
  
  atomic_inc64(&statistics_.num_inserts);
  inode2cwd_.Add(inode, parent_inode, GetFileName(path));
  return inode;
}
  
  
void CwdTracker::MaterializePaths(catalog::AbstractCatalogManager *source) {
  vector<PathString> open_cwds = GatherCwds();
  
  Lock();
  
  // Clean garbage
  // Note: just dropping the buffer is wrong because it is unclear
  // in which generation a cwd has been acquired
  inode_buffer_.clear();
  for (InodeContainer::InodeMap::const_iterator i = inode2cwd_.map()->begin(),
       iEnd = inode2cwd_.map()->end(); i != iEnd; ++i)
  { 
    PathString old_cwd_path;
    bool retval = inode2cwd_.ConstructPath(i->first, &old_cwd_path);
    assert(retval);
    old_cwd_path.Append("/", 1);
    
    bool found = false;
    PathString tmp;
    for (unsigned j = 0; j < open_cwds.size(); ++j) {
      tmp.Assign(open_cwds[j]);
      tmp.Append("/", 1);
      if (tmp.StartsWith(old_cwd_path)) {
        found = true;
        break;
      }
    }
    if (!found) {
      inode_buffer_.push_back(i->first);
    } 
  }
  
  for (unsigned i = 0, l = inode_buffer_.size(); i < l; ++i) {
    atomic_inc64(&statistics_.num_removes);
    inode2cwd_.map()->erase(inode_buffer_[i]);
  }
  
  InodeContainer *open_tracker_map = ensemble_->open_tracker()->inode2path();
  for (unsigned i = 0; i < open_cwds.size(); ++i) {
    Path2Chain(open_cwds[i], source);
    uint32_t transferred = open_tracker_map->TransferAll(open_cwds[i], 
                                                         &inode2cwd_);
    atomic_xadd64(&statistics_.num_transfers, transferred);
  }
  
  Unlock();
}
  
  
void CwdTracker::Inject(const uint64_t initial_inode, 
                        const std::vector<Dirent> &chain)
{
  if (chain.empty())
    return;
  
  uint64_t inode = initial_inode;
  unsigned idx = 0;
  Lock();
  do {
    if (inode2cwd_.Contains(inode))
      break;
    
    inode2cwd_.AddDirent(inode, chain[idx]);
    atomic_inc64(&statistics_.num_injects);
    idx++;
  } while (idx < chain.size());
  Unlock();
}
  
  
//------------------------------------------------------------------------------
  

void OpenTracker::InitLocks() {
  lock_inodes_ =
    reinterpret_cast<pthread_mutex_t *>(smalloc(sizeof(pthread_mutex_t)));
  int retval = pthread_mutex_init(lock_inodes_, NULL);
  assert(retval == 0);
  lock_paths_ =
    reinterpret_cast<pthread_mutex_t *>(smalloc(sizeof(pthread_mutex_t)));
  retval = pthread_mutex_init(lock_paths_, NULL);
  assert(retval == 0);
}


void OpenTracker::InitSpecialInodes() {
  inode_references_.set_deleted_key(0);
  //inode_references_.set_empty_key(1);  Only required for densemap
}


void OpenTracker::CopyFrom(const OpenTracker &other) {
  assert(other.version_ == kVersion);
  version_ = kVersion;
  inode2path_ = new InodeContainer(*other.inode2path_);
  inode_references_ = other.inode_references_;
  ensemble_ = other.ensemble_;
  statistics_ = other.statistics_;
}


OpenTracker::OpenTracker() { 
  version_ = kVersion;
  inode2path_ = new InodeContainer();
  InitLocks();
  InitSpecialInodes();
}


OpenTracker::OpenTracker(const OpenTracker &other) {
  CopyFrom(other);
  InitLocks();
  InitSpecialInodes();
}


OpenTracker &OpenTracker::operator= (const OpenTracker &other) {
  if (&other == this)
    return *this;
  
  delete inode2path_;
  CopyFrom(other);
  return *this;
}


OpenTracker::~OpenTracker() {
  delete inode2path_;
  pthread_mutex_destroy(lock_inodes_);
  pthread_mutex_destroy(lock_paths_);
  free(lock_inodes_);
  free(lock_paths_);
}
  
  
uint32_t OpenTracker::IncInodeReference(const uint64_t inode) {
  InodeReferences::iterator iter_inodes = inode_references_.find(inode);
  uint32_t result;
  if (iter_inodes != inode_references_.end()) {
    result = (*iter_inodes).second + 1;
    (*iter_inodes).second = result;
  } else {
    inode_references_[inode] = 1;
    result = 1;
  }
  atomic_inc64(&statistics_.num_references);
  return result;
}
  
  
void OpenTracker::AddChainbuffer(const uint64_t initial_inode, 
                                 InodeContainer *container) 
{
  if (container->Contains(initial_inode))
    return;
  
  container->AddDirent(initial_inode, chain_buffer_[0]);
  for (unsigned i = 1; i < chain_buffer_.size(); ++i) {
    if (container->Contains(chain_buffer_[i-1].parent_inode))
      break;
    container->AddDirent(chain_buffer_[i-1].parent_inode, chain_buffer_[i]);
  }
}


void OpenTracker::VfsGetLiving(const uint64_t inode) {
  LockInodes();
  uint32_t references = IncInodeReference(inode);
  if (references == 1)
    atomic_inc64(&statistics_.num_inserts_living);
  UnlockInodes();
}
  
  
void OpenTracker::VfsGetDeprecated(const uint64_t inode) {  
  LockInodes();
  // Materialize ancient path immediately
  chain_buffer_.clear();
  bool retval = ensemble_->FindChain(inode, &chain_buffer_);
  if (retval) {
    LockPaths();
    AddChainbuffer(inode, inode2path_);
    UnlockPaths();
  } else {
    LogCvmfs(kLogGlueBuffer, kLogDebug | kLogSyslog, "internal error: "
             "failed to materialize path for ancient inode %"PRIu64, inode);
  }
  uint32_t references = IncInodeReference(inode);
  if (references == 1)
    atomic_inc64(&statistics_.num_inserts_deprecated);
  UnlockInodes();
}
  

void OpenTracker::VfsPut(const uint64_t inode) {
  LockInodes();
  InodeReferences::iterator iter_inodes = inode_references_.find(inode);
  if (iter_inodes == inode_references_.end()) {
    LogCvmfs(kLogGlueBuffer, kLogDebug | kLogSyslog, "failed to locate inode %"
             PRIu64" in VfsPut", inode);
    UnlockInodes();
    return;
  }
  uint32_t new_references = (*iter_inodes).second - 1;
  if (new_references == 0) {
    inode_references_.erase(iter_inodes);
    atomic_inc64(&statistics_.num_removes);
  } else {
    (*iter_inodes).second = new_references;
  }
  UnlockInodes();
  atomic_dec64(&statistics_.num_references);
}


bool OpenTracker::Find(const uint64_t inode, PathString *path) {
  assert(path->IsEmpty());
  bool result = false;
  LockPaths();
  result = inode2path_->ConstructPath(inode, path);
  UnlockPaths();
  
  if (result) {
    atomic_inc64(&statistics_.num_ancient_hits);
    return true;
  }
  atomic_inc64(&statistics_.num_ancient_misses);
  return false;
}
  

// TODO: add ancient inodes to cwd buffer if necessary
void OpenTracker::MaterializePaths(catalog::AbstractCatalogManager *source) {
  LockInodes();
  LockPaths();
  InodeContainer *new_map = new InodeContainer();
  for (InodeReferences::const_iterator i = inode_references_.begin(),
       iEnd = inode_references_.end(); i != iEnd; ++i)
  {
    // Rescue in-use ancient inodes
    uint64_t needle_inode = i->first;
    chain_buffer_.clear();
    if (inode2path_->ConstructChain(needle_inode, &chain_buffer_)) {
      AddChainbuffer(needle_inode, new_map);      
      continue;
    }
    
    // Recover the path step by step from catalog and existing entries
    do {
      if (new_map->Contains(needle_inode))
        break;
      
      catalog::DirectoryEntry dirent;
      bool retval = source->Inode2DirentUnprotected(needle_inode, &dirent);
      atomic_inc64(&statistics_.num_dirent_lookups);
      if (!retval) {
        LogCvmfs(kLogGlueBuffer, kLogDebug | kLogSyslog, "internal error: "
                 "active inode buffer lookup failure inode %"PRIu64, i->first);
        break;
      }
      
      new_map->AddCatalogDirent(dirent);
      // Root inode reached
      if (dirent.name().IsEmpty())
        break;
      needle_inode = dirent.parent_inode();
    } while (true);
  }
  delete inode2path_;
  inode2path_ = new_map;
  UnlockPaths();
  UnlockInodes();
}
  

//------------------------------------------------------------------------------
  

bool Ensemble::Find(const uint64_t inode, PathString *path) {
  bool found = open_tracker_->Find(inode, path);
  if (found) {
    LogCvmfs(kLogCvmfs, kLogDebug, "found path %s in active inodes buffer "
             "for ancient inode %"PRIu64, path->c_str(), inode);
    return true;
  }
  
  found = cwd_tracker_->Find(inode, path);
  if (found) {
    LogCvmfs(kLogCvmfs, kLogDebug, "found path %s in cwd buffer for "
             "ancient inode %"PRIu64, path->c_str(), inode);
    return true;
  }
  
  found = lookup_tracker_->Find(inode, path);
  if (found) {
    // Potential cwd, inject into cwd buffer
    vector<Dirent> chain;
    bool retval = FindChain(inode, &chain);
    if (retval) {
      cwd_tracker_->Inject(inode, chain);
    } else {
      LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslog, "internal error: "
               "failed to inject %"PRIu64" into cwd tracker");
    }
  }
  
  return found;
}
  
  
bool Ensemble::FindChain(const uint64_t inode, 
                         std::vector<glue::Dirent> *chain) 
{
  bool found = open_tracker_->FindChain(inode, chain);
  if (found) {
    LogCvmfs(kLogCvmfs, kLogDebug, "found chain in active inodes buffer "
             "for ancient inode %"PRIu64, inode);
    return true;
  }
  
  found = cwd_tracker_->FindChain(inode, chain);
  if (found) {
    LogCvmfs(kLogCvmfs, kLogDebug, "found chain in cwd buffer for "
             "ancient inode %"PRIu64, inode);
    return true;
  }
  
  found = lookup_tracker_->FindChain(inode, chain);
  return found;
}
  
  
void Ensemble::Embrace(LookupTracker *l, CwdTracker *c, OpenTracker *o) {
  lookup_tracker_ = l;
  cwd_tracker_ = c;
  open_tracker_ = o;
  lookup_tracker_->SetEnsemble(this);
  cwd_tracker_->SetEnsemble(this);
  open_tracker_->SetEnsemble(this);
}
  
  
void Ensemble::CopyFrom(const Ensemble &other) {
  assert(other.version_ == kVersion);
  version_ = kVersion;
  lookup_tracker_ = new LookupTracker(*other.lookup_tracker_);
  cwd_tracker_ = new CwdTracker(*other.cwd_tracker_);
  open_tracker_ = new OpenTracker(*other.open_tracker_);
  Embrace(lookup_tracker_, cwd_tracker_, open_tracker_);
}
  
  
Ensemble::Ensemble(const Ensemble &other) {
  CopyFrom(other);  
}


Ensemble &Ensemble::operator= (const Ensemble &other) {
  if (&other == this)
    return *this;
  
  assert(other.version_ == kVersion);
  Cleanup();
  CopyFrom(other);
  return *this;
}

  
void Ensemble::Cleanup() {
  delete lookup_tracker_;
  delete cwd_tracker_;
  delete open_tracker_;
}

}  // namespace glue
