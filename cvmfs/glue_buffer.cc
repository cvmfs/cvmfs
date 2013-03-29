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
  InodeMap::const_iterator needle = inode2path_.find(inode);
  if (needle == inode2path_.end())
    return false;
  
  if (needle->second.name.IsEmpty())
    return true;
  
  bool retval = ConstructPath(needle->second.parent_inode, path);
  path->Append("/", 1);
  path->Append(needle->second.name.GetChars(), 
               needle->second.name.GetLength());
  if (!retval) {
    // Logging
  }
  return retval;
}


bool InodeContainer::ConstructChain(const uint64_t inode, 
                                    std::vector<glue::Dirent> *chain)
{
  uint64_t needle_inode = inode;
  InodeMap::const_iterator needle;
  do {
    needle = inode2path_.find(needle_inode);
    if (needle == inode2path_.end())
      return false;
    
    chain->push_back(needle->second);    
    needle_inode = needle->second.parent_inode;
  } while (!needle->second.name.IsEmpty());

  return true;
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
  uint64_t needle_inode = buffer_[buffer_idx].parent_inode;
  int parent_idx = -1;
  for (unsigned i = 0; i < size_; ++i) {
    if (buffer_[i].inode == needle_inode) {
      parent_idx = i;
      break;
    }
  }
  bool result;
  if (parent_idx >= 0) {
    result = ConstructPath(parent_idx, path);
  } else {
    /*if (active_inodes_) {
      LogCvmfs(kLogGlueBuffer, kLogDebug,  "jumping from glue buffer to "
               "active inodes buffer, inode: %u", needle_inode);
      bool retval = active_inodes_->Find(needle_inode, path);
      if (retval) {
        atomic_inc64(&statistics_.num_jmpai_hits);
        result = true;
        goto construct_path_append;
      }
      atomic_inc64(&statistics_.num_jmpai_misses);
    }
    
    if (cwd_buffer_) {
      LogCvmfs(kLogGlueBuffer, kLogDebug,  "jumping from glue buffer to "
               "cwd buffer, inode: %u", needle_inode);
      bool retval = cwd_buffer_->Find(needle_inode, path);
      if (retval) {
        atomic_inc64(&statistics_.num_jmpcwd_hits);
        result = true;
        goto construct_path_append;
      }
      atomic_inc64(&statistics_.num_jmpcwd_misses);
    }*/
    
    result = false;
  }
  
construct_path_append:
  path->Append("/", 1);
  path->Append(buffer_[buffer_idx].name.GetChars(), 
               buffer_[buffer_idx].name.GetLength());  
  return result;
}


bool LookupTracker::Find(const uint64_t inode, PathString *path) {
  assert(path->IsEmpty());
  WriteLock();
  
  // Find inode with highest revision < new_revision
  int index = -1;
  for (unsigned i = 0; i < size_; ++i) {
    //LogCvmfs(kLogGlueBuffer, kLogDebug, "GLUE: idx %d, inode %u, parent %u, "
    //         "revision %u, name %s",
    //         i, buffer_[i].inode, buffer_[i].parent_inode, buffer_[i].revision, 
    //         buffer_[i].name.c_str());
    if (buffer_[i].inode == inode) {
      index = i;
      break;
    }
  }
  if (index < 0) {
    LogCvmfs(kLogGlueBuffer, kLogDebug, "failed to find initial needle for "
             "ancient inode %"PRIu64, inode);
    Unlock();
    atomic_inc64(&statistics_.num_ancient_misses);
    return false;
  }
  
  // Recursively build path
  bool retval = ConstructPath(index, path);
  Unlock();
  
  if (retval) {
    atomic_inc64(&statistics_.num_ancient_hits);
    return true;
  }
  atomic_inc64(&statistics_.num_ancient_misses);
  return false;
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
  
  Lock();
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
   while ((relative_cwd = GetParentPath(relative_cwd)) != "") {
   result.push_back(PathString(relative_cwd));
   }
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
        while ((relative_cwd = GetParentPath(relative_cwd)) != "") {
          result.push_back(PathString(relative_cwd));
        }
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
  
  // Clean garbage
  // Note: just dropping the buffer is wrong because it is unclear
  // in which generation a cwd has been acquired
/*  for (std::map<uint64_t, PathString>::iterator i = inode2cwd_.begin(),
       iEnd = inode2cwd_.end(); i != iEnd; )
  {
    bool found = false;
    for (unsigned j = 0; j < result.size(); ++j) {
      if (i->second == result[j]) {
        found = true;
        break;
      }
    }
    if (!found) {
      std::map<uint64_t, PathString>::iterator delete_me = i;
      ++i;
      inode2cwd_.erase(delete_me);
      atomic_inc64(&statistics_.num_removes);
    } else {
      ++i;
    }
  }*/
  Unlock();
  
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

  
void CwdTracker::MaterializePaths(catalog::AbstractCatalogManager *source) {
  vector<PathString> open_cwds = GatherCwds();
  for (unsigned i = 0; i < open_cwds.size(); ++i) {
    catalog::inode_t inode;
    bool retval = source->Path2InodeUnprotected(open_cwds[i], &inode);
    //if (retval)
    //  Add(inode, open_cwds[i]);
  }
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


void OpenTracker::VfsGetLiving(const uint64_t inode) {
  LockInodes();
  InodeReferences::iterator iter_inodes = inode_references_.find(inode);
  if (iter_inodes != inode_references_.end()) {
    (*iter_inodes).second = (*iter_inodes).second + 1;
  } else {
    inode_references_[inode] = 1;
    atomic_inc64(&statistics_.num_inserts);
  }
  UnlockInodes();
  atomic_inc64(&statistics_.num_references);
}
  
  
void OpenTracker::VfsGetDeprecated(const uint64_t inode) {
  // TODO
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
    // Recover the path step by step from catalog and existing entries
    uint64_t needle_inode = i->first;
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
      
      new_map->AddDirent(dirent);
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


/*void GlueBuffer::InitLock() {
  rwlock_ =
    reinterpret_cast<pthread_rwlock_t *>(smalloc(sizeof(pthread_rwlock_t)));
  int retval = pthread_rwlock_init(rwlock_, NULL);
  assert(retval == 0);
}


GlueBuffer::GlueBuffer(const unsigned size) {
  assert(size >= 2);
  version_ = kVersion;
  size_ = size;
  buffer_ = new BufferEntry[size_];
  atomic_init64(&buffer_pos_);
  cwd_buffer_ = NULL;
  active_inodes_ = NULL;
  InitLock();
}


void GlueBuffer::CopyFrom(const GlueBuffer &other) {
  if (other.version_ > kVersion)
    abort();
  
  version_ = kVersion;
  size_ = other.size_;
  buffer_ = new BufferEntry[size_];
  for (unsigned i = 0; i < size_; ++i)
    buffer_[i] = other.buffer_[i];
  buffer_pos_ = other.buffer_pos_;
  statistics_ = other.statistics_;
  cwd_buffer_ = other.cwd_buffer_;
  active_inodes_ = other.active_inodes_;
}


GlueBuffer::GlueBuffer(const GlueBuffer &other) {
  CopyFrom(other);
  InitLock();
}


GlueBuffer &GlueBuffer::operator= (const GlueBuffer &other) {
  if (&other == this)
    return *this;

  delete[] buffer_;
  CopyFrom(other);
  return *this;
}


GlueBuffer::~GlueBuffer() {
  delete[] buffer_;
  pthread_rwlock_destroy(rwlock_);
  free(rwlock_);
}


void GlueBuffer::Resize(const unsigned new_size) {
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
  
  // TODO
}


bool GlueBuffer::ConstructPath(const unsigned buffer_idx, PathString *path) {
  // Root inode found?
  if (buffer_[buffer_idx].name.IsEmpty())
    return true;
  
  // Construct path until buffer_idx
  LogCvmfs(kLogGlueBuffer, kLogDebug, "construct inode %u, parent %u, name %s", 
           buffer_[buffer_idx].inode, buffer_[buffer_idx].parent_inode, 
           buffer_[buffer_idx].name.c_str());
  uint32_t needle_generation = buffer_[buffer_idx].generation;
  uint64_t needle_inode = buffer_[buffer_idx].parent_inode;
  int parent_idx = -1;
  for (unsigned i = 0; i < size_; ++i) {
    if ((buffer_[i].inode == needle_inode) && 
        (buffer_[i].generation == needle_generation))
    {
      parent_idx = i;
      break;
    }
  }
  bool result;
  if (parent_idx >= 0) {
    result = ConstructPath(parent_idx, path);
  } else {
    if (active_inodes_) {
      LogCvmfs(kLogGlueBuffer, kLogDebug,  "jumping from glue buffer to "
               "active inodes buffer, inode: %u", needle_inode);
      bool retval = active_inodes_->Find(needle_inode, path);
      if (retval) {
        atomic_inc64(&statistics_.num_jmpai_hits);
        result = true;
        goto construct_path_append;
      }
      atomic_inc64(&statistics_.num_jmpai_misses);
    }
    
    if (cwd_buffer_) {
      LogCvmfs(kLogGlueBuffer, kLogDebug,  "jumping from glue buffer to "
               "cwd buffer, inode: %u", needle_inode);
      bool retval = cwd_buffer_->Find(needle_inode, path);
      if (retval) {
        atomic_inc64(&statistics_.num_jmpcwd_hits);
        result = true;
        goto construct_path_append;
      }
      atomic_inc64(&statistics_.num_jmpcwd_misses);
    }
    
    result = false;
  }
  
 construct_path_append:
  path->Append("/", 1);
  path->Append(buffer_[buffer_idx].name.GetChars(), 
               buffer_[buffer_idx].name.GetLength());  
  return result;
}


bool GlueBuffer::Find(const uint64_t inode, const uint32_t current_generation,
                      PathString *path)
{
  assert(path->IsEmpty());
  WriteLock();
  
  // Find inode with highest revision < new_revision
  unsigned max_generation = 0;
  int index = -1;
  for (unsigned i = 0; i < size_; ++i) {
    //LogCvmfs(kLogGlueBuffer, kLogDebug, "GLUE: idx %d, inode %u, parent %u, "
    //         "revision %u, name %s",
    //         i, buffer_[i].inode, buffer_[i].parent_inode, buffer_[i].revision, 
    //         buffer_[i].name.c_str());
    if ((buffer_[i].inode == inode) && 
        ((buffer_[i].generation < current_generation)) &&
        (buffer_[i].generation >= max_generation))
    {
      max_generation = buffer_[i].generation;
      index = i;
    }
  }
  if (index < 0) {
    LogCvmfs(kLogGlueBuffer, kLogDebug, "failed to find initial needle for "
             "ancient inode %"PRIu64, inode);
    Unlock();
    atomic_inc64(&statistics_.num_ancient_misses);
    return false;
  }
  
  // Recursively build path
  bool retval = ConstructPath(index, path);
  Unlock();
  
  if (retval) {
    atomic_inc64(&statistics_.num_ancient_hits);
    return true;
  }
  atomic_inc64(&statistics_.num_ancient_misses);
  return false;
}


//------------------------------------------------------------------------------


void CwdBuffer::InitLock() {
  lock_ =
    reinterpret_cast<pthread_mutex_t *>(smalloc(sizeof(pthread_mutex_t)));
  int retval = pthread_mutex_init(lock_, NULL);
  assert(retval == 0);
}


void CwdBuffer::CopyFrom(const CwdBuffer &other) {
  inode2cwd_ = other.inode2cwd_;
  mountpoint_ = other.mountpoint_;
  statistics_ = other.statistics_;
}


CwdBuffer::CwdBuffer(const std::string &mountpoint) { 
  version_ = kVersion;
  mountpoint_ = mountpoint;
  InitLock();
}


CwdBuffer::CwdBuffer(const CwdBuffer &other) {
  assert(other.version_ == kVersion);
  version_ = kVersion;
  CopyFrom(other);
  InitLock();
}


CwdBuffer &CwdBuffer::operator= (const CwdBuffer &other) {
  if (&other == this)
    return *this;
  
  assert(other.version_ == kVersion);
  CopyFrom(other);
  return *this;
}


CwdBuffer::~CwdBuffer() {
  pthread_mutex_destroy(lock_);
  free(lock_);
}


vector<PathString> CwdBuffer::GatherCwds() {
  pid_t save_uid = geteuid();
  gid_t save_gid = getegid();
  
  Lock();
  int retval = SwitchCredentials(0, save_gid, true);
  if (!retval) {
    LogCvmfs(kLogGlueBuffer, kLogDebug, 
             "failed to switch to root for gathering cwds");
  }
  
  vector<PathString> result;
#ifdef __APPLE__
  // TODO: list cwds without stating them
  // List PIDs
*//*  vector<pid_t> pids;
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
        while ((relative_cwd = GetParentPath(relative_cwd)) != "") {
          result.push_back(PathString(relative_cwd));
        }
      }
    }
  }*//*
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
        while ((relative_cwd = GetParentPath(relative_cwd)) != "") {
          result.push_back(PathString(relative_cwd));
        }
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
  
  // Clean garbage
  // Note: just dropping the buffer is wrong because it is unclear
  // in which generation a cwd has been acquired
  for (std::map<uint64_t, PathString>::iterator i = inode2cwd_.begin(),
       iEnd = inode2cwd_.end(); i != iEnd; )
  {
    bool found = false;
    for (unsigned j = 0; j < result.size(); ++j) {
      if (i->second == result[j]) {
        found = true;
        break;
      }
    }
    if (!found) {
      std::map<uint64_t, PathString>::iterator delete_me = i;
      ++i;
      inode2cwd_.erase(delete_me);
      atomic_inc64(&statistics_.num_removes);
    } else {
      ++i;
    }
  }
  Unlock();
  
  return result;
}


void CwdBuffer::Add(const uint64_t inode, const PathString &path) {
  Lock();
  inode2cwd_[inode] = path;
  Unlock();
  atomic_inc64(&statistics_.num_inserts);
}


void CwdBuffer::Remove(const uint64_t inode) {
  Lock();
  unsigned num_erased = inode2cwd_.erase(inode);
  Unlock();
  if (num_erased)
    atomic_inc64(&statistics_.num_removes);
}


bool CwdBuffer::Find(const uint64_t inode, PathString *path) {
  bool result = false;
  Lock();
  map<uint64_t, PathString>::const_iterator needle = inode2cwd_.find(inode);
  if (needle != inode2cwd_.end()) {
    result = true;
    path->Assign(needle->second);
  }
  Unlock();
  
  if (result) {
    atomic_inc64(&statistics_.num_ancient_hits);
    return true;
  }
  atomic_inc64(&statistics_.num_ancient_misses);
  return false;
}

void CwdBuffer::BeforeRemount(catalog::AbstractCatalogManager *source) {
  vector<PathString> open_cwds = GatherCwds();
  for (unsigned i = 0; i < open_cwds.size(); ++i) {
    catalog::inode_t inode;
    bool retval = source->Path2InodeUnprotected(open_cwds[i], &inode);
    if (retval)
      Add(inode, open_cwds[i]);
  }
}


//------------------------------------------------------------------------------


void ActiveInodesBuffer::InitLocks() {
  lock_inodes_ =
    reinterpret_cast<pthread_mutex_t *>(smalloc(sizeof(pthread_mutex_t)));
  int retval = pthread_mutex_init(lock_inodes_, NULL);
  assert(retval == 0);
  lock_paths_ =
    reinterpret_cast<pthread_mutex_t *>(smalloc(sizeof(pthread_mutex_t)));
  retval = pthread_mutex_init(lock_paths_, NULL);
  assert(retval == 0);
}


void ActiveInodesBuffer::InitSpecialInodes() {
  inode_references_.set_deleted_key(0);
  inode2path_->set_deleted_key(0);
  //inode_references_.set_empty_key(1);  Only required for densemap
}


void ActiveInodesBuffer::CopyFrom(const ActiveInodesBuffer &other) {
  inode2path_ = new PathMap(*other.inode2path_);
  inode_references_ = other.inode_references_;
  statistics_ = other.statistics_;
}


ActiveInodesBuffer::ActiveInodesBuffer() { 
  version_ = kVersion;
  inode2path_ = new PathMap();
  InitLocks();
  InitSpecialInodes();
}


ActiveInodesBuffer::ActiveInodesBuffer(const ActiveInodesBuffer &other) {
  assert(other.version_ == kVersion);
  version_ = kVersion;
  CopyFrom(other);
  InitLocks();
  InitSpecialInodes();
}


ActiveInodesBuffer &ActiveInodesBuffer::operator= (
  const ActiveInodesBuffer &other) 
{
  if (&other == this)
    return *this;
  
  assert(other.version_ == kVersion);
  delete inode2path_;
  CopyFrom(other);
  return *this;
}


ActiveInodesBuffer::~ActiveInodesBuffer() {
  delete inode2path_;
  pthread_mutex_destroy(lock_inodes_);
  pthread_mutex_destroy(lock_paths_);
  free(lock_inodes_);
  free(lock_paths_);
}


void ActiveInodesBuffer::VfsGet(const uint64_t inode) {
  LockInodes();
  InodeReferences::iterator iter_inodes = inode_references_.find(inode);
  if (iter_inodes != inode_references_.end()) {
    (*iter_inodes).second = (*iter_inodes).second + 1;
  } else {
    inode_references_[inode] = 1;
    atomic_inc64(&statistics_.num_inserts);
  }
  UnlockInodes();
  atomic_inc64(&statistics_.num_references);
}


void ActiveInodesBuffer::VfsPut(const uint64_t inode) {
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


bool ActiveInodesBuffer::Find(const uint64_t inode, PathString *path) {
  assert(path->IsEmpty());
  bool result = false;
  LockPaths();
  result = ConstructPath(inode, path);
  UnlockPaths();
  
  if (result) {
    atomic_inc64(&statistics_.num_ancient_hits);
    return true;
  }
  atomic_inc64(&statistics_.num_ancient_misses);
  return false;
}


bool ActiveInodesBuffer::ConstructPath(const uint64_t inode, PathString *path) {
  PathMap::const_iterator needle = inode2path_->find(inode);
  if (needle == inode2path_->end())
    return false;
  if (needle->second.name.IsEmpty())
    return true;
  
  bool retval = ConstructPath(needle->second.parent_inode, path);
  path->Append("/", 1);
  path->Append(needle->second.name.GetChars(), needle->second.name.GetLength());  
  if (!retval) {
    LogCvmfs(kLogGlueBuffer, kLogDebug | kLogSyslog, "internal error: "
             "active inode buffer inconsistent (path %s)", path->c_str());
  }
  return retval;
}


// TODO: add ancient inodes to cwd buffer if necessary
void ActiveInodesBuffer::MaterializePaths(
  catalog::AbstractCatalogManager *source)
{
  LockInodes();
  LockPaths();
  PathMap *new_map = new PathMap();
  for (InodeReferences::const_iterator i = inode_references_.begin(),
       iEnd = inode_references_.end(); i != iEnd; ++i)
  {
    PathMap::const_iterator hit = inode2path_->find(i->first);
    if (hit != inode2path_->end()) {
      // Rescue the old inode including its parents
      (*new_map)[i->first] = hit->second;
      while (!hit->second.name.IsEmpty()) {
        hit = inode2path_->find(hit->second.parent_inode);
        if (hit == inode2path_->end()) {
          LogCvmfs(kLogGlueBuffer, kLogDebug | kLogSyslog,
                   "internal error: reconstruction error for inode %"PRIu64,
                   i->first);
          break;
        }
        (*new_map)[hit->first] = hit->second;
      }
      continue;
    }
    
    // Recover the path step by step from catalog and existing entries
    uint64_t needle_inode = i->first;
    do {
      hit = new_map->find(needle_inode);
      if (hit != new_map->end())
        break;
      
      catalog::DirectoryEntry dirent;
      bool retval = source->Inode2DirentUnprotected(needle_inode, &dirent);
      atomic_inc64(&statistics_.num_dirent_lookups);
      if (!retval) {
        LogCvmfs(kLogGlueBuffer, kLogDebug | kLogSyslog, "internal error: "
                 "active inode buffer lookup failure inode %"PRIu64, i->first);
        break;
      }
      
      MinimalDirent new_entry(dirent.parent_inode(), dirent.name());
      (*new_map)[needle_inode] = new_entry;
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
}*/
