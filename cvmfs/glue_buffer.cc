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
  
  
/**
 * Parent inode has to be already present (or inode is a root inode)
 * \return true if the inode is new, false otherwise
 */
bool InodeContainer::Get(const uint64_t inode, const uint64_t parent_inode, 
                         const NameString &name) 
{
  LogCvmfs(kLogGlueBuffer, kLogDebug, "get inode %"PRIu64", name %s", 
           inode, name.c_str());
  InodeMap::iterator iter_inode = map_.find(inode);
  if (iter_inode != map_.end()) {
    (*iter_inode).second.references++;
    return false;
  }
  
  // New inode
  map_[inode] = Dirent(parent_inode, name);
  if (!name.IsEmpty()) {
    InodeMap::iterator iter_parent_inode = map_.find(parent_inode);
    assert(iter_parent_inode != map_.end());
    (*iter_parent_inode).second.references++;
  }
  return true;
}
  
  
/**
 * Same as Get() but don't add parent inodes reference (done before);
 * \return true if it is a new inode, false otherwise
 */
bool InodeContainer::Add(const uint64_t inode, const uint64_t parent_inode, 
                         const NameString &name)
{
  LogCvmfs(kLogGlueBuffer, kLogDebug, "add inode %"PRIu64", name %s", 
           inode, name.c_str());
  InodeMap::iterator iter_inode = map_.find(inode);
  if (iter_inode != map_.end()) {
    (*iter_inode).second.references++;
    return false;
  }
  
  // New inode
  map_[inode] = Dirent(parent_inode, name);
  return true;
}
  

/**
 * \return number of removed inodes
 */
uint32_t InodeContainer::Put(const uint64_t inode, const uint32_t by) {
  LogCvmfs(kLogGlueBuffer, kLogDebug, "put inode %"PRIu64" by %u", inode, by);
  InodeMap::iterator iter_inode = map_.find(inode);
  assert(iter_inode != map_.end());
  assert((*iter_inode).second.references >= by);
  (*iter_inode).second.references -= by;
  uint32_t result = 0;
  if ((*iter_inode).second.references == 0) {
    result = 1;
    if (!iter_inode->second.name.IsEmpty()) 
      result += Put((*iter_inode).second.parent_inode, 1);
    map_.erase(iter_inode);
  }
  return result;
}
  

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
  
  
string InodeContainer::DebugPrint() {
  string result;
  for (InodeMap::const_iterator i = map_.begin(), iEnd = map_.end(); 
       i != iEnd; ++i)
  {
    result += "[" + StringifyInt(i->first) + "]  parent: " + 
      StringifyInt(i->second.parent_inode) + "  name: " +
      i->second.name.ToString() + "  refcnt: " + 
      StringifyInt(i->second.references) + "\n";
  }
  return result;
}


//------------------------------------------------------------------------------
  
  
void InodeTracker::InitLock() {
  lock_ =
    reinterpret_cast<pthread_mutex_t *>(smalloc(sizeof(pthread_mutex_t)));
  int retval = pthread_mutex_init(lock_, NULL);
  assert(retval == 0);
}


void InodeTracker::CopyFrom(const InodeTracker &other) {
  assert(other.version_ == kVersion);
  version_ = kVersion;
  inode2path_ = other.inode2path_;
  statistics_ = other.statistics_;
}


InodeTracker::InodeTracker() { 
  version_ = kVersion;
  InitLock();
}


InodeTracker::InodeTracker(const InodeTracker &other) {
  CopyFrom(other);
  InitLock();
}


InodeTracker &InodeTracker::operator= (const InodeTracker &other) {
  if (&other == this)
    return *this;
  
  CopyFrom(other);
  return *this;
}


InodeTracker::~InodeTracker() {
  pthread_mutex_destroy(lock_);
  free(lock_);
}


/**
 * \return true if inode was processed, false otherwise if parent_inode was not
 *         present
 */
bool InodeTracker::VfsGet(const uint64_t inode, const uint64_t parent_inode,
                          const NameString &name) 
{
  Lock();
  if (!name.IsEmpty() && !inode2path_.Contains(parent_inode)) {
    Unlock();
    atomic_inc64(&statistics_.num_dangling_try);
    return false;
  }
  bool new_inode = inode2path_.Get(inode, parent_inode, name);
  Unlock();
  
  if (new_inode)
    atomic_inc64(&statistics_.num_inserts);
  atomic_inc64(&statistics_.num_references);
  return true;
}
  
  
/**
 * Assumes that the parent is already present
 * \return true if it is a new inode, false otherwise
 */
bool InodeTracker::VfsAdd(const uint64_t inode, const uint64_t parent_inode,
                          const NameString &name) 
{
  Lock();
  bool new_inode = inode2path_.Add(inode, parent_inode, name);
  Unlock();
  if (new_inode) {
    atomic_inc64(&statistics_.num_inserts);
  } else {
    atomic_inc64(&statistics_.num_references);
    atomic_inc64(&statistics_.num_double_add);
  }
  return new_inode;
}


void InodeTracker::VfsPut(const uint64_t inode, const uint32_t by) {
  Lock();
  uint32_t removed_inodes = inode2path_.Put(inode, by);
  Unlock();
  atomic_xadd64(&statistics_.num_removes, removed_inodes);
  atomic_xadd64(&statistics_.num_references, -int32_t(by));
}


bool InodeTracker::Find(const uint64_t inode, PathString *path) {
  bool result = false;
  Lock();
  result = inode2path_.ConstructPath(inode, path);
  Unlock();
  
  if (result) {
    atomic_inc64(&statistics_.num_ancient_hits);
    return true;
  }
  atomic_inc64(&statistics_.num_ancient_misses);
  return false;
}

}  // namespace glue
