/**
 * This file is part of the CernVM File System.
 */

#include "util.h"

#include "hash.h"
#include "libcvmfs.h"
#include "xattr.h"

shash::Any HashMeta(struct cvmfs_stat *stat_info) {
  // TODO(steuber): Can we do any better here?
  shash::Any meta_hash(shash::kMd5);
  unsigned min_buffer_size = sizeof(mode_t)
    + 1
    + sizeof(uid_t)
    + 1
    + sizeof(gid_t)
    + 1;
  XattrList *xlist = reinterpret_cast<XattrList *>(stat_info->cvm_xattrs);
  unsigned char *xlist_buffer;
  unsigned xlist_buffer_size;
  xlist->Serialize(&xlist_buffer, &xlist_buffer_size);
  unsigned char buffer[min_buffer_size+xlist_buffer_size];
  unsigned offset = 0;
  // Add mode
  *(buffer+0) = stat_info->st_mode;
  offset+=sizeof(mode_t);
  *(buffer+offset) = 0;
  offset+=1;
  // Add uid
  *(buffer+offset) = stat_info->st_uid;
  offset+=sizeof(uid_t);
  *(buffer+offset) = 0;
  offset+=1;
  // Add gid
  *(buffer+offset) = stat_info->st_gid;
  offset+=sizeof(gid_t);
  *(buffer+offset) = 0;
  offset+=1;
  // Add xlist
  memcpy(buffer+offset, xlist_buffer, xlist_buffer_size);
  delete xlist_buffer;
  // Hash
  shash::HashMem(buffer, min_buffer_size+xlist_buffer_size, &meta_hash);
  return meta_hash;
}
