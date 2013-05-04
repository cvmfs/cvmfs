/**
 * Implements hash functions for different key types
 */

#include "smallhash.h"
#include "MurmurHash2.h"

#include "hash.h"
#include <fuse/fuse_lowlevel.h>

uint32_t hasher_md5(const hash::Md5 &key) {
  // Don't start with the first bytes, because == is using them as well
  return (uint32_t) *((uint32_t *)key.digest + 1);
}
  
uint32_t hasher_inode(const fuse_ino_t &inode) {
  return MurmurHash2(&inode, sizeof(inode), 0x07387a4f);
}
