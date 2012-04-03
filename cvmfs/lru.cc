

#include "lru.h"

namespace lru {

uint32_t hasher_md5(const hash::Md5 &key) {
  return (uint32_t) *((uint32_t *)key.digest);
}

uint32_t hasher_inode(const fuse_ino_t &inode) {
#ifdef __x86_64__
  return MurmurHash64A(&inode, sizeof(inode), 0x9ce603115bba659bLLU);
#else
  return MurmurHash2(&inode, sizeof(inode), 0x07387a4f);
#endif
}

}
