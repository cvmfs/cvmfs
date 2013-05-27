/**
 * This file is part of the CernVM File System.
 *
 * Ensures that cvmfs aborts on out-of-memory errors.
 */

#ifndef CVMFS_SMALLOC_H_
#define CVMFS_SMALLOC_H_

#include <sys/mman.h>
#include <stdlib.h>
#include <cassert>

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

static inline void * __attribute__((used)) smalloc(size_t size) {
  void *mem = malloc(size);
  assert(mem && "Out Of Memory");
  return mem;
}

static inline void * __attribute__((used)) srealloc(void *ptr, size_t size) {
  void *mem = realloc(ptr, size);
  assert(mem && "Out Of Memory");
  return mem;
}

static inline void * __attribute__((used)) scalloc(size_t count, size_t size) {
  void *mem = calloc(count, size);
  assert(mem && "Out Of Memory");
  return mem;
}

static inline void * __attribute__((used)) smmap(size_t size) {
  assert(size > 0);
  size_t pages = ((size + sizeof(size_t))+4095)/4096;
  unsigned char *mem =
    static_cast<unsigned char *>(mmap(NULL, pages*4096, PROT_READ | PROT_WRITE,
                                 MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
  assert((mem != MAP_FAILED) && "Out Of Memory");
  *((size_t *)(mem)) = pages;
  return mem + sizeof(size_t);
}

static inline void * __attribute__((used)) smunmap(void *mem) {
  unsigned char *area = static_cast<unsigned char *>(mem);
  area = area - sizeof(size_t);
  size_t pages = *((size_t *)(area));
  int retval = munmap(area, pages*4096);
  assert((retval == 0) && "Invalid umnmap");
  return mem;
}

#ifdef CVMFS_NAMESPACE_GUARD
}
#endif

#endif  // CVMFS_SMALLOC_H_
