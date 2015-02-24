/**
 * This file is part of the CernVM File System.
 *
 * Ensures that cvmfs aborts on out-of-memory errors.
 */

#ifndef CVMFS_SMALLOC_H_
#define CVMFS_SMALLOC_H_

#include <stdint.h>
#include <stdlib.h>
#include <sys/mman.h>

#include <cassert>
// #include <cstdio>

#ifdef __APPLE__
#define PLATFORM_MAP_ANONYMOUS MAP_ANON
#else
#define PLATFORM_MAP_ANONYMOUS MAP_ANONYMOUS
#endif


#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

// Checkerboard marker (binary 101010...)
const uint32_t kMemMarker = 0xAAAAAAAA;

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
  size_t pages = ((size + 2*sizeof(size_t))+4095)/4096;
  unsigned char *mem =
    static_cast<unsigned char *>(mmap(NULL, pages*4096, PROT_READ | PROT_WRITE,
                                 MAP_PRIVATE | PLATFORM_MAP_ANONYMOUS, -1, 0));
  // printf("SMMAP %d bytes at %p\n", pages*4096, mem);
  assert((mem != MAP_FAILED) && "Out Of Memory");
  *(reinterpret_cast<size_t *>(mem)) = kMemMarker;
  *(reinterpret_cast<size_t *>(mem) + 1) = pages;
  return mem + 2*sizeof(size_t);
}

static inline void __attribute__((used)) smunmap(void *mem) {
  unsigned char *area = static_cast<unsigned char *>(mem);
  area = area - sizeof(size_t);
  size_t pages = *(reinterpret_cast<size_t *>(area));
  int retval = munmap(area-sizeof(size_t), pages*4096);
  // printf("SUNMMAP %d bytes at %p\n", pages*4096, area);
  assert((retval == 0) && "Invalid umnmap");
}

#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif

#endif  // CVMFS_SMALLOC_H_
