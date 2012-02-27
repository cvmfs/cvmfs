/**
 * This file is part of the CernVM File System.
 *
 * Ensures that cvmfs aborts on out-of-memory errors.
 */

#ifndef CVMFS_SMALLOC_H_
#define CVMFS_SMALLOC_H_

#include <stdlib.h>
#include <cassert>

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

#endif  // CVMFS_SMALLOC_H_
