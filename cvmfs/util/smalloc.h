/**
 * This file is part of the CernVM File System.
 *
 * Ensures that cvmfs aborts on out-of-memory errors.
 */

#ifndef CVMFS_UTIL_SMALLOC_H_
#define CVMFS_UTIL_SMALLOC_H_

#include <stdint.h>
#include <stdlib.h>
#include <sys/mman.h>

#include <cassert>
// #include <cstdio>

#include <limits>

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

/**
 * Round up size to the next larger multiple of 8.  This is used to enforce
 * 8-byte alignment.
 */
static inline uint64_t RoundUp8(const uint64_t size) {
  return (size + 7) & ~static_cast<uint64_t>(7);
}

static inline void * __attribute__((used)) smalloc(size_t size) {
  void *mem = NULL;
#ifdef CVMFS_SUPPRESS_ASSERTS
  do {
#endif
    mem = malloc(size);
#ifdef CVMFS_SUPPRESS_ASSERTS
  } while ((size > 0) && (mem == NULL));
#endif
  assert((mem || (size == 0)) && "Out Of Memory");
  return mem;
}

static inline void * __attribute__((used)) srealloc(void *ptr, size_t size) {
  void *mem = NULL;

#ifdef CVMFS_SUPPRESS_ASSERTS
  do {
#endif
    mem = realloc(ptr, size);
#ifdef CVMFS_SUPPRESS_ASSERTS
  } while ((size > 0) && (mem == NULL));
#endif
  assert((mem || (size == 0)) && "Out Of Memory");
  return mem;
}

static inline void * __attribute__((used)) scalloc(size_t count, size_t size) {
  void *mem = NULL;

#ifdef CVMFS_SUPPRESS_ASSERTS
  do {
#endif
    mem = calloc(count, size);
#ifdef CVMFS_SUPPRESS_ASSERTS
  } while ((count * size > 0) && (mem == NULL));
#endif
  assert((mem || ((count * size) == 0)) && "Out Of Memory");
  return mem;
}

static inline void * __attribute__((used)) smmap(size_t size) {
  // TODO(reneme): make page size platform independent
  assert(size > 0);
  assert(size < std::numeric_limits<size_t>::max() - 4096);

  const int anonymous_fd = -1;
  const off_t offset = 0;
  size_t pages = ((size + 2 * sizeof(size_t)) + 4095)
                 / 4096;  // round to full page
  unsigned char *mem = NULL;

#ifdef CVMFS_SUPPRESS_ASSERTS
  do {
#endif
    mem = static_cast<unsigned char *>(
        mmap(NULL,
             pages * 4096,
             PROT_READ | PROT_WRITE,
             MAP_PRIVATE | PLATFORM_MAP_ANONYMOUS,
             anonymous_fd,
             offset));

#ifdef CVMFS_SUPPRESS_ASSERTS
  } while (mem == MAP_FAILED);
#endif
  // printf("SMMAP %d bytes at %p\n", pages*4096, mem);
  // NOLINTNEXTLINE(performance-no-int-to-ptr)
  assert((mem != MAP_FAILED) && "Out Of Memory");
  *(reinterpret_cast<size_t *>(mem)) = kMemMarker;
  *(reinterpret_cast<size_t *>(mem) + 1) = pages;
  return mem + 2 * sizeof(size_t);
}

static inline void __attribute__((used)) smunmap(void *mem) {
  unsigned char *area = static_cast<unsigned char *>(mem);
  area = area - sizeof(size_t);
  size_t pages = *(reinterpret_cast<size_t *>(area));
  int retval = munmap(area - sizeof(size_t), pages * 4096);
  // printf("SUNMMAP %d bytes at %p\n", pages*4096, area);
  assert((retval == 0) && "Invalid umnmap");
}


/**
 * Used when the caller remembers the size, so that it can call sxunmap later.
 */
static inline void * __attribute__((used)) sxmmap(size_t size) {
  const int anonymous_fd = -1;
  const off_t offset = 0;
  void *mem = NULL;

#ifdef CVMFS_SUPPRESS_ASSERTS
  do {
#endif
    mem = mmap(NULL,
               size,
               PROT_READ | PROT_WRITE,
               MAP_PRIVATE | PLATFORM_MAP_ANONYMOUS,
               anonymous_fd,
               offset);

#ifdef CVMFS_SUPPRESS_ASSERTS
  } while (mem == MAP_FAILED);
#endif
  // NOLINTNEXTLINE(performance-no-int-to-ptr)
  assert((mem != MAP_FAILED) && "Out Of Memory");
  return mem;
}


/**
 * Free memory acquired by sxmmap.
 */
static inline void __attribute__((used)) sxunmap(void *mem, size_t size) {
  int retval = munmap(mem, size);
  assert((retval == 0) && "Invalid umnmap");
}


/**
 * Pointer is aligned at a multiple of the size.  The size has to be a multiple
 * of 2MB.
 */
static inline void * __attribute__((used)) sxmmap_align(size_t size) {
  assert((size % (2 * 1024 * 1024)) == 0);
  char *mem = NULL;
#ifdef CVMFS_SUPPRESS_ASSERTS
  do {
#endif
    mem = reinterpret_cast<char *>(sxmmap(2 * size));
#ifdef CVMFS_SUPPRESS_ASSERTS
  } while (mem == MAP_FAILED);
#endif

  uintptr_t head = size - (uintptr_t(mem) % size);
  sxunmap(mem, head);
  mem += head;
  uintptr_t tail = size - head;
  if (tail > 0)
    sxunmap(mem + size, tail);
  return mem;
}


#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif

#endif  // CVMFS_UTIL_SMALLOC_H_
