/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_RING_BUFFER_H_
#define CVMFS_RING_BUFFER_H_

#include <cstddef>

#include "util/single_copy.h"

/**
 * A ring buffer that allows appending objects to the front and removing
 * objects from the back. In the memory area, we prepend the object data with
 * the size of the object.
 */
class RingBuffer : SingleCopy {
 public:
  /**
   * The offset in buffer_
   */
  typedef size_t ObjectHandle_t;
  static const ObjectHandle_t kInvalidObjectHandle;

  explicit RingBuffer(size_t total_size);
  ~RingBuffer();

  /**
   * Returns kInvalidObjectHandle if there is not enough space to insert the
   * object.
   */
  ObjectHandle_t PushFront(const void *obj, size_t size);

  /**
   * Returns the handle of the remove object or kInvalidObjectHandle if the
   * ring buffer is empty
   */
  ObjectHandle_t RemoveBack();

  /**
   * The passed object handle must be valid
   */
  size_t GetObjectSize(ObjectHandle_t handle) const;

  /**
   * Note that we cannot just return a pointer because an object may be
   * split between the end and the beginning of the backing memory area
   */
  void CopyObject(ObjectHandle_t handle, void *to) const;

  /**
   * Copies a sub range of the object
   */
  void CopySlice(ObjectHandle_t handle, size_t size, size_t offset,
                 void *to) const;

  // All objects are prepended by a size tag, so we can store objects only
  // up to the available space minus the size of the size tag
  size_t GetMaxObjectSize() const { return total_size_ - sizeof(size_t); }
  bool HasSpaceFor(size_t size) const {
    return free_space_ >= size + sizeof(size_t);
  }

  size_t free_space() const { return free_space_; }

 private:
  /**
   * Writes data into the ring buffer at front_, taking care of a potential
   * buffer wrap. Assumes that the buffer has enough space.
   */
  void Put(const void *data, size_t size);
  /**
   * Copies data from the buffer to the memory area "to". The memory area
   * has to be big enough. Takes care of a potential buffer wrap.
   */
  void Get(size_t from, size_t size, void *to) const;
  /**
   * Moves the back_ pointer forward, taking care of potential buffer wraps
   */
  void Shrink(size_t by);

  /**
   * Size in bytes of the memory area backing the ring buffer
   */
  const size_t total_size_;
  /**
   * Size in bytes of the unused space in the buffer
   */
  size_t free_space_;
  /**
   * Pointer to the front of the memory buffer, in [0, size_ - 1]
   */
  size_t front_;
  /**
   * Pointer to the back of the memory buffer, in [0, size_ - 1]
   */
  size_t back_;
  /**
   * The memory area backing the ring buffer
   */
  unsigned char *buffer_;
};  // class RingBuffer

#endif  // CVMFS_RING_H_
