/**
 * This file is part of the CernVM File System.
 */

#include "ring_buffer.h"

#include <algorithm>
#include <cassert>
#include <cstdlib>
#include <cstring>

#include "util/smalloc.h"

const RingBuffer::ObjectHandle_t RingBuffer::kInvalidObjectHandle = size_t(-1);


RingBuffer::RingBuffer(size_t total_size)
  : total_size_(total_size)
  , free_space_(total_size)
  , front_(0)
  , back_(0)
  , buffer_(reinterpret_cast<unsigned char *>(sxmmap(total_size_)))
{
  assert(total_size_ >= sizeof(size_t));
}


RingBuffer::~RingBuffer() {
  sxunmap(buffer_, total_size_);
}


void RingBuffer::Put(const void *data, size_t size) {
  const size_t size_head = std::min(size, total_size_ - front_);
  if (size_head > 0)
    memcpy(buffer_ + front_, data, size_head);

  if (size_head < size) {
    const size_t size_tail = size - size_head;
    memcpy(buffer_, reinterpret_cast<const unsigned char *>(data) + size_head,
           size_tail);
  }

  front_ = (front_ + size) % total_size_;
  free_space_ -= size;
}


void RingBuffer::Get(size_t from, size_t size, void *to) const {
  const size_t size_head = std::min(size, total_size_ - from);
  if (size_head > 0)
    memcpy(to, buffer_ + from, size_head);

  if (size_head < size) {
    const size_t size_tail = size - size_head;
    memcpy(reinterpret_cast<unsigned char *>(to) + size_head, buffer_,
           size_tail);
  }
}


void RingBuffer::Shrink(size_t by) {
  back_ = (back_ + by) % total_size_;
  free_space_ += by;
}


size_t RingBuffer::GetObjectSize(ObjectHandle_t handle) const {
  size_t size_tag;
  Get(handle, sizeof(size_tag), &size_tag);
  assert(size_tag <= total_size_);
  return size_tag;
}


RingBuffer::ObjectHandle_t RingBuffer::PushFront(const void *obj, size_t size) {
  size_t size_tag = size;
  size += sizeof(size_tag);
  if (size > free_space_) {
    return kInvalidObjectHandle;
  }

  ObjectHandle_t result = front_;

  Put(&size_tag, sizeof(size_tag));
  Put(obj, size_tag);

  return result;
}


RingBuffer::ObjectHandle_t RingBuffer::RemoveBack() {
  ObjectHandle_t result = back_;

  size_t size_tag = GetObjectSize(result);
  Shrink(sizeof(size_tag));
  Shrink(size_tag);

  return result;
}


void RingBuffer::CopyObject(ObjectHandle_t handle, void *to) const
{
  size_t size_tag = GetObjectSize(handle);
  ObjectHandle_t object = (handle + sizeof(size_tag)) % total_size_;
  Get(object, size_tag, to);
}


void RingBuffer::CopySlice(ObjectHandle_t handle, size_t size, size_t offset,
                           void *to) const
{
  ObjectHandle_t begin = (handle + sizeof(size_t) + offset) % total_size_;
  Get(begin, size, to);
}
