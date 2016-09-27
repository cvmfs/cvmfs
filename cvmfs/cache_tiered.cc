/**
 * This file is part of the CernVM File System.
 */
#include "cvmfs_config.h"
#include "cache_tiered.h"

#include <errno.h>

#include <string>
#include <vector>

#include "platform.h"

int TieredCacheManager::Open(const BlessedObject &object) {
  int fd = upper_->Open(object);
  if ((fd >= 0) || (fd != -ENOENT)) {return fd;}

  int fd2 = lower_->Open(object);
  if (fd2 < 0) {return fd;}  // NOTE: use error code from upper.

  // Lower cache hit; upper cache miss.  Copy object into the
  // upper cache.
  int64_t size = lower_->GetSize(fd2);
  if (size < 0) {
    lower_->Close(fd2);
    return fd;
  }

  void *txn = alloca(upper_->SizeOfTxn());
  if (upper_->StartTxn(object.id, size, txn) < 0) {
    lower_->Close(fd2);
    return fd;
  }
  upper_->CtrlTxn(object.info, 0, txn);

  std::vector<char> m_buffer;
  m_buffer.reserve(kCopyBufferSize);
  uint64_t remaining = size;
  uint64_t offset = 0;
  while (remaining > 0) {
    unsigned nbytes = remaining > kCopyBufferSize ? kCopyBufferSize : remaining;
    int64_t result = lower_->Pread(fd2, &m_buffer[0], nbytes, offset);
    // The file we are reading is supposed to be exactly `size` bytes.
    if ((result < 0) || (result != nbytes)) {
      lower_->Close(fd2);
      upper_->AbortTxn(txn);
      return fd;
    }
    result = upper_->Write(&m_buffer[0], nbytes, txn);
    if (result < 0) {
      lower_->Close(fd2);
      upper_->AbortTxn(txn);
      return fd;
    }
    offset += nbytes;
    remaining -= nbytes;
  }
  lower_->Close(fd2);
  int fd_return = upper_->OpenFromTxn(txn);
  if (fd_return < 0) {
    upper_->AbortTxn(txn);
    return fd;
  }
  if (upper_->CommitTxn(txn) < 0) {
    upper_->Close(fd_return);
    return fd;
  }
  return fd_return;
}


int TieredCacheManager::StartTxn(const shash::Any &id, uint64_t size, void *txn)
{
  int upper_result = upper_->StartTxn(id, size, txn);
  if (upper_result < 0) {
    return upper_result;
  }

  void *txn2 = static_cast<char *>(txn) + upper_->SizeOfTxn();
  int lower_result = lower_->StartTxn(id, size, txn2);
  if (lower_result < 0) {
    upper_->AbortTxn(txn);
  }
  return lower_result;
}


void TieredCacheManager::CtrlTxn(
  const ObjectInfo &object_info,
  const int flags,
  void *txn)
{
  upper_->CtrlTxn(object_info, flags, txn);
  void *txn2 = static_cast<char*>(txn) + upper_->SizeOfTxn();
  lower_->CtrlTxn(object_info, flags, txn2);
}


int64_t TieredCacheManager::Write(const void *buf, uint64_t size, void *txn) {
  int upper_result = upper_->Write(buf, size, txn);
  if (upper_result < 0) { return upper_result; }

  void *txn2 = static_cast<char*>(txn) + upper_->SizeOfTxn();
  return lower_->Write(buf, size, txn2);
}


int TieredCacheManager::Reset(void *txn) {
  int upper_result = upper_->Reset(txn);

  void *txn2 = static_cast<char*>(txn) + upper_->SizeOfTxn();
  int lower_result = lower_->Reset(txn2);

  return (upper_result < 0) ? upper_result : lower_result;
}


int TieredCacheManager::AbortTxn(void *txn) {
  int upper_result = upper_->AbortTxn(txn);

  void *txn2 = static_cast<char*>(txn) + upper_->SizeOfTxn();
  int lower_result = lower_->AbortTxn(txn2);

  return (upper_result < 0) ? upper_result : lower_result;
}


int TieredCacheManager::CommitTxn(void *txn) {
  int upper_result = upper_->CommitTxn(txn);

  void *txn2 = static_cast<char*>(txn) + upper_->SizeOfTxn();
  int lower_result = lower_->CommitTxn(txn2);

  return (upper_result < 0) ? upper_result : lower_result;
}
