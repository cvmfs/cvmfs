/**
 * This file is part of the CernVM File System.
 */

#include "cache.h"

#include <alloca.h>
#include <errno.h>

#include <cassert>
#include <cstdlib>
#include <string>

#include "compression/compression.h"
#include "crypto/hash.h"
#include "directory_entry.h"
#include "network/download.h"
#include "quota.h"
#include "util/posix.h"
#include "util/smalloc.h"

using namespace std;  // NOLINT

const uint64_t CacheManager::kSizeUnknown = uint64_t(-1);


CacheManager::CacheManager() : quota_mgr_(new NoopQuotaManager()) { }


CacheManager::~CacheManager() { delete quota_mgr_; }


/**
 * Compresses and checksums the file pointed to by fd.  The hash algorithm needs
 * to be set in id.
 */
int CacheManager::ChecksumFd(int fd, shash::Any *id) {
  shash::ContextPtr hash_context(id->algorithm);
  hash_context.buffer = alloca(hash_context.size);
  shash::Init(hash_context);

  z_stream strm;
  zlib::CompressInit(&strm);
  zlib::StreamStates retval;

  unsigned char buf[4096];
  uint64_t pos = 0;
  bool eof;

  do {
    const int64_t nbytes = Pread(fd, buf, 4096, pos);
    if (nbytes < 0) {
      zlib::CompressFini(&strm);
      return nbytes;
    }
    pos += nbytes;
    eof = nbytes < 4096;
    retval = zlib::CompressZStream2Null(buf, nbytes, eof, &strm, &hash_context);
    if (retval == zlib::kStreamDataError) {
      zlib::CompressFini(&strm);
      return -EINVAL;
    }
  } while (!eof);

  zlib::CompressFini(&strm);
  if (retval != zlib::kStreamEnd)
    return -EINVAL;
  shash::Final(hash_context, id);
  return 0;
}


/**
 * Commits the memory blob buffer to the given chunk id.  No checking!
 * The hash and the memory blob need to match.
 */
bool CacheManager::CommitFromMem(const LabeledObject &object,
                                 const unsigned char *buffer,
                                 const uint64_t size) {
  void *txn = alloca(this->SizeOfTxn());
  const int fd = this->StartTxn(object.id, size, txn);
  if (fd < 0)
    return false;
  this->CtrlTxn(object.label, 0, txn);
  int64_t retval = this->Write(buffer, size, txn);
  if ((retval < 0) || (static_cast<uint64_t>(retval) != size)) {
    this->AbortTxn(txn);
    return false;
  }
  retval = this->CommitTxn(txn);
  return retval == 0;
}


void CacheManager::FreeState(const int fd_progress, void *data) {
  State *state = reinterpret_cast<State *>(data);
  if (fd_progress >= 0)
    SendMsg2Socket(fd_progress, "Releasing saved open files table\n");
  assert(state->version == kStateVersion);
  assert(state->manager_type == id());
  const bool result = DoFreeState(state->concrete_state);
  if (!result) {
    if (fd_progress >= 0) {
      SendMsg2Socket(fd_progress,
                     "   *** Releasing open files table failed!\n");
    }
    abort();
  }
  delete state;
}


/**
 * Tries to open a file and copies its contents into a newly malloc'd
 * memory area.  User of the function has to free buffer (if successful).
 *
 * @param[in] id content hash of the catalog entry.
 * @param[out] buffer Contents of the file
 * @param[out] size Size of the file
 * \return True if successful, false otherwise.
 */
bool CacheManager::Open2Mem(const LabeledObject &object,
                            unsigned char **buffer,
                            uint64_t *size) {
  *size = 0;
  *buffer = NULL;

  const int fd = this->Open(object);
  if (fd < 0)
    return false;

  const int64_t s = this->GetSize(fd);
  assert(s >= 0);
  *size = static_cast<uint64_t>(s);

  int64_t retval = 0;
  if (*size > 0) {
    *buffer = static_cast<unsigned char *>(smalloc(*size));
    retval = this->Pread(fd, *buffer, *size, 0);
  } else {
    *buffer = NULL;
  }

  this->Close(fd);
  if ((retval < 0) || (static_cast<uint64_t>(retval) != *size)) {
    free(*buffer);
    *buffer = NULL;
    *size = 0;
    return false;
  }
  return true;
}


/**
 * Uses the regular open and, if the file exists in the cache, pins it.  There
 * is a race condition: the file can be removed between the open and the Pin.
 * This is fixed by the quota manager's unpin method that removes files which
 * do not exist anymore in the cache.  (The quota manager also translates double
 * pins into a no-op, so that the accounting does not get out of sync.)
 */
int CacheManager::OpenPinned(const LabeledObject &object) {
  const int fd = this->Open(object);
  if (fd >= 0) {
    const int64_t size = this->GetSize(fd);
    if (size < 0) {
      this->Close(fd);
      return size;
    }
    const bool retval = quota_mgr_->Pin(object.id, static_cast<uint64_t>(size),
                                        object.label.GetDescription(),
                                        object.label.IsCatalog());
    if (!retval) {
      this->Close(fd);
      return -ENOSPC;
    }
  }
  return fd;
}


int CacheManager::RestoreState(const int fd_progress, void *data) {
  State *state = reinterpret_cast<State *>(data);
  if (fd_progress >= 0)
    SendMsg2Socket(fd_progress, "Restoring open files table... ");
  if (state->version != kStateVersion) {
    if (fd_progress >= 0)
      SendMsg2Socket(fd_progress, "unsupported state version!\n");
    abort();
  }
  if (state->manager_type != id()) {
    if (fd_progress >= 0)
      SendMsg2Socket(fd_progress, "switching cache manager unsupported!\n");
    abort();
  }
  const int new_root_fd = DoRestoreState(state->concrete_state);
  if (new_root_fd < -1) {
    if (fd_progress >= 0)
      SendMsg2Socket(fd_progress, "FAILED!\n");
    abort();
  }
  if (fd_progress >= 0)
    SendMsg2Socket(fd_progress, "done\n");
  return new_root_fd;
}


/**
 * The actual work is done in the concrete cache managers.
 */
void *CacheManager::SaveState(const int fd_progress) {
  if (fd_progress >= 0)
    SendMsg2Socket(fd_progress, "Saving open files table\n");
  State *state = new State();
  state->manager_type = id();
  state->concrete_state = DoSaveState();
  if (state->concrete_state == NULL) {
    if (fd_progress >= 0) {
      SendMsg2Socket(
          fd_progress,
          "  *** This cache manager does not support saving state!\n");
    }
    abort();
  }
  return state;
}
