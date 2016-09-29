/**
 * This file is part of the CernVM File System.
 */
#include "cvmfs_config.h"
#include "cache_extern.h"

#include <cassert>

#include "logging.h"

using namespace std;  // NOLINT

namespace cache {

int ExternalCacheManager::AbortTxn(void *txn) {
  return -1;
}


bool ExternalCacheManager::AcquireQuotaManager(QuotaManager *quota_mgr) {
  assert(quota_mgr != NULL);
  quota_mgr_ = quota_mgr;
  LogCvmfs(kLogCache, kLogDebug, "set quota manager");
  return true;
}


int ExternalCacheManager::Close(int fd) {
  return -1;
}


int ExternalCacheManager::CommitTxn(void *txn) {
  return -1;
}


int ExternalCacheManager::ControlTxn(
  const string &description,
  const ObjectType type,
  const int flags,
  void *txn)
{
  return -1;
}


int ExternalCacheManager::Dup(int fd) {
  return -1;
}


int64_t ExternalCacheManager::GetSize(int fd) {
  return -1;
}


ExternalCacheManager::ExternalCacheManager()
  : fd_socket_(-1)
{
}


ExternalCacheManager::~ExternalCacheManager() {
}


int ExternalCacheManager::Open(const shash::Any &id) {
  return -1;
}


int ExternalCacheManager::OpenFromTxn(void *txn) {
  return -1;
}


int64_t ExternalCacheManager::Pread(
  int fd,
  void *buf,
  uint64_t size,
  uint64_t offset)
{
  return -1;
}


int ExternalCacheManager::Readahead(int fd) {
  return -1;
}


int ExternalCacheManager::Reset(void *txn) {
  return -1;
}


uint16_t ExternalCacheManager::SizeOfTxn() {
  return 0;
}


int ExternalCacheManager::StartTxn(
  const shash::Any &id,
  uint64_t size,
  void *txn)
{
  return -1;
}


int64_t ExternalCacheManager::Write(const void *buf, uint64_t sz, void *txn) {
  return -1;
}

}  // namespace cache
