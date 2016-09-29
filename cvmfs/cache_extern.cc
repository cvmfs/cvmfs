/**
 * This file is part of the CernVM File System.
 */
#include "cvmfs_config.h"
#include "cache_extern.h"

#include <unistd.h>

#include <cassert>

#include "cache.pb.h"
#include "logging.h"
#include "util/pointer.h"
#include "util/posix.h"

using namespace std;  // NOLINT

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


ExternalCacheManager *ExternalCacheManager::Create(int fd_connection) {
  UniquePtr<ExternalCacheManager> cache_mgr(
    new ExternalCacheManager(fd_connection));
  assert(cache_mgr.IsValid());

  cvmfs::MsgClientCall msg_client_call;
  cvmfs::MsgHandshake msg_handshake;
  msg_client_call.set_allocated_msg_handshake(&msg_handshake);
  cache_mgr->transport_.SendMsg(&msg_client_call);

  cvmfs::MsgServerCall msg_ack;
  bool retval = cache_mgr->transport_.RecvMsg(&msg_ack);
  if (!retval || !msg_ack.has_msg_handshake_ack())
    return NULL;
  cache_mgr->session_id_ = msg_ack.msg_handshake_ack().session_id();
  return cache_mgr.Release();
}


void ExternalCacheManager::CtrlTxn(
  const ObjectInfo &object_info,
  const int flags,
  void *txn)
{
}


int ExternalCacheManager::Dup(int fd) {
  return -1;
}


int64_t ExternalCacheManager::GetSize(int fd) {
  return -1;
}


ExternalCacheManager::ExternalCacheManager(int fd_connection)
  : transport_(fd_connection)
  , session_id_(-1)
{
}


ExternalCacheManager::~ExternalCacheManager() {
  if (session_id_ >= 0) {
    cvmfs::MsgQuit msg_quit;
    msg_quit.set_session_id(session_id_);
    cvmfs::MsgClientCall msg_client_call;
    msg_client_call.set_allocated_msg_quit(&msg_quit);
    transport_.SendMsg(&msg_client_call);
  }
  close(transport_.fd_connection());
}


int ExternalCacheManager::Open(const BlessedObject &object) {
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
