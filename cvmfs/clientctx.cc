/**
 * This file is part of the CernVM File System.
 */

#include "clientctx.h"

#include <cassert>

#include "interrupt.h"
#include "util/concurrency.h"
#include "util/smalloc.h"

using namespace std;  // NOLINT

ClientCtx *ClientCtx::instance_ = NULL;


void ClientCtx::CleanupInstance() {
  delete instance_;
  instance_ = NULL;
}


ClientCtx::ClientCtx() {
  lock_tls_blocks_ = reinterpret_cast<pthread_mutex_t *>(
      smalloc(sizeof(pthread_mutex_t)));
  const int retval = pthread_mutex_init(lock_tls_blocks_, NULL);
  assert(retval == 0);
}


ClientCtx::~ClientCtx() {
  pthread_mutex_destroy(lock_tls_blocks_);
  free(lock_tls_blocks_);

  for (unsigned i = 0; i < tls_blocks_.size(); ++i) {
    delete tls_blocks_[i];
  }

  const int retval = pthread_key_delete(thread_local_storage_);
  assert(retval == 0);
}


ClientCtx *ClientCtx::GetInstance() {
  if (instance_ == NULL) {
    instance_ = new ClientCtx();
    const int retval = pthread_key_create(&instance_->thread_local_storage_,
                                          TlsDestructor);
    assert(retval == 0);
  }

  return instance_;
}


void ClientCtx::Get(uid_t *uid, gid_t *gid, pid_t *pid, InterruptCue **ic) {
  ThreadLocalStorage *tls = static_cast<ThreadLocalStorage *>(
      pthread_getspecific(thread_local_storage_));
  if ((tls == NULL) || !tls->is_set) {
    *uid = -1;
    *gid = -1;
    *pid = -1;
    *ic = NULL;
  } else {
    *uid = tls->uid;
    *gid = tls->gid;
    *pid = tls->pid;
    *ic = tls->interrupt_cue;
  }
}


bool ClientCtx::IsSet() {
  ThreadLocalStorage *tls = static_cast<ThreadLocalStorage *>(
      pthread_getspecific(thread_local_storage_));
  if (tls == NULL)
    return false;

  return tls->is_set;
}


void ClientCtx::Set(uid_t uid, gid_t gid, pid_t pid, InterruptCue *ic) {
  ThreadLocalStorage *tls = static_cast<ThreadLocalStorage *>(
      pthread_getspecific(thread_local_storage_));

  if (tls == NULL) {
    tls = new ThreadLocalStorage(uid, gid, pid, ic);
    const int retval = pthread_setspecific(thread_local_storage_, tls);
    assert(retval == 0);
    const MutexLockGuard lock_guard(lock_tls_blocks_);
    tls_blocks_.push_back(tls);
  } else {
    tls->uid = uid;
    tls->gid = gid;
    tls->pid = pid;
    tls->interrupt_cue = ic;
    tls->is_set = true;
  }
}


void ClientCtx::TlsDestructor(void *data) {
  ThreadLocalStorage *tls = static_cast<ClientCtx::ThreadLocalStorage *>(data);
  delete tls;

  assert(instance_);
  const MutexLockGuard lock_guard(instance_->lock_tls_blocks_);
  for (vector<ThreadLocalStorage *>::iterator
           i = instance_->tls_blocks_.begin(),
           iEnd = instance_->tls_blocks_.end();
       i != iEnd;
       ++i) {
    if ((*i) == tls) {
      instance_->tls_blocks_.erase(i);
      break;
    }
  }
}


void ClientCtx::Unset() {
  ThreadLocalStorage *tls = static_cast<ThreadLocalStorage *>(
      pthread_getspecific(thread_local_storage_));
  if (tls != NULL) {
    tls->is_set = false;
    tls->uid = -1;
    tls->gid = -1;
    tls->pid = -1;
    tls->interrupt_cue = NULL;
  }
}
