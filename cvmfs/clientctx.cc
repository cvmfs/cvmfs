/**
 * This file is part of the CernVM File System.
 */

#include "clientctx.h"

#include <cassert>

ClientCtx *ClientCtx::instance_ = NULL;

namespace {

void TLSDestructor(void *data) {
  ClientCtx::ThreadLocalStorage *tls =
    static_cast<ClientCtx::ThreadLocalStorage *>(data);
  delete tls;
}

}

void ClientCtx::CleanupInstance() {
  if (instance_ == NULL)
    return;

  int retval = pthread_key_delete(instance_->thread_local_storage_);
  assert(retval == 0);
  delete instance_;
  instance_ = NULL;
}


ClientCtx *ClientCtx::GetInstance() {
  if (instance_ == NULL) {
    instance_ = new ClientCtx();
    int retval =
      pthread_key_create(&instance_->thread_local_storage_, TLSDestructor);
    assert(retval == 0);
  }

  return instance_;
}


void ClientCtx::Get(uid_t *uid, gid_t *gid, pid_t *pid) {
  ThreadLocalStorage *tls = static_cast<ThreadLocalStorage *>(
    pthread_getspecific(thread_local_storage_));
  if ((tls == NULL) || !tls->is_set) {
    *uid = -1;
    *gid = -1;
    *pid = -1;
  } else {
    *uid = tls->uid;
    *gid = tls->gid;
    *pid = tls->pid;
  }
}


bool ClientCtx::IsSet() {
  ThreadLocalStorage *tls = static_cast<ThreadLocalStorage *>(
    pthread_getspecific(thread_local_storage_));
  if (tls == NULL)
    return false;

  return tls->is_set;
}


void ClientCtx::Set(uid_t uid, gid_t gid, pid_t pid) {
  ThreadLocalStorage *tls = static_cast<ThreadLocalStorage *>(
    pthread_getspecific(thread_local_storage_));

  if (tls == NULL) {
    tls = new ThreadLocalStorage(uid, gid, pid);
    int retval = pthread_setspecific(thread_local_storage_, tls);
    assert(retval == 0);
  } else {
    tls->uid = uid;
    tls->gid = gid;
    tls->pid = pid;
    tls->is_set = true;
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
  }
}
