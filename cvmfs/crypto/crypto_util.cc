/**
 * This file is part of the CernVM File System.
 */

#include "crypto/crypto_util.h"

#include <openssl/crypto.h>
#include <openssl/rand.h>
#include <pthread.h>

#include <cassert>

#include "crypto/openssl_version.h"
#include "util/platform.h"

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

#ifndef OPENSSL_API_INTERFACE_V11
namespace {

pthread_mutex_t *gLibcryptoLocks = NULL;

static void CallbackLibcryptoLock(int mode, int type, const char * /* file */,
                                  int /* line */) {
  int retval;

  if (mode & CRYPTO_LOCK) {
    retval = pthread_mutex_lock(&(gLibcryptoLocks[type]));
  } else {
    retval = pthread_mutex_unlock(&(gLibcryptoLocks[type]));
  }
  assert(retval == 0);
}

static unsigned long CallbackLibcryptoThreadId() {  // NOLINT(runtime/int)
  return platform_gettid();
}

}  // anonymous namespace
#endif


void crypto::InitRng() { RAND_poll(); }


void crypto::SetupLibcryptoMt() {
#ifndef OPENSSL_API_INTERFACE_V11
  if (gLibcryptoLocks != NULL)
    return;

  gLibcryptoLocks = static_cast<pthread_mutex_t *>(
      OPENSSL_malloc(CRYPTO_num_locks() * sizeof(pthread_mutex_t)));
  for (int i = 0; i < CRYPTO_num_locks(); ++i) {
    int retval = pthread_mutex_init(&(gLibcryptoLocks[i]), NULL);
    assert(retval == 0);
  }

  CRYPTO_set_id_callback(CallbackLibcryptoThreadId);
  CRYPTO_set_locking_callback(CallbackLibcryptoLock);
#endif
}


void crypto::CleanupLibcryptoMt() {
#ifndef OPENSSL_API_INTERFACE_V11
  if (gLibcryptoLocks == NULL)
    return;

  CRYPTO_set_locking_callback(NULL);
  for (int i = 0; i < CRYPTO_num_locks(); ++i)
    pthread_mutex_destroy(&(gLibcryptoLocks[i]));

  OPENSSL_free(gLibcryptoLocks);
  gLibcryptoLocks = NULL;
#endif
}

#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif
