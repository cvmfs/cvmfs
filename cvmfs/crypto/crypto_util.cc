/**
 * This file is part of the CernVM File System.
 */

#include "crypto/crypto_util.h"

#include <openssl/rand.h>

void crypto::InitRng() {
  RAND_poll();
}
