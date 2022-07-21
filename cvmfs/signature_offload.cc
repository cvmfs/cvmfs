/**
 * This file is part of the CernVM File System.
 *
 * A standalone utility that executes SignatureManager::Sign().
 * This is a transitional workaround for EL9 / OpenSSL 3; it is necessary
 * because setuid binaries (and binaries with elevated file capailities)
 * cannot use SignatureManager::Sign themselves even with the environemnt
 * variable OPENSSL_ENABLE_SHA1_SIGNATURES defined.
 * It buys us time until a proper fix is found.
 */

#include "signature.h"

int main() {
  return 0;
}
