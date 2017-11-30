/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <cstdio>

#include "duplex_ssl.h"

using namespace std;  // NOLINT

TEST(T_S3Fanout, Init) {
#ifdef OPENSSL_API_INTERFACE_V09
  printf("Skipping!\n");
#else
#endif
}

