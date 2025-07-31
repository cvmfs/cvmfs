/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_DUPLEX_TESTING_H_
#define CVMFS_DUPLEX_TESTING_H_

#ifdef _CVMFS_UNITTESTS_ENABLED
#include "gtest/gtest_prod.h"
#else
// no unittests are being built, so FRIEND_TEST
// is not needed. Make it a no-op
//#define FRIEND_TEST(test_case_name, test_name)
#define FRIEND_TEST(test_case_name, test_name) \
  friend class test_case_name##_##test_name##_Test
#endif

#endif  // CVMFS_DUPLEX_TESTING_H_
