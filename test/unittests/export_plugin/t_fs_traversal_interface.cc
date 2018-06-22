/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include "fs_traversal_interface.h"
#include "fs_traversal_posix.h"

template <typename T>
class FsTraversalTest: public ::testing::Test {

};

TYPED_TEST_CASE_P(FsTraversalTest);