/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include "fs_traversal_interface.h"
#include "fs_traversal_posix.h"

template<class T>
struct TestFsTraversalInterface : public ::testing::Test
{
    static T traversal_interface;
};

TYPED_TEST_CASE_P(TestFsTraversalInterface);

TYPED_TEST_P(TestFsTraversalInterface, CreateDirectoriesTest)
{
  ASSERT_FALSE(true);
}

REGISTER_TYPED_TEST_CASE_P(TestFsTraversalInterface, CreateDirectoriesTest);


typedef ::testing::Types<fs_traversal> Traversal;


template<> fs_traversal TestFsTraversalInterface<fs_traversal>
  ::traversal_interface(posix_get_interface());
