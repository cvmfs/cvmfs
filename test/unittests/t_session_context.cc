/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <session_context.h>

class T_SessionContext : public ::testing::Test {};

TEST_F(T_SessionContext, Dummy) { upload::SessionContext ctx; }
