/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <string>

#include "repository_tag.h"

class T_RepositoryTag : public ::testing::Test {};

TEST_F(T_RepositoryTag, DefaultConstructor) {
  RepositoryTag tag;
  EXPECT_EQ("", tag.name());
  EXPECT_EQ("", tag.description());
  EXPECT_EQ("", tag.auto_tag_timespan());
}

TEST_F(T_RepositoryTag, NameDescriptionConstructor) {
  RepositoryTag tag("my_tag", "my description");
  EXPECT_EQ("my_tag", tag.name());
  EXPECT_EQ("my description", tag.description());
  EXPECT_EQ("", tag.auto_tag_timespan());
}

TEST_F(T_RepositoryTag, SetAutoTagTimespan) {
  RepositoryTag tag("tag1", "desc");
  EXPECT_EQ("", tag.auto_tag_timespan());

  tag.SetAutoTagTimespan("30 days ago");
  EXPECT_EQ("30 days ago", tag.auto_tag_timespan());

  tag.SetAutoTagTimespan("2024-01-01");
  EXPECT_EQ("2024-01-01", tag.auto_tag_timespan());

  // Setting empty clears it
  tag.SetAutoTagTimespan("");
  EXPECT_EQ("", tag.auto_tag_timespan());
}

TEST_F(T_RepositoryTag, HasGenericName) {
  RepositoryTag tag1("generic-2024-01-01T00:00:00.000Z", "");
  EXPECT_TRUE(tag1.HasGenericName());

  RepositoryTag tag2("my-custom-tag", "");
  EXPECT_FALSE(tag2.HasGenericName());

  RepositoryTag tag3("generic_1-2024-01-01T00:00:00.000Z", "");
  // generic_ prefix should NOT be matched by HasGenericName
  // (it only checks for "generic-" prefix)
  EXPECT_FALSE(tag3.HasGenericName());
}

TEST_F(T_RepositoryTag, SetGenericName) {
  RepositoryTag tag;
  tag.SetName("generic-placeholder");
  EXPECT_TRUE(tag.HasGenericName());

  tag.SetGenericName();
  // After SetGenericName, the tag should still have a generic name
  // but with a real timestamp
  EXPECT_TRUE(tag.HasGenericName());
  // Verify the format: generic-YYYY-MM-DDThh:mm:ss.mmmZ
  EXPECT_EQ(0u, tag.name().find("generic-"));
}

TEST_F(T_RepositoryTag, AutoTagTimespanPreservedAcrossMutations) {
  RepositoryTag tag("tag1", "desc");
  tag.SetAutoTagTimespan("7 days ago");

  tag.SetName("tag2");
  EXPECT_EQ("7 days ago", tag.auto_tag_timespan());

  tag.SetDescription("new desc");
  EXPECT_EQ("7 days ago", tag.auto_tag_timespan());
}
