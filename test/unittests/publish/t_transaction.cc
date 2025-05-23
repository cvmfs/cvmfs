/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include "c_repository.h"
#include "publish/except.h"
#include "publish/repository.h"
#include "publish/settings.h"

using namespace std;  // NOLINT

namespace publish {

class T_Transaction : public ::testing::Test {
 protected:
  virtual void SetUp() { }

 protected:
};

TEST_F(T_Transaction, TemplateSettings) {
  SettingsTransaction settings("test.cvmfs.io");
  EXPECT_FALSE(settings.HasTemplate());
  EXPECT_THROW(settings.SetTemplate("", "/foo"), EPublish);
  EXPECT_THROW(settings.SetTemplate("/foo", ""), EPublish);
  settings.SetTemplate("/foo", "/bar");
  EXPECT_TRUE(settings.HasTemplate());
  EXPECT_EQ("foo", settings.template_from());
  EXPECT_EQ("bar", settings.template_to());
}


TEST_F(T_Transaction, BasicTransaction) {
  Publisher *publisher = GetTestPublisher();
  EXPECT_FALSE(publisher->in_transaction().IsSet());
  publisher->Transaction();
  EXPECT_TRUE(publisher->in_transaction().IsSet());
  delete publisher;

  // TODO(jblomer): add more tests when publish and abort are implemented
}

}  // namespace publish
