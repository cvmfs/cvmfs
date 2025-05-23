/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <string>

#include "publish/command.h"

using namespace std;  // NOLINT

namespace publish {

class CommandTest : public Command {
 public:
  virtual string GetName() const { return "test"; }
  virtual string GetBrief() const { return "unit test command"; }
  virtual ParameterList GetParams() const {
    ParameterList p;
    p.push_back(Parameter::Mandatory("mandatory", 'm', "argument", "desc"));
    p.push_back(Parameter::Optional("optional", 'o', "argument", "desc"));
    p.push_back(Parameter::Switch("switch", 's', "desc"));
    return p;
  }

  virtual int Main(const Options &options) { return 0; }
};

class T_Command : public ::testing::Test {
 protected:
  virtual void SetUp() { }

 protected:
  CommandTest cmd_;
};

TEST_F(T_Command, ParseOptions) {
  const char *cmdline[] = {"progname", "test", "-s", "--mandatory", "foo", "x"};
  Command::Options options = cmd_.ParseOptions(6, const_cast<char **>(cmdline));
  EXPECT_EQ(2U, options.GetSize());
  EXPECT_TRUE(options.Has("switch"));
  EXPECT_TRUE(options.Has("mandatory"));
  EXPECT_EQ("foo", options.Get("mandatory").value_str);
  EXPECT_EQ(1U, options.plain_args().size());
  EXPECT_EQ("x", options.plain_args()[0].value_str);
}

}  // namespace publish
