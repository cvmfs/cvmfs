/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include "../../cvmfs/util.h"

class T_Pipe : public ::testing::Test {
 protected:
  virtual void SetUp() {
    const int fd_read_flags  = fcntl(pipe.read_end,  F_GETFD);
    const int fd_write_flags = fcntl(pipe.write_end, F_GETFD);

    ASSERT_GE(fd_read_flags,  0) << "Failed to create pipe (read end)";
    ASSERT_GE(fd_write_flags, 0) << "Failed to create pipe (write end)";
  }


  struct Foo {
    Foo() : integer(42), character('T'), floating_point(13.37) {}

    bool Check(const Foo &foo) const {
      EXPECT_EQ(foo.integer,        integer);
      EXPECT_EQ(foo.character,      character);
      EXPECT_EQ(foo.floating_point, floating_point);

      return foo.integer        == integer         &&
             foo.character      == character       &&
             foo.floating_point == floating_point;
    }

    int    integer;
    char   character;
    double floating_point;
  };

  struct TestEnum {
    enum MyEnum {
      kFirstChoice,
      kSecondChoice,
      kThirdChoice,
      //...
      kNthChoice
    };
  };

 protected:
  Pipe pipe;
};


TEST_F(T_Pipe, Create) {
}


TEST_F(T_Pipe, WriteTemplate) {
  const int  integer   = 100;
  const char character = 'a';
  const Foo  foobar;

  bool retval;
  retval = pipe.Write(integer);   EXPECT_TRUE(retval);
  retval = pipe.Write(character); EXPECT_TRUE(retval);
  retval = pipe.Write(foobar);    EXPECT_TRUE(retval);
  EXPECT_DEATH(pipe.Write(&foobar), "");
}


TEST_F(T_Pipe, ReadTemplate) {
  const int              integer   = 100;
  const char             character = 'a';
  const Foo              foobar;
  const TestEnum::MyEnum optional  = TestEnum::kThirdChoice;

  bool retval;
  retval = pipe.Write(integer);   EXPECT_TRUE(retval);
  retval = pipe.Write(character); EXPECT_TRUE(retval);
  retval = pipe.Write(foobar);    EXPECT_TRUE(retval);
  retval = pipe.Write(optional);  EXPECT_TRUE(retval);

  int              res_integer;
  char             res_character;
  Foo              res_foobar;
  TestEnum::MyEnum res_optional;

  retval = pipe.Read(&res_integer);   EXPECT_TRUE(retval);
  retval = pipe.Read(&res_character); EXPECT_TRUE(retval);
  retval = pipe.Read(&res_foobar);    EXPECT_TRUE(retval);
  retval = pipe.Read(&res_optional);  EXPECT_TRUE(retval);

  EXPECT_EQ(res_integer, integer)     << "Failed to retrieve integer";
  EXPECT_EQ(res_character, character) << "Failed to retrieve character";
  EXPECT_TRUE(foobar.Check(res_foobar)) << "Failed to retrieve structure";
  EXPECT_EQ(res_optional, optional)   << "Failed to retrieve enum value";
}


TEST_F(T_Pipe, WriteRaw) {
  const char   *data        = "This is just POD...";
  const size_t  data_length = 19 + 1;

  bool retval;
  retval = pipe.Write(data, data_length); EXPECT_TRUE(retval);
}


TEST_F(T_Pipe, ReadRaw) {
  const char   *data        = "This is just POD...";
  const size_t  data_length = 19 + 1;

  bool retval;
  retval = pipe.Write(data, data_length); EXPECT_TRUE(retval);

  char res_data[100];
  retval = pipe.Read(res_data, data_length); EXPECT_TRUE(retval);

  EXPECT_EQ(0, strncmp(data, res_data, data_length)) << "Data did not match";
}
