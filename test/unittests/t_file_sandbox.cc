#include <gtest/gtest.h>
#include <string>

#include "c_file_sandbox.h"


class T_FileSandbox : public FileSandbox {
 private:
  static const std::string sandbox_path;

 public:
  typedef std::vector<ExpectedHashString> ExpectedHashStrings;

  T_FileSandbox() :
    FileSandbox(T_FileSandbox::sandbox_path) {}

 protected:
  void SetUp() {
    CreateSandbox(T_FileSandbox::sandbox_path);
  }

  void TearDown() {
    RemoveSandbox(T_FileSandbox::sandbox_path);
  }
};

const std::string T_FileSandbox::sandbox_path = "/tmp/cvmfs_ut_filesandbox";


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


TEST_F(T_FileSandbox, EmptyFile) {

}
