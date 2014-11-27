#include "gtest/gtest.h"

#include <unistd.h>

#include "../../cvmfs/download.h"
#include "../../cvmfs/util.h"

using namespace std;  // NOLINT

namespace download {

class T_Download : public ::testing::Test {
 protected:
  virtual void SetUp() {
  }

  virtual ~T_Download() {
  }
};


//------------------------------------------------------------------------------


TEST_F(T_Download, File) {
}

}  // namespace download
