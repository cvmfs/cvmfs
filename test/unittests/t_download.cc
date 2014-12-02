#include "gtest/gtest.h"

#include <unistd.h>

#include <cstdio>

#include "../../cvmfs/download.h"
#include "../../cvmfs/util.h"

using namespace std;  // NOLINT

namespace download {

class T_Download : public ::testing::Test {
 protected:
  virtual void SetUp() {
    download_mgr.Init(8, false /* use_system_proxy */);
    ffoo = CreateTempFile("/tmp/cvmfstest", 0600, "w+", &foo_path);
    assert(ffoo);
    foo_url = "file://" + foo_path;
  }

  virtual ~T_Download() {
    download_mgr.Fini();
    fclose(ffoo);
    unlink(foo_path.c_str());
  }

  DownloadManager download_mgr;
  FILE *ffoo;
  string foo_path;
  string foo_url;
};


//------------------------------------------------------------------------------


// A placeholder test for future unit testing of the download module
TEST_F(T_Download, File) {
  string dest_path;
  FILE *fdest = CreateTempFile("/tmp/cvmfstest", 0600, "w+", &dest_path);
  ASSERT_TRUE(fdest != NULL);
  UnlinkGuard unlink_guard(dest_path);

  JobInfo info(&foo_url, false /* compressed */, false /* probe hosts */,
               fdest, NULL);
  download_mgr.Fetch(&info);
  EXPECT_EQ(info.error_code, kFailOk);
  fclose(fdest);
}



}  // namespace download
