/**
 * This file is part of the CernVM File System.
 */

#ifndef TEST_UNITTESTS_C_FILE_SANDBOX_H_
#define TEST_UNITTESTS_C_FILE_SANDBOX_H_

#include <string>
#include <utility>

#include "../../cvmfs/hash.h"
#include "../../cvmfs/prng.h"
#include "../../cvmfs/util.h"

class FileSandbox : public ::testing::Test {
 public:
  typedef std::pair<std::string, shash::Suffix> ExpectedHashString;

 public:
  explicit FileSandbox(const std::string &sandbox_path) :
    sandbox_path_(sandbox_path) {}

 protected:
  const std::string& GetEmptyFile() {
    LazilyCreateDummyFile(sandbox_path_, 0, &empty_file_, 42);
    return empty_file_;
  }

  const std::string GetSmallFile() {
    LazilyCreateDummyFile(sandbox_path_, 50, &small_file_, 314);
    return small_file_;
  }

  const std::string GetBigFile() {
    LazilyCreateDummyFile(sandbox_path_, 4*1024, &big_file_, 1337);
    return big_file_;
  }

  const std::string GetHugeFile() {
    LazilyCreateDummyFile(sandbox_path_, 100*1024, &huge_file_, 1);
    return huge_file_;
  }

  template <class VectorT>
  void AppendVectorToVector(const VectorT &appendee, VectorT *vector) const {
    vector->insert(vector->end(), appendee.begin(), appendee.end());
  }

  void CreateSandbox(const std::string &sandbox_tmp_dir = "") {
    bool retval;

    retval = MkdirDeep(sandbox_path_, 0700);
    ASSERT_TRUE(retval) << "failed to create sandbox in '" << sandbox_path_
                        << "' (" << errno << ")";

    if (!sandbox_tmp_dir.empty()) {
      retval = MkdirDeep(sandbox_tmp_dir, 0700);
      ASSERT_TRUE(retval) << "failed to create sandbox tmp directory";
    }
  }

  void RemoveSandbox(const std::string &sandbox_tmp_dir = "") {
    bool retval;

    if (!sandbox_tmp_dir.empty()) {
      retval = RemoveTree(sandbox_tmp_dir);
      ASSERT_TRUE(retval) << "failed to remove sandbox tmp directory";
    }

    retval = RemoveTree(sandbox_path_);
    ASSERT_TRUE(retval) << "failed to remove sandbox";
  }

  void CreateRandomBuffer(const size_t nbytes, char *buffer, Prng *rng) {
    for (size_t i = 0; i < nbytes; ++i) {
      buffer[i] = rng->Next(256);
    }
  }

  void LazilyCreateDummyFile(const std::string &sandbox_path,
                             const size_t       file_size_kb,
                                   std::string *file_name,
                             const uint64_t     seed) {
    static const size_t kb = 1024;

    // if file was already created, we do not do it again!
    if (!file_name->empty()) {
      return;
    }

    // create a temporary file
    FILE *file = CreateTempFile(sandbox_path + "/dummy", 0600, "r+", file_name);
    ASSERT_NE(static_cast<FILE*>(0), file) << "failed to create tmp file";

    // file the temporary file with the requested number of (pseudo) random data
    Prng rng;
    rng.InitSeed(seed);
    for (size_t i = 0; i < file_size_kb; ++i) {
      typedef char buffer_type;
      buffer_type buffer[kb];
      CreateRandomBuffer(kb, buffer, &rng);
      const size_t written = fwrite(buffer, sizeof(buffer_type), kb, file);
      ASSERT_EQ(written, kb * sizeof(buffer_type))
        << "failed to write to tmp (errno: " << errno << ")";
    }

    // close the generated dummy file
    const int retval = fclose(file);
    ASSERT_EQ(0, retval) << "failed to close tmp file";
  }

 protected:
  const std::string sandbox_path_;

  std::string       empty_file_;
  std::string       small_file_;
  std::string       big_file_;
  std::string       huge_file_;
};

#endif  // TEST_UNITTESTS_C_FILE_SANDBOX_H_
