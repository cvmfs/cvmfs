/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <set>
#include <string>

#include "pack.h"
#include "util/pointer.h"
#include "util/posix.h"

#include "bundle.h"

TEST(T_Bundle, ParseBundleSpec) {
  std::string json_string = "{"
    "\"bundle_id\":\"abc\","
    "\"filepaths\":["
    "\"/cvmfs/test.cern.ch/file1.txt\","
    "\"/cvmfs/test.cern.ch/file2.txt\","
    "\"/cvmfs/test.cern.ch/file3.txt\" "
    "]}";

  std::set<std::string> exp_result;
  exp_result.insert("/cvmfs/test.cern.ch/file1.txt");
  exp_result.insert("/cvmfs/test.cern.ch/file2.txt");
  exp_result.insert("/cvmfs/test.cern.ch/file3.txt");

  const JsonDocument *json = JsonDocument::Create(json_string);
  Bundle b;
  std::set<std::string> result = b.ParseBundleSpec(json->root());
  EXPECT_TRUE(result == exp_result);
}

TEST(T_Bundle, CreateBundle) {
  std::string filepath1, filepath2;
  FILE *file1 = CreateTempFile("cvmfstest", 0600, "w+", &filepath1);
  FILE *file2 = CreateTempFile("cvmfstest", 0600, "w+", &filepath2);

  size_t object_size = 0;

  std::string file_content = "abcxyza";
  size_t nbytes = fwrite(file_content.data(), 1, file_content.size(), file1);
  assert(nbytes == file_content.size());
  object_size += file_content.size();

  file_content = "abc1234567890";
  nbytes = fwrite(file_content.data(), 1, file_content.size(), file2);
  assert(nbytes == file_content.size());
  object_size += file_content.size();

  fclose(file1);
  fclose(file2);

  std::set<std::string> filepaths;
  filepaths.insert(GetAbsolutePath(filepath1));
  filepaths.insert(GetAbsolutePath(filepath2));

  Bundle b;
  UniquePtr<ObjectPack> *result = b.CreateBundle(filepaths);

  EXPECT_TRUE(result->IsValid());
  EXPECT_TRUE((*result)->size() == object_size);
}
