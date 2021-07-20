/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

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
