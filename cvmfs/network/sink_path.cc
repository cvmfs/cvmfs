/**
 * This file is part of the CernVM File System.
 */

#include <cerrno>
#include <cstdio>
#include <string>


#include "util/posix.h"
#include "sink_path.h"

namespace cvmfs {

PathSink::PathSink(const std::string &destination_path) : Sink(true),
                                                      path_(destination_path) {
  file_ = fopen(destination_path.c_str(), "w");
  sink_ = new FileSink(file_);
}

int PathSink::Purge() {
  int ret = Reset();
  int ret2 = unlink(path_.c_str());

  if (ret != 0) {
    return ret;
  }
  if (ret2 != 0) {
    return ret2;
  }
  return 0;
}

/**
 * Return a string representation of the sink
*/
std::string PathSink::Describe() {
  std::string result = "Path sink for ";
  result += "path " + path_ + " and ";
  result += IsValid() ? " valid file pointer" : " invalid file pointer";
  return result;
}

}  // namespace cvmfs
