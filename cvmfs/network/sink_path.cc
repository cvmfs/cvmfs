/**
 * This file is part of the CernVM File System.
 */

#include <cerrno>
#include <cstdio>
#include <string>

#include "sink_path.h"
#include "util/posix.h"


namespace cvmfs {

PathSink::PathSink(const std::string &destination_path) : Sink(true),
                                                      path_(destination_path) {
  file_ = fopen(destination_path.c_str(), "w");
  sink_ = new FileSink(file_, true);
}

/**
 * Purges all resources leaving the sink in an invalid state.
 * More aggressive version of Reset().
 * For some sinks and depending on owner status it might do 
 * the same as Reset().
 *
 * @returns Success = 0
 *          Failure = -errno
 */
int PathSink::Purge() {
  const int ret = sink_->Purge();
  const int ret2 = unlink(path_.c_str());

  if (ret != 0) {
    return ret;
  }
  if (ret2 != 0) {
    return ret2;
  }
  return 0;
}

/**
 * Return a string representation describing the type of sink and its status
 */
std::string PathSink::Describe() {
  std::string result = "Path sink for ";
  result += "path " + path_ + " and ";
  result += IsValid() ? " valid file pointer" : " invalid file pointer";
  return result;
}

}  // namespace cvmfs
