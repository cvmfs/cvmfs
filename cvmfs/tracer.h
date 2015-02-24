/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_TRACER_H_
#define CVMFS_TRACER_H_ 1

#include <string>

#include "atomic.h"
#include "shortstring.h"

namespace tracer {

extern bool active_;

enum TraceEvents {
  kFuseOpen = 1,
  kFuseLs,
  kFuseRead,
  kFuseReadlink,
  kFuseKcache,
  kFuseLookup,
  kFuseStat,
  kFuseCrowd,
};


void Init(const int buffer_size, const int flush_threshold,
          const std::string &tracefile);
void InitNull();
void Fini();

int32_t TraceInternal(const int event, const PathString &path,
                      const std::string &msg);
void Flush();
void inline __attribute__((used)) Trace(const int event, const PathString &path,
                                        const std::string &msg)
{
  // TODO(jblomer): could be done more elegantly by templates
  // (only 1 if when initialized)
  if (active_) TraceInternal(event, path, msg);
}

}  // namespace tracer

#endif  // CVMFS_TRACER_H_
