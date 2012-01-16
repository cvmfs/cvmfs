/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_TRACER_H_
#define CVMFS_TRACER_H_ 1

#include <string>
#include "atomic.h"

namespace tracer {

extern bool active;

enum TraceEvents {
  kFuseOpen = 1,
  kFuseLs,
  kFuseRead,
  kFuseReadlink,
  kFuseKcache,
  kFuseStat,
  kFuseCrowd,
};


void Init(const int buffer_size, const int flush_threshold,
          const std::string &tracefile);
void InitNull();
void Fini();

int32_t TraceInternal(const int event, const std::string &id,
                      const std::string &msg);
void Flush();
void inline __attribute__((used)) Trace(const int event, const std::string &id,
                                        const std::string &msg)
{
   if (active) TraceInternal(event, id, msg);
}

}  // namespace tracer

#endif  // CVMFS_TRACER_H_
