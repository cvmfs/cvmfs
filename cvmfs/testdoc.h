/**
 * This file is part of the CernVM File System.
 *
 * MyThread class manages a thread that does some work... . 
 * A custom myThread subclass is needed to do the ACTUAL work...
 */

#ifndef CVMFS_TESTDOC_H_
#define CVMFS_TESTDOC_H_

#include <pthread.h>
#include <stdint.h>

#include <cstring>
#include <map>
#include <string>
#include <vector>

#include "telemetry_aggregator.h"
#include "util/pipe.h"
#include "util/pointer.h"
#include "util/single_copy.h"

namespace mynamespace {

// List of available custom telemetry classes
enum TypeSelector {
  kMyThreadSubclass
};

class MyThread : SingleCopy {
 public:
  /**
   * Creates the requested myThread. This function is also used to
   * register new classes to myThread work.
   *
   * Returns the newly created MyThread or NULL if the creation
   * was not successful.
  */
  static MyThread* Create(const perf::TelemetrySelector type);
  virtual ~MyThread();
  void Spawn();  // Spawns the myThread thread.

 protected:
  const int send_rate_sec_;
  UniquePtr<Pipe<kPipeThreadTerminator> > pipe_terminate_;
  pthread_t thread_telemetry_;
  // State of constructed object. Used in custom myThread subclasses to
  // specify that the object was correctly constructed.
  bool is_zombie_;

  uint64_t timestamp_;

  /**
   * Main loop executed by the myThread thread to do work....
   * Checks every x seconds if the telemetry thread should continue running.
  */
  static void *MainTelemetry(void *data);

  /**
   * Base constructor taking care of threading infrastructure.
   * Must always be called in the constructor of the custom telemetry classes.
  */
  MyThread() : is_zombie_(true), timestamp_(0), send_rate_sec_(1) {
    pipe_terminate_ = NULL;
    memset(&thread_telemetry_, 0, sizeof(thread_telemetry_));
  }

  /**
   * Needs to be implemented in the custom myThread subclass.
  */
  virtual void DoTheWork() = 0;
};

}  // namespace mynamespace

#endif  // CVMFS_TESTDOC_H_
