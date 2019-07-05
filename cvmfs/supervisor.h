/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SUPERVISOR_H_
#define CVMFS_SUPERVISOR_H_

#include "stdint.h"

/**
 * Run a task and retry in case of failure
 *
 * This helper class is used to run a task and retry it in case of failure.
 * Examples of use cases: long-running tasks, such a function with connects to
 * a server and runs an event loop. If the connection fails, we want to
 * reestablish it. If failures accumulate in the cooldown interval, we finally
 * give up and exit.
 *
 * The class is supposed to be derived: the task to be run is defined as the
 * Task() virtual method. This method returns true in case of a normal
 * termination and false, in case of failure.
 *
 * The construction parameter of the class are the number of retries
 * (`max_retries`) and the cooldown interval (`interval_sec`), given in
 * seconds. The retry loop works as follows: in case of failure, the task will
 * be re-run if number of failures in the last `interval_sec` is at most
 * `max_retries`.
 */
class Supervisor {
 public:
  Supervisor(uint64_t max_retries, uint64_t interval_sec);
  virtual ~Supervisor();

  virtual bool Task() = 0;

  bool Run();

 private:
  uint64_t max_retries_;
  uint64_t interval_;
};

#endif  // CVMFS_SUPERVISOR_H_
