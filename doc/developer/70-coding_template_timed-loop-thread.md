# Coding Templates

This section contains coding templates that are commonly used in CVMFS, and
are easily adaptable to different situations.

## Execute work every X seconds (in separate thread)
Idea: Have some work executed every X seconds
Solution: Use a thread with an infinite-loop that uses `poll` to
  - Periodically execute the work
  - Stop the thread if requested from the outside

Details:
- `Create()`-Function allows registering subclasses
- `MainMyThread()` contains loop that is started by calling `Spawn()`
- `DoTheWork()` contains the actual work to be executed in interval

Header `mythread.h`
```c++
/**
 * This file is part of the CernVM File System.
 *
 * MyThread class manages a thread that does some work... . 
 * A custom myThread subclass is needed to do the ACTUAL work...
 */

#ifndef CVMFS_MY_THREAD_H_
#define CVMFS_MY_THREAD_H_

#include <pthread.h>
#include <stdint.h>

#include <cstring>
#include <map>
#include <string>
#include <vector>

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
  static MyThread* Create(params..., const TelemetrySelector type);
  virtual ~MyThread();
  void Spawn(); // Spawns the myThread thread.

 protected:
  const int send_rate_sec_;
  int pipe_terminate_[2];
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
  MyThread(params...) : is_zombie_(true), timestamp_(0) {
    pipe_terminate_[0] = pipe_terminate_[1] = -1;
    memset(&thread_telemetry_, 0, sizeof(thread_telemetry_));
  }

  /**
   * Needs to be implemented in the custom myThread subclass.
  */
  virtual void DoTheWork() = 0;
};

}  // namespace mynamespace

#endif  // CVMFS_MY_THREAD_H_
```


Source `mythread.cc`
```c++
#include "mythread.h"

#include <errno.h>
#include <poll.h>
#include <unistd.h>

#include "util/exception.h"
#include "util/logging.h"
#include "util/platform.h"
#include "util/pointer.h"
#include "util/posix.h"

namespace mynamespace {

MyThread* MyThread::Create(params..., const TypeSelector type) {
  UniquePtr<MyThreadSubclass> myThreadSubclass;
  UniquePtr<MyThread> *myThread;

  switch (type) {
    case kMyThreadSubclass:
      myThreadSubclass = new MyThreadSubclass(params...);
      myThread = reinterpret_cast<UniquePtr<MyThread>*> (&myThreadSubclass);
    break;
    default:
      LogCvmfs(kLogCvmfs, kLogDebug,
                      "No implementation available for given myThread class.");
      return NULL;
    break;
  }

  if (myThread->weak_ref()->is_zombie_) {
    LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslogErr,
      "Requested myThread will NOT be used. "
      "It was not constructed correctly.");
    return NULL;
  }

  LogCvmfs(kLogCvmfs, kLogDebug, "MyThread created.");
  return myThread->Release();
}

MyThread::~MyThread() {
  if (pipe_terminate_[1] >= 0) {
    char t = 'T';
    WritePipe(pipe_terminate_[1], &t, 1);
    pthread_join(thread_myThread_, NULL);
    ClosePipe(pipe_terminate_);
  }
}

void MyThread::Spawn() {
  assert(pipe_terminate_[0] == -1);
  assert(send_rate_sec_ > 0);
  MakePipe(pipe_terminate_);
  int retval = pthread_create(&thread_myThread_, NULL, MainMyThread, this);
  assert(retval == 0);
  LogCvmfs(kLogCvmfs, kLogDebug, "Spawning of myThread thread.");
}

void *MyThread::MainMyThread(void *data) {
  MyThread *mythread = reinterpret_cast<MyThread*>(data);

  struct pollfd watch_term;
  watch_term.fd = mythread->pipe_terminate_[0];
  watch_term.events = POLLIN | POLLPRI;
  int timeout_ms = mythread->send_rate_sec_ * 1000;
  uint64_t deadline_sec = platform_monotonic_time()
                          + mythread->send_rate_sec_;
  while (true) {
    // sleep and check if end - blocking wait for "send_rate_sec_" seconds
    watch_term.revents = 0;
    int retval = poll(&watch_term, 1, timeout_ms);
    if (retval < 0) {
      if (errno == EINTR) {  // external interrupt occured - no error for us
        if (timeout_ms >= 0) {
          uint64_t now = platform_monotonic_time();
          timeout_ms = (now > deadline_sec) ? 0 :
                                  static_cast<int>((deadline_sec - now) * 1000);
        }
        continue;
      }
      PANIC(kLogSyslogErr | kLogDebug, "Error in myThread thread. "
                                       "Poll returned %d", retval);
    }

    // reset timeout and deadline of poll
    timeout_ms = mythread->send_rate_sec_ * 1000;
    deadline_sec = platform_monotonic_time() + mythread->send_rate_sec_;

    if (retval == 0) {
      // DO THE WORK HERE
      // DO THE WORK HERE
      myThread->DoTheWork();
      // DO THE WORK HERE
      // DO THE WORK HERE
      continue;
    }

    // stop thread due to poll event
    assert(watch_term.revents != 0);

    char c = 0;
    ReadPipe(mythread->pipe_terminate_[0], &c, 1);
    assert(c == 'T');
    break;
  }
  LogCvmfs(kLogCvmfs, kLogDebug, "Stopping myThread");
  return NULL;
}

}  // namespace mynamespace
```