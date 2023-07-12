/**
 * This file is part of the CernVM File System.
 */
#include "testdoc.h"

#include <errno.h>
#include <poll.h>
#include <unistd.h>

#include "util/exception.h"
#include "util/logging.h"
#include "util/platform.h"
#include "util/pointer.h"
#include "util/posix.h"

namespace mynamespace {

class MyThreadSubclass: MyThread {
  void DoTheWork() {}
};

MyThread* MyThread::Create(const perf::TelemetrySelector type) {
  UniquePtr<MyThreadSubclass> myThreadSubclass;
  UniquePtr<MyThread> *myThread;

  switch (type) {
    case kMyThreadSubclass:
      myThreadSubclass = new MyThreadSubclass();
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
  if (pipe_terminate_.IsValid()) {
    pipe_terminate_->Write(kPipeTerminateSignal);
    pthread_join(thread_telemetry_, NULL);
    pipe_terminate_.Destroy();
  }
}

void MyThread::Spawn() {
  assert(send_rate_sec_ > 0);
  pipe_terminate_ = new Pipe<kPipeThreadTerminator>();
  int retval = pthread_create(&thread_telemetry_, NULL,
                              MyThread::MainTelemetry, this);
  assert(retval == 0);
  LogCvmfs(kLogCvmfs, kLogDebug, "Spawning of myThread thread.");
}

void *MyThread::MainTelemetry(void *data) {
  MyThread *mythread = reinterpret_cast<MyThread*>(data);

  struct pollfd watch_term;
  watch_term.fd = mythread->pipe_terminate_->GetReadFd();
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
      mythread->DoTheWork();
      // DO THE WORK HERE
      // DO THE WORK HERE
      continue;
    }

    // stop thread due to poll event
    assert(watch_term.revents != 0);
    // maybe introduce some "invalidSignal"
    PipeSignals terminate_signal = kPipeTerminateSignal;
    mythread->pipe_terminate_->Read<PipeSignals>(&terminate_signal);
    assert(terminate_signal == kPipeTerminateSignal);
    break;
  }
  LogCvmfs(kLogCvmfs, kLogDebug, "Stopping myThread");
  return NULL;
}


}  // namespace mynamespace
