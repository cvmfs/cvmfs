/**
 * This file is part of the CernVM File System.
 */


#include "quota_listener.h"

#include <poll.h>
#include <pthread.h>

#include <cstdlib>

#include "catalog_mgr.h"
#include "quota.h"
#include "util/exception.h"
#include "util/logging.h"
#include "util/posix.h"
#include "util/smalloc.h"

using namespace std;  // NOLINT

namespace quota {

struct ListenerHandle {
  int pipe_backchannel[2];
  int pipe_terminate[2];
  QuotaManager *quota_manager;
  CatalogManager *catalog_manager;
  string repository_name;
  pthread_t thread_listener;
};


static void *MainUnpinListener(void *data) {
  ListenerHandle *handle = static_cast<ListenerHandle *>(data);
  LogCvmfs(kLogQuota, kLogDebug, "starting unpin listener for %s",
           handle->repository_name.c_str());

  struct pollfd *watch_fds = static_cast<struct pollfd *>(
      smalloc(2 * sizeof(struct pollfd)));
  watch_fds[0].fd = handle->pipe_terminate[0];
  watch_fds[0].events = POLLIN | POLLPRI;
  watch_fds[0].revents = 0;
  watch_fds[1].fd = handle->pipe_backchannel[0];
  watch_fds[1].events = POLLIN | POLLPRI;
  watch_fds[1].revents = 0;
  while (true) {
    int retval = poll(watch_fds, 2, -1);
    if (retval < 0) {
      continue;
    }

    // Terminate I/O thread
    if (watch_fds[0].revents)
      break;

    // Release pinned catalogs
    if (watch_fds[1].revents) {
      watch_fds[1].revents = 0;
      char cmd;
      ReadPipe(handle->pipe_backchannel[0], &cmd, sizeof(cmd));
      if (cmd == 'R') {
        handle->catalog_manager->DetachNested();
        LogCvmfs(kLogQuota, kLogDebug | kLogSyslog, "released nested catalogs");
      }
    }
  }
  free(watch_fds);

  LogCvmfs(kLogQuota, kLogDebug, "stopping unpin listener for %s",
           handle->repository_name.c_str());
  return NULL;
}


static void *MainWatchdogListener(void *data) {
  ListenerHandle *handle = static_cast<ListenerHandle *>(data);
  LogCvmfs(kLogQuota, kLogDebug, "starting cache manager watchdog for %s",
           handle->repository_name.c_str());

  struct pollfd *watch_fds = static_cast<struct pollfd *>(
      smalloc(2 * sizeof(struct pollfd)));
  watch_fds[0].fd = handle->pipe_terminate[0];
  watch_fds[0].events = POLLIN | POLLPRI;
  watch_fds[0].revents = 0;
  watch_fds[1].fd = handle->pipe_backchannel[0];
  watch_fds[1].events = POLLIN | POLLPRI;
  watch_fds[1].revents = 0;
  while (true) {
    int retval = poll(watch_fds, 2, -1);
    if (retval < 0) {
      continue;
    }

    // Terminate I/O thread
    if (watch_fds[0].revents)
      break;

    // Release pinned catalogs
    if (watch_fds[1].revents) {
      if ((watch_fds[1].revents & POLLERR) || (watch_fds[1].revents & POLLHUP)
          || (watch_fds[1].revents & POLLNVAL)) {
        PANIC(kLogDebug | kLogSyslogErr, "cache manager disappeared, aborting");
      }
      // Clean the pipe
      watch_fds[1].revents = 0;
      char dummy;
      ReadPipe(handle->pipe_backchannel[0], &dummy, sizeof(dummy));
    }
  }
  free(watch_fds);

  LogCvmfs(kLogQuota, kLogDebug, "stopping cache manager watchdog for %s",
           handle->repository_name.c_str());
  return NULL;
}


/**
 * Registers a back channel that reacts on high watermark of pinned chunks
 */
ListenerHandle *RegisterUnpinListener(QuotaManager *quota_manager,
                                      CatalogManager *catalog_manager,
                                      const string &repository_name) {
  ListenerHandle *handle = new ListenerHandle();
  quota_manager->RegisterBackChannel(handle->pipe_backchannel, repository_name);
  MakePipe(handle->pipe_terminate);
  handle->quota_manager = quota_manager;
  handle->catalog_manager = catalog_manager;
  handle->repository_name = repository_name;
  int retval = pthread_create(&handle->thread_listener, NULL, MainUnpinListener,
                              static_cast<void *>(handle));
  assert(retval == 0);
  return handle;
}


/**
 * Registers a back channel that kills the fuse module if the cache manager
 * disappears
 */
ListenerHandle *RegisterWatchdogListener(QuotaManager *quota_manager,
                                         const string &repository_name) {
  ListenerHandle *handle = new ListenerHandle();
  quota_manager->RegisterBackChannel(handle->pipe_backchannel, repository_name);
  MakePipe(handle->pipe_terminate);
  handle->quota_manager = quota_manager;
  handle->catalog_manager = NULL;
  handle->repository_name = repository_name;
  int retval = pthread_create(&handle->thread_listener, NULL,
                              MainWatchdogListener,
                              static_cast<void *>(handle));
  assert(retval == 0);
  return handle;
}


void UnregisterListener(ListenerHandle *handle) {
  const char terminate = 'T';
  WritePipe(handle->pipe_terminate[1], &terminate, sizeof(terminate));
  pthread_join(handle->thread_listener, NULL);
  ClosePipe(handle->pipe_terminate);

  handle->quota_manager->UnregisterBackChannel(handle->pipe_backchannel,
                                               handle->repository_name);

  delete handle;
}

}  // namespace quota
