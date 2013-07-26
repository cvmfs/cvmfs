/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "quota_listener.h"

#include <pthread.h>
#include <poll.h>

#include "quota.h"
#include "catalog_mgr.h"
#include "smalloc.h"
#include "logging.h"

using namespace std;  // NOLINT

namespace quota {

struct ListenerHandle {
  int pipe_backchannel[2];
  int pipe_terminate[2];
  catalog::AbstractCatalogManager *catalog_manager;
  string repository_name;
  pthread_t thread_listener;
};


static void *MainUnpinListener(void *data) {
  ListenerHandle *handle = static_cast<ListenerHandle *>(data);
  LogCvmfs(kLogQuota, kLogDebug, "starting unpin listener for %s",
           handle->repository_name.c_str());

  struct pollfd *watch_fds =
    static_cast<struct pollfd *>(smalloc(2 * sizeof(struct pollfd)));
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
      // The pipe may break if Ctrl+C is pressed in foreground cvmfs.
      // This should be gracefully handled
      int bytes_read = read(handle->pipe_backchannel[0], &cmd, sizeof(cmd));
      if ((bytes_read == 1) && (cmd == 'R')) {
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


/**
 * Registers a back channel that reacts on high watermark of pinned chunks
 */
ListenerHandle *
RegisterUnpinListener(catalog::AbstractCatalogManager *catalog_manager,
                      const string &repository_name)
{
  ListenerHandle *handle = new ListenerHandle();
  quota::RegisterBackChannel(handle->pipe_backchannel, repository_name);
  MakePipe(handle->pipe_terminate);
  handle->catalog_manager = catalog_manager;
  handle->repository_name = repository_name;
  int retval = pthread_create(&handle->thread_listener, NULL, MainUnpinListener,
                              static_cast<void *>(handle));
  assert(retval == 0);
  return handle;
}


void UnregisterUnpinListener(ListenerHandle *handle) {
  UnregisterBackChannel(handle->pipe_backchannel, handle->repository_name);

  const char terminate = 'T';
  WritePipe(handle->pipe_terminate[1], &terminate, sizeof(terminate));
  pthread_join(handle->thread_listener, NULL);
  ClosePipe(handle->pipe_terminate);

  delete handle;
}

}  // namespace quota
