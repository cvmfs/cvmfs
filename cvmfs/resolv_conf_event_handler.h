/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_RESOLV_CONF_EVENT_HANDLER_H_
#define CVMFS_RESOLV_CONF_EVENT_HANDLER_H_

#include <string>

#include "download.h"
#include "file_watcher.h"

namespace download {

class DownloadManager;

}  // namespace download

class ResolvConfEventHandler : public file_watcher::EventHandler {
 public:
  ResolvConfEventHandler(download::DownloadManager* download_manager,
                         download::DownloadManager* external_download_manager);
  virtual ~ResolvConfEventHandler();

  virtual bool Handle(const std::string& file_path,
                      file_watcher::Event event);

 private:
  download::DownloadManager* download_manager_;
  download::DownloadManager* external_download_manager_;
};

#endif  // CVMFS_RESOLV_CONF_EVENT_HANDLER_H_
