/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_RESOLV_CONF_EVENT_HANDLER_H_
#define CVMFS_RESOLV_CONF_EVENT_HANDLER_H_

#include <string>
#include <utility>
#include <vector>

#include "file_watcher.h"
#include "network/download.h"

namespace download {

class DownloadManager;

}  // namespace download

class ResolvConfEventHandler : public file_watcher::EventHandler {
 public:
  /**
   * List of IP addresses
   *
   * Each address is stored as a <TYPE, ADDRESS> pair, were TYPE is either
   * 4 or 6.
   */
  typedef std::vector<std::pair<int, std::string> > AddressList;

  ResolvConfEventHandler(download::DownloadManager *download_manager,
                         download::DownloadManager *external_download_manager);
  virtual ~ResolvConfEventHandler();

  virtual bool Handle(const std::string &file_path,
                      file_watcher::Event event,
                      bool *clear_handler);

  static void GetDnsAddresses(const std::string &resolv_file,
                              AddressList *addresses);

  static void SetDnsAddress(download::DownloadManager *download_manager,
                            const AddressList &addresses);

 private:
  download::DownloadManager *download_manager_;
  download::DownloadManager *external_download_manager_;
};

#endif  // CVMFS_RESOLV_CONF_EVENT_HANDLER_H_
