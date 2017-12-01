/**
 * This file is part of the CernVM File System.
 */

#include "resolv_conf_event_handler.h"

#include <string>
#include <vector>

#include "backoff.h"
#include "logging.h"
#include "util/string.h"

/**
 * Returns any  nameserver IP addresses from file
 *
 * Reads a file, line by line. Returns the IP addresses corresponding
 * to nameservers (i.e. from lines "nameserver <IP_ADDRESS>")
 */
static bool GetDnsAddresses(const std::string& resolv_file,
                            std::vector<std::string>* ipv4_addresses,
                            std::vector<std::string>* ipv6_addresses) {
  bool done = false;
  BackoffThrottle throttle(100, 1000, 5000);
  while (!done) {
    FILE* f = std::fopen(resolv_file.c_str(), "r");
    if (!f) {
      LogCvmfs(kLogCvmfs, kLogDebug,
               "ResolvConfEventHandler - Could not open /etc/resolv.conf");
      throttle.Throttle();
      continue;
    }
    std::string line;
    while (GetLineFile(f, &line)) {
      std::vector<std::string> key_val = SplitString(line, ' ');
      if (key_val.size() == 2 && key_val[0] == "nameserver") {
        // We have found a nameserver line
        if (SplitString(key_val[1], '.').size() == 4) {
          if (ipv4_addresses) {
            // This looks like an IPv4 address
            ipv4_addresses->push_back(key_val[1]);
          }
        } else if (SplitString(key_val[1], ':').size() == 6) {
          if (ipv6_addresses) {
            // This looks like an IPv6 address
            ipv6_addresses->push_back(key_val[1]);
          }
        }
      }
    }
    std::fclose(f);
    done = true;
  }
  return true;
}

ResolvConfEventHandler::ResolvConfEventHandler(
    download::DownloadManager* download_manager,
    download::DownloadManager* external_download_manager)
    : download_manager_(download_manager),
      external_download_manager_(external_download_manager) {}

ResolvConfEventHandler::~ResolvConfEventHandler() {}

bool ResolvConfEventHandler::Handle(const std::string& file_path,
                                    file_watcher::Event /*event*/,
                                    bool* clear_handler) {
  std::vector<std::string> addresses;
  GetDnsAddresses(file_path, &addresses, NULL);
  if (!addresses.empty()) {
    LogCvmfs(kLogCvmfs, kLogDebug,
             "ResolvConfEventhandler - resolv.conf file changed. "
             "Setting new DNS address: %s",
             addresses[0].c_str());
    if (download_manager_) {
      download_manager_->SetDnsServer(addresses[0]);
    }
    if (external_download_manager_) {
      external_download_manager_->SetDnsServer(addresses[0]);
    }
  }
  *clear_handler = false;
  return true;
}
