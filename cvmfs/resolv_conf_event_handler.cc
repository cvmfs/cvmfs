/**
 * This file is part of the CernVM File System.
 */

#include "resolv_conf_event_handler.h"

#include <string>
#include <vector>

#include "backoff.h"
#include "util/logging.h"
#include "util/string.h"

namespace {

/**
 * Get the first address of type address_type from the list of addresses
 *
 * @param address_list List of IP addresses
 * @param address_type Type of IP address desired; Can be either 4 or 6
 * @param address (out) The first address found
 */
bool GetFirstAddress(const ResolvConfEventHandler::AddressList &address_list,
                     int address_type, std::string *address) {
  bool found = false;

  for (size_t i = 0u; i < address_list.size(); ++i) {
    if (address_list[i].first == address_type) {
      *address = address_list[i].second;
      found = true;
      break;
    }
  }
  return found;
}

}  // namespace

ResolvConfEventHandler::ResolvConfEventHandler(
    download::DownloadManager *download_manager,
    download::DownloadManager *external_download_manager)
    : download_manager_(download_manager)
    , external_download_manager_(external_download_manager) { }

ResolvConfEventHandler::~ResolvConfEventHandler() { }

bool ResolvConfEventHandler::Handle(const std::string &file_path,
                                    file_watcher::Event /*event*/,
                                    bool *clear_handler) {
  AddressList addresses;
  GetDnsAddresses(file_path, &addresses);
  if (!addresses.empty()) {
    SetDnsAddress(download_manager_, addresses);
    SetDnsAddress(external_download_manager_, addresses);
  }
  *clear_handler = false;
  return true;
}

/**
 * Returns any  nameserver IP addresses from file
 *
 * Reads a file, line by line. Returns the IP addresses corresponding
 * to nameservers (i.e. from lines "nameserver <IP_ADDRESS>")
 */
void ResolvConfEventHandler::GetDnsAddresses(const std::string &resolv_file,
                                             AddressList *addresses) {
  bool done = false;
  BackoffThrottle throttle(100, 1000, 5000);
  while (!done) {
    FILE *f = std::fopen(resolv_file.c_str(), "r");
    if (!f) {
      LogCvmfs(kLogCvmfs, kLogDebug,
               "ResolvConfEventHandler - Could not open: %s",
               resolv_file.c_str());
      throttle.Throttle();
      continue;
    }
    std::string line;
    while (GetLineFile(f, &line)) {
      std::vector<std::string> key_val = SplitString(line, ' ');
      if (key_val.size() == 2 && key_val[0] == "nameserver") {
        // We have found a nameserver line
        if (SplitString(key_val[1], '.').size() == 4) {
          // This looks like an IPv4 address
          addresses->push_back(std::make_pair(4, key_val[1]));
        } else if (SplitString(key_val[1], ':').size() == 8) {
          // This looks like an IPv6 address
          addresses->push_back(std::make_pair(6, key_val[1]));
        }
      }
    }
    std::fclose(f);
    done = true;
  }
}

void ResolvConfEventHandler::SetDnsAddress(
    download::DownloadManager *download_manager, const AddressList &addresses) {
  // Default to IPv4 addresses unless kIpPreferV6 is specified
  const int address_type = download_manager->opt_ip_preference()
                                   == dns::kIpPreferV6
                               ? 6
                               : 4;

  std::string new_address;
  if (GetFirstAddress(addresses, address_type, &new_address)) {
    LogCvmfs(kLogCvmfs, kLogDebug,
             "ResolvConfEventhandler - resolv.conf file changed. "
             "Setting new DNS address: %s",
             new_address.c_str());
    download_manager->SetDnsServer(new_address);
  }
}
