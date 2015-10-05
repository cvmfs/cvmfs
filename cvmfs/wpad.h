/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_WPAD_H_
#define CVMFS_WPAD_H_

#include <string>

namespace download {

class DownloadManager;

/**
 * Tries to discover list of proxy description string.
 * See: https://twiki.cern.ch/twiki/bin/view/LCG/HttpProxyDiscoveryProposal
 */
std::string AutoProxy(DownloadManager *download_manager);

/**
 * Uses AutoProxy to replace any "auto" proxy by the proxies from the PAC file
 */
std::string ResolveProxyDescription(const std::string &cvmfs_proxies,
                                    DownloadManager *download_manager);

int MainResolveProxyDescription(int argc, char **argv);

}  // namespace download

#endif  // CVMFS_WPAD_H_
