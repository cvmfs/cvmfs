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

}  // namespace download

#endif  // CVMFS_WPAD_H_
