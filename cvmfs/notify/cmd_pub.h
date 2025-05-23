/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_NOTIFY_CMD_PUB_H_
#define CVMFS_NOTIFY_CMD_PUB_H_

#include <string>

namespace notify {

/**
 * Encapsulates a publish operation to a notification server
 *
 * Publish the manifest of a CernVM-FS repository located at "repository_url" to
 * a CernVM-FS notification server located at "server_url"
 */
int DoPublish(const std::string &server_url, const std::string &repository_url,
              bool verbose);

}  // namespace notify

#endif  // CVMFS_NOTIFY_CMD_PUB_H_
