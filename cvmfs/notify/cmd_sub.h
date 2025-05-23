/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_NOTIFY_CMD_SUB_H_
#define CVMFS_NOTIFY_CMD_SUB_H_

#include <stdint.h>

#include <string>

namespace notify {

/**
 * Encapsulates a subscription operation to a notification server
 *
 * Subscribe to messages concerning "repo" (the fully-qualified domain
 * name of a CernVM-FS repository) from the CernVM-FS notification server
 * located at "server_url". Only messages with revision number >= "min_revision"
 * will be delivered. Setting "continuous" to false will cancel the subscription
 * after the first message is delivered.
 */
int DoSubscribe(const std::string &server_url, const std::string &repo,
                uint64_t min_revision, bool continuous, bool verbose);

}  // namespace notify

#endif  // CVMFS_NOTIFY_CMD_SUB_H_
