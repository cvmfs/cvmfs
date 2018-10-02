/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_NOTIFY_CMD_SUB_H_
#define CVMFS_NOTIFY_CMD_SUB_H_

#include <stdint.h>
#include <string>

namespace notify {

int DoSubscribe(const std::string& server_url, const std::string& topic,
                uint64_t min_revision, bool continuous, bool verbose);

}  // namespace notify

#endif  // CVMFS_NOTIFY_CMD_SUB_H_
