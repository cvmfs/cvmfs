/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_PUBLISH_CMD_UTIL_H_
#define CVMFS_PUBLISH_CMD_UTIL_H_

#include <string>

namespace publish {

/**
 * Runs the shell function $name from /etc/cvmfs/cvmfs_server_hooks.sh if
 * it exists.
 * Returns the exit code of the hook.
 */
int CallServerHook(
    const std::string &func,
    const std::string &fqrn,
    const std::string &path_hooks = "/etc/cvmfs/cvmfs_server_hooks.sh");

}  // namespace publish

#endif  // CVMFS_PUBLISH_CMD_UTIL_H_
