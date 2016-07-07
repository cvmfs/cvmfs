/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_AUTHZ_HELPER_UTIL_H_
#define CVMFS_AUTHZ_HELPER_UTIL_H_

#include <string>

const unsigned kProtocolVersion = 1;

void CheckCallContext();
void ParseHandshakeInit(const std::string &msg);
void ParseRequest(const std::string &msg);
std::string ReadMsg();
void WriteMsg(const std::string &msg);

#endif  // CVMFS_AUTHZ_HELPER_UTIL_H_
