/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_AUTHZ_HELPER_LOG_H_
#define CVMFS_AUTHZ_HELPER_LOG_H_

#include <string>

const unsigned kLogAuthzDebug = 0x01;
const unsigned kLogAuthzSyslog = 0x02;
const unsigned kLogAuthzSyslogWarn = 0x04;
const unsigned kLogAuthzSyslogErr = 0x08;

void SetLogAuthzDebug(const std::string &path);
void SetLogAuthzSyslogLevel(const int level);
void SetLogAuthzSyslogFacility(const int local_facility);
void SetLogAuthzSyslogPrefix(const std::string &prefix);
void LogAuthz(const int flags, const char *format, ...);

#endif  // CVMFS_AUTHZ_HELPER_LOG_H_
