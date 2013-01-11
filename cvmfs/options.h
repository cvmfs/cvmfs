/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_OPTIONS_H_
#define CVMFS_OPTIONS_H_

#include <string>
#include <vector>

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

namespace options {

void Init();
void Fini();

void ParsePath(const std::string &config_file);
void ParseDefault(const std::string &repository_name);
void ClearConfig();
bool GetValue(const std::string &key, std::string *value);
bool GetSource(const std::string &key, std::string *value);
bool IsOn(const std::string &param_value);
bool QueryDns(const std::string &hostname,
              int type,
              const std::string &dns_server,
              const uint16_t port,
              std::string *result)
std::vector<std::string> GetAllKeys();
std::string Dump();

}  // namespace options

#ifdef CVMFS_NAMESPACE_GUARD
}
#endif

#endif  // CVMFS_OPTIONS_H_
