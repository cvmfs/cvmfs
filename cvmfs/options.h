/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_OPTIONS_H_
#define CVMFS_OPTIONS_H_

#include <string>
#include <vector>

namespace options {

void Init();
void Fini();

void ParsePath(const std::string config_file);
void ClearConfig();
std::string *GetValue(std::string *key);
std::string *GetSource(std::string *key);
std::vector<std::string> GetAllKeys();

}  // namespace options

#endif  // CVMFS_OPTIONS_H_
