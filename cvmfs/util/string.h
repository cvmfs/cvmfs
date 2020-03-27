/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UTIL_STRING_H_
#define CVMFS_UTIL_STRING_H_

#include <stdint.h>
#include <sys/time.h>

#include <cstdio>
#include <map>
#include <string>
#include <vector>

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

std::string StringifyBool(const bool value);
std::string StringifyInt(const int64_t value);
std::string StringifyUint(const uint64_t value);
std::string StringifyByteAsHex(const unsigned char value);
std::string StringifyDouble(const double value);
std::string StringifyTime(const time_t seconds, const bool utc);
std::string StringifyTimeval(const timeval value);
std::string RfcTimestamp();
std::string IsoTimestamp();
std::string WhitelistTimestamp(time_t when);
time_t IsoTimestamp2UtcTime(const std::string &iso8601);
int64_t String2Int64(const std::string &value);
uint64_t String2Uint64(const std::string &value);
bool String2Uint64Parse(const std::string &value, uint64_t *result);

void String2Uint64Pair(const std::string &value, uint64_t *a, uint64_t *b);
bool HasPrefix(const std::string &str, const std::string &prefix,
               const bool ignore_case);
bool HasSuffix(const std::string &str, const std::string &suffix,
               const bool ignore_case);

std::vector<std::string> SplitString(const std::string &str, const char delim,
                                     const unsigned max_chunks = 0);
std::string JoinStrings(const std::vector<std::string> &strings,
                        const std::string &joint);
void ParseKeyvalMem(const unsigned char *buffer, const unsigned buffer_size,
                    std::map<char, std::string> *content);
bool ParseKeyvalPath(const std::string &filename,
                     std::map<char, std::string> *content);

std::string GetLineMem(const char *text, const int text_size);
bool GetLineFile(FILE *f, std::string *line);
bool GetLineFd(const int fd, std::string *line);
std::string Trim(const std::string &raw, bool trim_newline = false);
std::string ToUpper(const std::string &mixed_case);
std::string ReplaceAll(const std::string &haystack, const std::string &needle,
                       const std::string &replace_by);
std::string Tail(const std::string &source, unsigned num_lines);

std::string Base64(const std::string &data);
std::string Base64Url(const std::string &data);
bool Debase64(const std::string &data, std::string *decoded);
std::string GetGMTimestamp(std::string format = "%Y-%m-%d %H:%M:%S");


#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif

#endif  // CVMFS_UTIL_STRING_H_
