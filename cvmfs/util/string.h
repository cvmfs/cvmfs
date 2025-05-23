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

#include "util/export.h"

const int kTrimNone = 0;
const int kTrimLeading = 1 << 0;
const int kTrimTrailing = 1 << 1;
const int kTrimAll = kTrimLeading | kTrimTrailing;

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

CVMFS_EXPORT std::string StringifyBool(const bool value);
CVMFS_EXPORT std::string StringifyInt(const int64_t value);
CVMFS_EXPORT std::string StringifyUint(const uint64_t value);
CVMFS_EXPORT std::string StringifyByteAsHex(const unsigned char value);
CVMFS_EXPORT std::string StringifyDouble(const double value);
CVMFS_EXPORT std::string StringifyTime(const time_t seconds, const bool utc);
CVMFS_EXPORT std::string StringifyLocalTime(const time_t seconds);
CVMFS_EXPORT std::string StringifyTimeval(const timeval value);
CVMFS_EXPORT std::string RfcTimestamp();
CVMFS_EXPORT std::string IsoTimestamp();
CVMFS_EXPORT std::string WhitelistTimestamp(time_t when);
CVMFS_EXPORT time_t IsoTimestamp2UtcTime(const std::string &iso8601);
CVMFS_EXPORT int64_t String2Int64(const std::string &value);
CVMFS_EXPORT uint64_t String2Uint64(const std::string &value);
CVMFS_EXPORT bool String2Uint64Parse(const std::string &value,
                                     uint64_t *result);

CVMFS_EXPORT void String2Uint64Pair(const std::string &value, uint64_t *a,
                                    uint64_t *b);
CVMFS_EXPORT bool HasPrefix(const std::string &str, const std::string &prefix,
                            const bool ignore_case);
CVMFS_EXPORT bool HasSuffix(const std::string &str, const std::string &suffix,
                            const bool ignore_case);

CVMFS_EXPORT std::vector<std::string> SplitStringBounded(unsigned max_chunks,
                                                         const std::string &str,
                                                         char delim);
CVMFS_EXPORT std::vector<std::string> SplitString(const std::string &str,
                                                  char delim);
CVMFS_EXPORT std::vector<std::string> SplitStringMultiChar(
    const std::string &str, const std::string &delim);

CVMFS_EXPORT std::string JoinStrings(const std::vector<std::string> &strings,
                                     const std::string &joint);
CVMFS_EXPORT void ParseKeyvalMem(const unsigned char *buffer,
                                 const unsigned buffer_size,
                                 std::map<char, std::string> *content);
CVMFS_EXPORT bool ParseKeyvalPath(const std::string &filename,
                                  std::map<char, std::string> *content);

CVMFS_EXPORT std::string GetLineMem(const char *text, const int text_size);
CVMFS_EXPORT bool GetLineFile(FILE *f, std::string *line);
CVMFS_EXPORT bool GetLineFd(const int fd, std::string *line);
CVMFS_EXPORT std::string Trim(const std::string &raw,
                              bool trim_newline = false);
CVMFS_EXPORT std::string TrimString(const std::string &path,
                                    const std::string &toTrim,
                                    const int trimMode = kTrimAll);

CVMFS_EXPORT std::string ToUpper(const std::string &mixed_case);
CVMFS_EXPORT std::string ReplaceAll(const std::string &haystack,
                                    const std::string &needle,
                                    const std::string &replace_by);
CVMFS_EXPORT std::string Tail(const std::string &source, unsigned num_lines);

CVMFS_EXPORT std::string Base64(const std::string &data);
CVMFS_EXPORT std::string Base64Url(const std::string &data);
CVMFS_EXPORT bool Debase64(const std::string &data, std::string *decoded);
CVMFS_EXPORT std::string GetGMTimestamp(
    const std::string &format = "%Y-%m-%d %H:%M:%S");

#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif

#endif  // CVMFS_UTIL_STRING_H_
