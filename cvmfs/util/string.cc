/**
 * This file is part of the CernVM File System.
 *
 * Some common functions.
 */

#ifndef __STDC_FORMAT_MACROS
// NOLINTNEXTLINE
#define __STDC_FORMAT_MACROS
#endif

#include "string.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdint.h>
#include <unistd.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <string>

using namespace std;  // NOLINT

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

const char b64_table[] = {'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K',
                          'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V',
                          'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f', 'g',
                          'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r',
                          's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '0', '1', '2',
                          '3', '4', '5', '6', '7', '8', '9', '+', '/'};

/**
 * Decode Base64 and Base64Url
 */
const int8_t db64_table[] = {
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, 62, -1, 62, -1, 63, 52, 53, 54, 55, 56, 57, 58, 59, 60,
    61, -1, -1, -1, 0,  -1, -1, -1, 0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10,
    11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, -1, -1, -1, -1,
    63, -1, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42,
    43, 44, 45, 46, 47, 48, 49, 50, 51, -1, -1, -1, -1, -1,

    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
};

namespace {

/**
 * Used for cas  insensitive HasSuffix
 */
struct IgnoreCaseComperator {
  IgnoreCaseComperator() { }
  bool operator()(const std::string::value_type a,
                  const std::string::value_type b) const {
    return std::tolower(a) == std::tolower(b);
  }
};

}  // anonymous namespace

string StringifyBool(const bool value) { return value ? "yes" : "no"; }

string StringifyInt(const int64_t value) {
  char buffer[48];
  snprintf(buffer, sizeof(buffer), "%" PRId64, value);
  return string(buffer);
}

std::string StringifyUint(const uint64_t value) {
  char buffer[48];
  snprintf(buffer, sizeof(buffer), "%" PRIu64, value);
  return string(buffer);
}

string StringifyByteAsHex(const unsigned char value) {
  char buffer[3];
  snprintf(buffer, sizeof(buffer), "%02x", value);
  return string(buffer);
}

string StringifyDouble(const double value) {
  char buffer[64];
  snprintf(buffer, sizeof(buffer), "%.03f", value);
  return string(buffer);
}

/**
 * Converts seconds since UTC 0 into something readable
 */
string StringifyTime(const time_t seconds, const bool utc) {
  struct tm timestamp;
  if (utc) {
    localtime_r(&seconds, &timestamp);
  } else {
    gmtime_r(&seconds, &timestamp);
  }

  const char *months[] = {"Jan", "Feb", "Mar", "Apr", "May", "Jun",
                          "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
  char buffer[21];
  snprintf(buffer, sizeof(buffer), "%d %s %d %02d:%02d:%02d", timestamp.tm_mday,
           months[timestamp.tm_mon], timestamp.tm_year + 1900,
           timestamp.tm_hour, timestamp.tm_min, timestamp.tm_sec);

  return string(buffer);
}

/**
 * Converts seconds since UTC 0 into something like 12 Sep 14:59:37 CDT
 */
string StringifyLocalTime(const time_t seconds) {
  struct tm timestamp;
  localtime_r(&seconds, &timestamp);

  const char *months[] = {"Jan", "Feb", "Mar", "Apr", "May", "Jun",
                          "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
  char buffer[26];
  (void)/* cast to void ignores return and placates clang-tidy */
      snprintf(buffer, sizeof(buffer), "%d %s %d %02d:%02d:%02d %s",
               timestamp.tm_mday, months[timestamp.tm_mon],
               timestamp.tm_year + 1900, timestamp.tm_hour, timestamp.tm_min,
               timestamp.tm_sec, timestamp.tm_zone);

  return string(buffer);
}


/**
 * Current time in format Wed, 01 Mar 2006 12:00:00 GMT
 */
std::string RfcTimestamp() {
  const char *months[] = {"Jan", "Feb", "Mar", "Apr", "May", "Jun",
                          "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
  const char *day_of_week[] = {"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"};

  struct tm timestamp;
  time_t now = time(NULL);
  gmtime_r(&now, &timestamp);

  char buffer[30];
  snprintf(buffer, sizeof(buffer), "%s, %02d %s %d %02d:%02d:%02d %s",
           day_of_week[timestamp.tm_wday], timestamp.tm_mday,
           months[timestamp.tm_mon], timestamp.tm_year + 1900,
           timestamp.tm_hour, timestamp.tm_min, timestamp.tm_sec,
           timestamp.tm_zone);
  return string(buffer);
}


/**
 * Current time in format YYYYMMDDTHHMMSSZ.  Used in AWS4 requests.
 */
std::string IsoTimestamp() {
  struct tm timestamp;
  time_t now = time(NULL);
  gmtime_r(&now, &timestamp);

  char buffer[17];
  snprintf(buffer, sizeof(buffer), "%04d%02d%02dT%02d%02d%02dZ",
           timestamp.tm_year + 1900, timestamp.tm_mon + 1, timestamp.tm_mday,
           timestamp.tm_hour, timestamp.tm_min, timestamp.tm_sec);
  return string(buffer);
}


/**
 * UTC time in format YYYYMMDDHHMMSS.  Used in cvmfs whitelists.
 */
std::string WhitelistTimestamp(time_t when) {
  struct tm timestamp;
  gmtime_r(&when, &timestamp);

  char buffer[15];
  snprintf(buffer, sizeof(buffer), "%04d%02d%02d%02d%02d%02d",
           timestamp.tm_year + 1900, timestamp.tm_mon + 1, timestamp.tm_mday,
           timestamp.tm_hour, timestamp.tm_min, timestamp.tm_sec);
  return string(buffer);
}


string StringifyTimeval(const timeval value) {
  char buffer[64];
  int64_t msec = value.tv_sec * 1000;
  msec += value.tv_usec / 1000;
  snprintf(buffer, sizeof(buffer), "%" PRId64 ".%03d", msec,
           static_cast<int>(value.tv_usec % 1000));
  return string(buffer);
}

/**
 * Parses a timestamp of the form YYYY-MM-DDTHH:MM:SSZ
 * Return 0 on error
 */
time_t IsoTimestamp2UtcTime(const std::string &iso8601) {
  time_t utc_time = 0;
  unsigned length = iso8601.length();

  if (length != 20)
    return utc_time;
  if ((iso8601[4] != '-') || (iso8601[7] != '-') || (iso8601[10] != 'T')
      || (iso8601[13] != ':') || (iso8601[16] != ':') || (iso8601[19] != 'Z')) {
    return utc_time;
  }

  struct tm tm_wl;
  memset(&tm_wl, 0, sizeof(struct tm));
  tm_wl.tm_year = static_cast<int>(String2Int64(iso8601.substr(0, 4))) - 1900;
  tm_wl.tm_mon = static_cast<int>(String2Int64(iso8601.substr(5, 2))) - 1;
  tm_wl.tm_mday = static_cast<int>(String2Int64(iso8601.substr(8, 2)));
  tm_wl.tm_hour = static_cast<int>(String2Int64(iso8601.substr(11, 2)));
  tm_wl.tm_min = static_cast<int>(String2Int64(iso8601.substr(14, 2)));
  tm_wl.tm_sec = static_cast<int>(String2Int64(iso8601.substr(17, 2)));
  utc_time = timegm(&tm_wl);
  if (utc_time < 0)
    return 0;

  return utc_time;
}

int64_t String2Int64(const string &value) {
  int64_t result;
  sscanf(value.c_str(), "%" PRId64, &result);
  return result;
}

uint64_t String2Uint64(const string &value) {
  uint64_t result;
  if (sscanf(value.c_str(), "%" PRIu64, &result) == 1) {
    return result;
  }
  return 0;
}

/**
 * Parse a string into a a uint64_t.
 *
 * Unlike String2Uint64, this:
 *   - Checks to make sure the full string is parsed
 *   - Can indicate an error occurred.
 *
 * If an error occurs, this returns false and sets errno appropriately.
 */
bool String2Uint64Parse(const std::string &value, uint64_t *result) {
  char *endptr = NULL;
  errno = 0;
  long long myval = strtoll(value.c_str(), &endptr, 10);  // NOLINT
  if ((value.size() == 0) || (endptr != (value.c_str() + value.size()))
      || (myval < 0)) {
    errno = EINVAL;
    return false;
  }
  if (errno) {
    return false;
  }
  if (result) {
    *result = myval;
  }
  return true;
}

void String2Uint64Pair(const string &value, uint64_t *a, uint64_t *b) {
  sscanf(value.c_str(), "%" PRIu64 " %" PRIu64, a, b);
}

bool HasPrefix(const string &str, const string &prefix,
               const bool ignore_case) {
  if (prefix.length() > str.length())
    return false;

  for (unsigned i = 0, l = prefix.length(); i < l; ++i) {
    if (ignore_case) {
      if (toupper(str[i]) != toupper(prefix[i]))
        return false;
    } else {
      if (str[i] != prefix[i])
        return false;
    }
  }
  return true;
}

bool HasSuffix(const std::string &str, const std::string &suffix,
               const bool ignore_case) {
  if (suffix.size() > str.size())
    return false;
  const IgnoreCaseComperator icmp;
  return (ignore_case)
             ? std::equal(suffix.rbegin(), suffix.rend(), str.rbegin(), icmp)
             : std::equal(suffix.rbegin(), suffix.rend(), str.rbegin());
}

vector<string> SplitString(const string &str, char delim) {
  return SplitStringBounded(0, str, delim);
}

vector<string> SplitStringBounded(unsigned max_chunks, const string &str,
                                  char delim) {
  vector<string> result;

  // edge case... one chunk is always the whole string
  if (1 == max_chunks) {
    result.push_back(str);
    return result;
  }

  // split the string
  const unsigned size = str.size();
  unsigned marker = 0;
  unsigned chunks = 1;
  unsigned i;
  for (i = 0; i < size; ++i) {
    if (str[i] == delim) {
      result.push_back(str.substr(marker, i - marker));
      marker = i + 1;

      // we got what we want... good bye
      if (++chunks == max_chunks)
        break;
    }
  }

  // push the remainings of the string and return
  result.push_back(str.substr(marker));
  return result;
}

vector<string> SplitStringMultiChar(const string &str, const string &delim) {
  size_t pos_start = 0, pos_end = 0, delim_len = delim.length();
  std::string substring;
  std::vector<std::string> result;

  while ((pos_end = str.find(delim, pos_start)) != string::npos) {
    substring = str.substr(pos_start, pos_end - pos_start);
    pos_start = pos_end + delim_len;
    result.push_back(substring);
  }

  result.push_back(str.substr(pos_start));
  return result;
}

string JoinStrings(const vector<string> &strings, const string &joint) {
  string result = "";
  const unsigned size = strings.size();

  if (size > 0) {
    result = strings[0];
    for (unsigned i = 1; i < size; ++i)
      result += joint + strings[i];
  }

  return result;
}

void ParseKeyvalMem(const unsigned char *buffer, const unsigned buffer_size,
                    map<char, string> *content) {
  string line;
  unsigned pos = 0;
  while (pos < buffer_size) {
    if (static_cast<char>(buffer[pos]) == '\n') {
      if (line == "--")
        return;

      if (line != "") {
        const string tail = (line.length() == 1) ? "" : line.substr(1);
        // Special handling of 'Z' key because it can exist multiple times
        if (line[0] != 'Z') {
          (*content)[line[0]] = tail;
        } else {
          if (content->find(line[0]) == content->end()) {
            (*content)[line[0]] = tail;
          } else {
            (*content)[line[0]] = (*content)[line[0]] + "|" + tail;
          }
        }
      }
      line = "";
    } else {
      line += static_cast<char>(buffer[pos]);
    }
    pos++;
  }
}

bool ParseKeyvalPath(const string &filename, map<char, string> *content) {
  int fd = open(filename.c_str(), O_RDONLY);
  if (fd < 0)
    return false;

  unsigned char buffer[4096];
  ssize_t num_bytes = read(fd, buffer, sizeof(buffer));
  close(fd);

  if ((num_bytes <= 0) || (unsigned(num_bytes) >= sizeof(buffer)))
    return false;

  ParseKeyvalMem(buffer, unsigned(num_bytes), content);
  return true;
}

string GetLineMem(const char *text, const int text_size) {
  int pos = 0;
  while ((pos < text_size) && (text[pos] != '\n'))
    pos++;
  return string(text, pos);
}

bool GetLineFile(FILE *f, std::string *line) {
  int retval;
  line->clear();
  while (true) {
    retval = fgetc(f);
    if (ferror(f) && (errno == EINTR)) {
      clearerr(f);
      continue;
    } else if (retval == EOF) {
      break;
    }
    char c = static_cast<char>(retval);
    if (c == '\n')
      break;
    line->push_back(c);
  }
  return (retval != EOF) || !line->empty();
}

bool GetLineFd(const int fd, std::string *line) {
  ssize_t retval;
  char c;
  line->clear();
  while (true) {
    retval = read(fd, &c, 1);
    if (retval == 0) {
      break;
    }
    if ((retval == -1) && (errno == EINTR)) {
      continue;
    }
    if (retval == -1) {
      break;
    }
    if (c == '\n')
      break;
    line->push_back(c);
  }
  return (retval == 1) || !line->empty();
}

/**
 * Removes leading and trailing whitespaces.
 */
string Trim(const string &raw, bool trim_newline) {
  if (raw.empty())
    return "";

  unsigned start_pos = 0;
  for (; (start_pos < raw.length())
         && (raw[start_pos] == ' ' || raw[start_pos] == '\t'
             || (trim_newline
                 && (raw[start_pos] == '\n' || raw[start_pos] == '\r')));
       ++start_pos) {
  }
  unsigned end_pos = raw.length() - 1;  // at least one character in raw
  for (;
       (end_pos >= start_pos)
       && (raw[end_pos] == ' ' || raw[end_pos] == '\t'
           || (trim_newline && (raw[end_pos] == '\n' || raw[end_pos] == '\r')));
       --end_pos) {
  }

  return raw.substr(start_pos, end_pos - start_pos + 1);
}

std::string TrimString(const std::string &path,
                       const std::string &toTrim,
                       const int trimMode) {
  std::string trimmed = path;
  if (trimmed != toTrim) {
    while ((trimMode & kTrimLeading) && HasPrefix(trimmed, toTrim, true)
           && (trimmed.size() > toTrim.size())) {
      trimmed = trimmed.substr(toTrim.size());
    }
    while ((trimMode & kTrimTrailing) && HasSuffix(trimmed, toTrim, true)
           && (trimmed.size() > toTrim.size())) {
      trimmed = trimmed.substr(0, trimmed.size() - toTrim.size());
    }
  }
  return trimmed;
}

/**
 * Converts all characters to upper case
 */
string ToUpper(const string &mixed_case) {
  string result(mixed_case);
  for (unsigned i = 0, l = result.length(); i < l; ++i) {
    result[i] = static_cast<char>(toupper(result[i]));
  }
  return result;
}

string ReplaceAll(const string &haystack, const string &needle,
                  const string &replace_by) {
  string result(haystack);
  size_t pos = 0;
  const unsigned needle_size = needle.size();
  if (needle == "")
    return result;

  while ((pos = result.find(needle, pos)) != string::npos)
    result.replace(pos, needle_size, replace_by);
  return result;
}

static inline void Base64Block(const unsigned char input[3], const char *table,
                               char output[4]) {
  output[0] = table[(input[0] & 0xFD) >> 2];
  output[1] = table[((input[0] & 0x03) << 4) | ((input[1] & 0xF0) >> 4)];
  output[2] = table[((input[1] & 0x0F) << 2) | ((input[2] & 0xD0) >> 6)];
  output[3] = table[input[2] & 0x3F];
}

string Base64(const string &data) {
  string result;
  result.reserve((data.length() + 3) * 4 / 3);
  unsigned pos = 0;
  const unsigned char *data_ptr = reinterpret_cast<const unsigned char *>(
      data.data());
  const unsigned length = data.length();
  while (pos + 2 < length) {
    char encoded_block[4];
    Base64Block(data_ptr + pos, b64_table, encoded_block);
    result.append(encoded_block, 4);
    pos += 3;
  }
  if (length % 3 != 0) {
    unsigned char input[3];
    input[0] = data_ptr[pos];
    input[1] = ((length % 3) == 2) ? data_ptr[pos + 1] : 0;
    input[2] = 0;
    char encoded_block[4];
    Base64Block(input, b64_table, encoded_block);
    result.append(encoded_block, 2);
    result.push_back(((length % 3) == 2) ? encoded_block[2] : '=');
    result.push_back('=');
  }

  return result;
}

/**
 * Safe encoding for URIs and path names: replace + by - and / by _
 */
string Base64Url(const string &data) {
  string base64 = Base64(data);
  for (unsigned i = 0, l = base64.length(); i < l; ++i) {
    if (base64[i] == '+') {
      base64[i] = '-';
    } else if (base64[i] == '/') {
      base64[i] = '_';
    }
  }
  return base64;
}

static bool Debase64Block(const unsigned char input[4],
                          unsigned char output[3]) {
  int32_t dec[4];
  for (int i = 0; i < 4; ++i) {
    dec[i] = db64_table[input[i]];
    if (dec[i] < 0)
      return false;
  }

  output[0] = (dec[0] << 2) | (dec[1] >> 4);
  output[1] = ((dec[1] & 0x0F) << 4) | (dec[2] >> 2);
  output[2] = ((dec[2] & 0x03) << 6) | dec[3];
  return true;
}

/**
 * Can decode both base64 and base64url
 */
bool Debase64(const string &data, string *decoded) {
  decoded->clear();
  decoded->reserve((data.length() + 4) * 3 / 4);
  unsigned pos = 0;
  const unsigned char *data_ptr = reinterpret_cast<const unsigned char *>(
      data.data());
  const unsigned length = data.length();
  if (length == 0)
    return true;
  if ((length % 4) != 0)
    return false;

  while (pos < length) {
    unsigned char decoded_block[3];
    bool retval = Debase64Block(data_ptr + pos, decoded_block);
    if (!retval)
      return false;
    decoded->append(reinterpret_cast<char *>(decoded_block), 3);
    pos += 4;
  }

  for (int i = 0; i < 2; ++i) {
    pos--;
    if (data[pos] == '=')
      decoded->erase(decoded->length() - 1);
  }
  return true;
}

/**
 * Assumes that source is terminated by a newline
 */
string Tail(const string &source, unsigned num_lines) {
  if (source.empty() || (num_lines == 0))
    return "";

  int l = static_cast<int>(source.length());
  int i = l - 1;
  for (; i >= 0; --i) {
    char c = source.data()[i];
    if (c == '\n') {
      if (num_lines == 0) {
        return source.substr(i + 1);
      }
      num_lines--;
    }
  }
  return source;
}

/**
 * Get UTC Time.
 *
 * @param format format if timestamp (YYYY-MM-DD HH:MM:SS by default)
 * @return a timestamp string on success, empty string on failure
 */
std::string GetGMTimestamp(const std::string &format) {
  struct tm time_ptr;
  char date_and_time[100];
  time_t t = time(NULL);
  gmtime_r(&t, &time_ptr);  // take UTC
  // return empty string if formatting fails
  if (!strftime(date_and_time, 100, format.c_str(), &time_ptr)) {
    return "";
  }
  std::string timestamp(date_and_time);
  return timestamp;
}

#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif
