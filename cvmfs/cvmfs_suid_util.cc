/**
 * This file is part of the CernVM File System
 */

#include "cvmfs_suid_util.h"

#include <sys/stat.h>
#include <unistd.h>

#include <cassert>
#include <climits>
#include <cstdlib>

#include "sanitizer.h"

using namespace std;  // NOLINT

namespace cvmfs_suid {

/**
 * Makes a systemd mount unit string from the given path, such as
 * a-b-c.mount from /a/b/c
 */
string EscapeSystemdUnit(const string &path) {
  assert(!path.empty());

  string normalized_path(path);
  size_t pos;
  while ((pos = normalized_path.find("//")) != string::npos) {
    normalized_path.replace(pos, 2, "/");
  }

  if (normalized_path == "/")
    return "-.mount";

  const sanitizer::InputSanitizer sanitizer("az AZ 09 _");
  const unsigned length = normalized_path.length();
  string result;
  for (unsigned i = 0; i < length; ++i) {
    const char c = normalized_path[i];
    if (c == '/') {
      if ((i == 0) || (i == length - 1))
        continue;
      result.push_back('-');
    } else if ((c == '.') && (i > 0)) {
      result.push_back('.');
    } else if (sanitizer.IsValid(string(&c, 1))) {
      result.push_back(c);
    } else {
      result.push_back('\\');
      result.push_back('x');
      result.push_back((c / 16) + ((c / 16 <= 9) ? '0' : 'a' - 10));
      result.push_back((c % 16) + ((c % 16 <= 9) ? '0' : 'a' - 10));
    }
  }

  return result + ".mount";
}


bool PathExists(const std::string &path) {
  struct stat info;
  const int retval = stat(path.c_str(), &info);
  return retval == 0;
}


string ResolvePath(const std::string &path) {
  char buf[PATH_MAX];
  char *retval = realpath(path.c_str(), buf);
  if (retval == NULL)
    return "";
  return string(buf);
}

}  // namespace cvmfs_suid
