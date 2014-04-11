/**
 * This file is part of the CernVM File System.
 *
 * It implements the CernVM-FS autofs map; it generates mount options
 * for a fully qualified repository name.
 */

#include <cstdio>
#include <string>

using namespace std;  // NOLINT

static bool IsInRange(const char c, const char begin, const char end) {
  return (c >= begin) && (c <= end);
}

static bool IsAlphanum(const char c) {
  return IsInRange(c, 'a', 'z') || IsInRange(c, 'A', 'Z') ||
         IsInRange(c, '0', '9');
}


int main(int argc, char **argv) {
  if (argc != 2)
    return 1;

  string hostname = argv[1];

  // Sanitize hostname
  if (hostname.empty())
    return 1;
  if (!IsAlphanum(hostname[0]))
    return 1;
  bool has_dot = false;
  for (unsigned i = 1; i < hostname.length(); ++i) {
    if (hostname[i] == '.') {
      has_dot = true;
      continue;
    }
    if (IsAlphanum(hostname[i]) ||
        (hostname[i] == '-') ||
        (hostname[i] == '_'))
    {
      continue;
    }
    return 1;
  }
  if (!has_dot)
    return 1;
  if (hostname[hostname.length()-1] == '.')
    return 1;

  printf("-fstype=cvmfs :%s\n", hostname.c_str());
  return 0;
}
