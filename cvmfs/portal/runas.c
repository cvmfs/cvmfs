/**
 * This file is part of the CernVM File System
 *
 * Starts a go service as a different user. Go has no built-in support for that
 */

#include <errno.h>
#include <pwd.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

int main(int argc, char **argv) {
  if (argc < 3) {
    return 1;
  }
  int retval;

  // Resolve user name to uid/gid
  char *username = argv[1];
  struct passwd *pwd = getpwnam(username);
  if (pwd == NULL)
    return 1;

  // Switch user
  retval = setgid(pwd->pw_gid) || setuid(pwd->pw_uid);
  if (retval != 0)
    return retval;

  printf("CernVM-FS: impersonated user %s\n", argv[1]);

  // Call main binary
  retval = execvp(argv[2], &argv[2]);
  return retval;
}
