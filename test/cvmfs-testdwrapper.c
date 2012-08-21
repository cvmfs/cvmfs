#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <stdlib.h>
#include <string.h>
#include <pwd.h>

#define CMDMAXLGTH 4096
#define SETUIDUSER "cvmfs-test"

int main( int argc, char ** argv ) {
  struct passwd *pwd;
  char user[] = SETUIDUSER;
  char buf[CMDMAXLGTH];
  char *p = buf;
  int i = 1;

  pwd = getpwnam(user);
  // success test needed here 
  setuid(pwd->pw_uid);
  // success test needed here 

  memset (buf, 0, sizeof(buf));
  while (argv[i]) {
    p += sprintf(p, " %s", argv[i++]);
  }
  system(buf);
  return 0;
}

