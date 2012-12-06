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
  int setuid_success;

  pwd = getpwnam(user);

  setuid_success = setuid(pwd->pw_uid);

  if (setuid_success == 0) {
	  memset (buf, 0, sizeof(buf));
	  while (argv[i]) {
		p += sprintf(p, " %s", argv[i++]);
	  }
	  system(buf);
  }
  else {
	  printf("Cannot setuid to cvmfs-test. Exiting.\n");
  }
  return 0;
}

