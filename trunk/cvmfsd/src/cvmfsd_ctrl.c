#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <string.h>


static void usage() 
{
   printf("Usage: cvmfs_ctrl [pause|resume|flush|lvmrotate]\n");
}

int main(int argc, char **argv)
{
   if ( (argc != 2) || 
        ((strcmp(argv[1], "pause") != 0) && (strcmp(argv[1], "resume") != 0) &&
         (strcmp(argv[1], "flush") != 0) && (strcmp(argv[1], "lvmrotate") != 0)) )
   {
      usage();
      return 1;
   }
   
   setuid(0);
   int retval;
   if (strcmp(argv[1], "pause") == 0)
      retval = system("/etc/init.d/cvmfsd pause; exit $?");
   else if (strcmp(argv[1], "resume") == 0)
      retval = system("/etc/init.d/cvmfsd resume; exit $?");
   else if (strcmp(argv[1], "flush") == 0)
      retval = system("/etc/init.d/cvmfsd flush; exit $?");
   else if (strcmp(argv[1], "lvmrotate") == 0)
      retval = system("/usr/bin/cvmfs-lvmrotate publish; exit $?");
   else {
      usage();
      return 1;
   }
      
   int exit_code;
   if (WIFEXITED(retval)) exit_code = WEXITSTATUS(retval);
   else exit_code = WTERMSIG(retval);
   
   return exit_code;
}
