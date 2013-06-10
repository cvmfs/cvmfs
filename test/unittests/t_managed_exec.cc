#include <gtest/gtest.h>

#include "../../cvmfs/util.h"


TEST(T_ManagedExec, RunShell) {
  int fd_stdin;
  int fd_stdout;
  int fd_stderr;
  bool retval = Shell(&fd_stdin, &fd_stdout, &fd_stderr);
  EXPECT_TRUE (retval);

  Pipe shell_pipe(fd_stdout, fd_stdin);
  const char *command = "echo \"Hello World\"\n";
  retval = shell_pipe.Write(command, strlen(command));
  EXPECT_TRUE (retval);

  char buffer[20];
  retval = shell_pipe.Read(&buffer, 11);
  EXPECT_TRUE (retval);
  buffer[11] = '\0';

  EXPECT_EQ (0, strncmp("Hello World", buffer, 12));
}
