/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include "../../cvmfs/util.h"
#include "testutil.h"


TEST(T_ManagedExec, RunShell) {
  int fd_stdin;
  int fd_stdout;
  int fd_stderr;
  bool retval = Shell(&fd_stdin, &fd_stdout, &fd_stderr);
  EXPECT_TRUE(retval);

  Pipe shell_pipe(fd_stdout, fd_stdin);
  const char *command = "echo \"Hello World\"\n";
  retval = shell_pipe.Write(command, strlen(command));
  EXPECT_TRUE(retval);

  char buffer[20];
  retval = shell_pipe.Read(&buffer, 11);
  EXPECT_TRUE(retval);
  buffer[11] = '\0';

  EXPECT_EQ(0, strncmp("Hello World", buffer, 12));
}


TEST(T_ManagedExec, ExecuteBinaryDoubleFork) {
  int fd_stdin, fd_stdout, fd_stderr;
  pid_t child_pid;

  // find gdb in the $PATH of this system
  const std::string gdb = GetExecutablePath("gdb");
  ASSERT_NE("", gdb) << "gdb not found, but needed by this test case";

  // spawn detached (double forked) child process
  const bool double_fork = true;
  bool retval = ExecuteBinary(&fd_stdin,
                              &fd_stdout,
                              &fd_stderr,
                               gdb,
                               std::vector<std::string>(),
                               double_fork,
                              &child_pid);
  // check if process is running
  ASSERT_TRUE(retval);
  EXPECT_EQ(0, kill(child_pid, 0));

  // check that the PPID of the process is 1 (belongs to init)
  pid_t child_parent_pid = GetParentPid(child_pid);
  EXPECT_EQ(1, child_parent_pid);

  // tell the process to terminate
  Pipe shell_pipe(fd_stdout, fd_stdin);
  const char *quit = "quit\n";
  retval = shell_pipe.Write(quit, strlen(quit));
  EXPECT_TRUE(retval);
  shell_pipe.Close();
  close(fd_stderr);

  // wait for the child process to terminate
  const unsigned int timeout = 1000;
  unsigned int counter = 0;
  while (counter < timeout && kill(child_pid, 0) == 0) {
    usleep(5000);
    ++counter;
  }
  EXPECT_LT(counter, timeout) << "detached process did not terminate in time";
}


TEST(T_ManagedExec, ExecuteBinaryAsChild) {
  int fd_stdin, fd_stdout, fd_stderr;
  pid_t child_pid;
  pid_t my_pid = getpid();

  // spawn a child process (not double forked)
  const bool double_fork = false;
  bool retval = ExecuteBinary(&fd_stdin,
                              &fd_stdout,
                              &fd_stderr,
                               "gdb",
                               std::vector<std::string>(),
                               double_fork,
                              &child_pid);

  // check that the process is running
  ASSERT_TRUE(retval);
  EXPECT_EQ(0, kill(child_pid, 0));

  // check that we are the parent of the spawned process
  pid_t child_parent_pid = GetParentPid(child_pid);
  EXPECT_EQ(my_pid, child_parent_pid);

  // tell the process to terminate
  Pipe shell_pipe(fd_stdout, fd_stdin);
  const char *quit = "quit\n";
  retval = shell_pipe.Write(quit, strlen(quit));
  EXPECT_TRUE(retval);
  shell_pipe.Close();
  close(fd_stderr);

  // wait for the child process to terminate
  int statloc;
  retval = waitpid(child_pid, &statloc, 0);
  EXPECT_NE(-1, retval);
}
