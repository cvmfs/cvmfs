/**
 * This file is part of the CernVM File System.
 */


#include "cmd_util.h"

#include <unistd.h>

#include <map>
#include <set>
#include <string>
#include <vector>

#include "publish/except.h"
#include "util/posix.h"

int publish::CallServerHook(const std::string &func,
                            const std::string &fqrn,
                            const std::string &path_hooks) {
  if (!FileExists(path_hooks))
    return 0;

  int pipe_stdin[2];
  MakePipe(pipe_stdin);
  std::set<int> preserve_fildes;
  preserve_fildes.insert(0);
  preserve_fildes.insert(1);
  preserve_fildes.insert(2);
  std::map<int, int> map_fildes;
  map_fildes[pipe_stdin[0]] = 0;  // Reading end of pipe_stdin
  std::vector<std::string> cmd_line;
  cmd_line.push_back("/bin/sh");
  pid_t child_pid;
  bool rvb = ManagedExec(cmd_line,
                         preserve_fildes,
                         map_fildes,
                         false /* drop_credentials */,
                         false /* clear_env */,
                         false /* double_fork */,
                         &child_pid);
  if (!rvb) {
    ClosePipe(pipe_stdin);
    return -127;
  }
  close(pipe_stdin[0]);

  const std::string callout = func + "() { :; }\n" + ". " + path_hooks + "\n"
                              + func + " " + fqrn + "\n";
  WritePipe(pipe_stdin[1], callout.data(), callout.length());
  close(pipe_stdin[1]);

  return WaitForChild(child_pid);
}
