/**
 * This file is part of the CernVM File System.
 *
 * Some common functions.
 */

#define __STDC_FORMAT_MACROS

#include "cvmfs_config.h"
#include "util.h"

#include <sys/stat.h>
#include <sys/time.h>
#include <sys/file.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <arpa/inet.h>
#include <sys/wait.h>
#include <time.h>
#include <inttypes.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <pthread.h>

#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <cassert>

#include <string>
#include <map>

#include "hash.h"
#include "smalloc.h"
#include "logging.h"

using namespace std;  // NOLINT


/**
 * Removes a trailing "/" from a path.
 */
string MakeCanonicalPath(const string &path) {
  if (path.length() == 0) return path;

  if (path[path.length()-1] == '/')
    return path.substr(0, path.length()-1);
  else
    return path;
}


/**
 * Gets the directory part of a path.
 */
string GetParentPath(const string &path) {
  const string::size_type idx = path.find_last_of('/');
  if (idx != string::npos)
    return path.substr(0, idx);
  else
    return "";
}


/**
 * Gets the file name part of a path.
 */
PathString GetParentPath(const PathString &path) {
  unsigned length = path.GetLength();
  if (length == 0)
    return path;
  const char *chars  = path.GetChars();

  for (unsigned i = length-1; i >= 0; --i) {
    if (chars[i] == '/')
      return PathString(chars, i);
  }

  return path;
}


/**
 * Gets the file name part of a path.
 */
string GetFileName(const string &path) {
  const string::size_type idx = path.find_last_of('/');
  if (idx != string::npos)
    return path.substr(idx+1);
  else
    return path;
}


/**
 * Creates and binds to a named socket.
 */
int MakeSocket(const string &path, const int mode) {
  struct sockaddr_un sock_addr;
  assert(path.length() < sizeof(sock_addr.sun_path));
  sock_addr.sun_family = AF_UNIX;
  strncpy(sock_addr.sun_path, path.c_str(), sizeof(sock_addr.sun_path));

  const int socket_fd = socket(AF_UNIX, SOCK_STREAM, 0);
  assert(socket_fd != -1);

#ifndef __APPLE__
  // fchmod on a socket is not allowed under Mac OS X
  // using default 0770 here
  if (fchmod(socket_fd, mode) != 0)
    return false;
#endif

  if (bind(socket_fd, (struct sockaddr *)&sock_addr,
           sizeof(sock_addr.sun_family) + sizeof(sock_addr.sun_path)) < 0)
  {
    if ((errno == EADDRINUSE) && (unlink(path.c_str()) == 0)) {
      // Second try, perhaps the file was left over
      if (bind(socket_fd, (struct sockaddr *)&sock_addr,
               sizeof(sock_addr.sun_family) + sizeof(sock_addr.sun_path)) < 0)
      {
        return -1;
      }
    } else {
      return -1;
    }
  }

  return socket_fd;
}


/**
 * Connects to a named socket.
 *
 * \return socket file descriptor on success, -1 else
 */
int ConnectSocket(const string &path) {
  struct sockaddr_un sock_addr;
  assert(path.length() < sizeof(sock_addr.sun_path));
  sock_addr.sun_family = AF_UNIX;
  strncpy(sock_addr.sun_path, path.c_str(), sizeof(sock_addr.sun_path));

  const int socket_fd = socket(AF_UNIX, SOCK_STREAM, 0);
  assert(socket_fd != -1);

  if (connect(socket_fd, (struct sockaddr *)&sock_addr,
              sizeof(sock_addr.sun_family) + sizeof(sock_addr.sun_path)) < 0)
  {
    close (socket_fd);
    return -1;
  }

  return socket_fd;
}


/**
 * Creating a pipe should always succeed.
 */
void MakePipe(int pipe_fd[2]) {
  int retval = pipe(pipe_fd);
  assert(retval == 0);
}


/**
 * Writes to a pipe should always succeed.
 */
void WritePipe(int fd, const void *buf, size_t nbyte) {
  int num_bytes = write(fd, buf, nbyte);
  assert((num_bytes >= 0) && (static_cast<size_t>(num_bytes) == nbyte));
}


/**
 * Reads from a pipe should always succeed.
 */
void ReadPipe(int fd, void *buf, size_t nbyte) {
  int num_bytes = read(fd, buf, nbyte);
  assert((num_bytes >= 0) && (static_cast<size_t>(num_bytes) == nbyte));
}


/**
 * Reads from a pipe where writer's end is not yet necessarily connected
 */
void ReadHalfPipe(int fd, void *buf, size_t nbyte) {
  int num_bytes;
  do {
    num_bytes = read(fd, buf, nbyte);
  } while (num_bytes == 0);
  assert((num_bytes >= 0) && (static_cast<size_t>(num_bytes) == nbyte));
}


/**
 * Closes both ends of a pipe
 */
void ClosePipe(int pipe_fd[2]) {
  close(pipe_fd[0]);
  close(pipe_fd[1]);
}


/**
 * Changes a non-blocking file descriptor to a blocking one.
 */
void Nonblock2Block(int filedes) {
  int flags = fcntl(filedes, F_GETFL);
  assert(flags != -1);
  int retval = fcntl(filedes, F_SETFL, flags & ~O_NONBLOCK);
  assert(retval != -1);
}


/**
 * Checks if the regular file path exists.
 */
bool FileExists(const string &path) {
  platform_stat64 info;
  return ((platform_lstat(path.c_str(), &info) == 0) &&
          S_ISREG(info.st_mode));
}


/**
 * Returns -1 on failure.
 */
int64_t GetFileSize(const string &path) {
  platform_stat64 info;
  int retval = platform_stat(path.c_str(), &info);
  if (retval != 0)
    return -1;
  return info.st_size;
}


/**
 * Checks if the directory (not symlink) path exists.
 */
bool DirectoryExists(const std::string &path) {
  platform_stat64 info;
  return ((platform_lstat(path.c_str(), &info) == 0) &&
          S_ISDIR(info.st_mode));
}


/**
 * The mkdir -p command.
 */
bool MkdirDeep(const std::string &path, const mode_t mode) {
  if (path == "") return false;

  int retval = mkdir(path.c_str(), mode);
  if (retval == 0) return true;

  if (errno == EEXIST) {
    platform_stat64 info;
    if ((platform_lstat(path.c_str(), &info) == 0) && S_ISDIR(info.st_mode))
      return true;
    return false;
  }

  if ((errno == ENOENT) && (MkdirDeep(GetParentPath(path), mode))) {
    return mkdir(path.c_str(), mode) == 0;
  }

  return false;
}


/**
 * Creates the "hash cache" directory structure in path.
 */
bool MakeCacheDirectories(const string &path, const mode_t mode) {
  const string canonical_path = MakeCanonicalPath(path);

  string this_path = canonical_path + "/quarantaine";
  if (!MkdirDeep(this_path, mode)) return false;

  this_path = canonical_path + "/ff";

  platform_stat64 stat_info;
  if (platform_stat(this_path.c_str(), &stat_info) != 0) {
    if (mkdir(this_path.c_str(), mode) != 0) return false;
    this_path = canonical_path + "/txn";
    if (mkdir(this_path.c_str(), mode) != 0) return false;
    for (int i = 0; i < 0xff; i++) {
      char hex[3];
      snprintf(hex, sizeof(hex), "%02x", i);
      this_path = canonical_path + "/" + string(hex);
      if (mkdir(this_path.c_str(), mode) != 0) return false;
    }
  }
  return true;
}

/**
 * Locks file path, blocks if file is already locked.  Creates path if required.
 *
 * \return file descriptor, -1 on error
 */
int LockFile(const std::string &path) {
  const int fd_lockfile = open(path.c_str(), O_RDONLY | O_CREAT, 0600);
  if (fd_lockfile < 0)
    return -1;

  
  if (flock(fd_lockfile, LOCK_EX | LOCK_NB) != 0) {
    if (errno != EWOULDBLOCK)
      return -1;
    LogCvmfs(kLogCvmfs, kLogSyslog, "another process holds %s, waiting.",
             path.c_str());
    if (flock(fd_lockfile, LOCK_EX) != 0)
      return -1;
    LogCvmfs(kLogCvmfs, kLogSyslog, "lock %s acquired", path.c_str());
  }

  return fd_lockfile;
}


void UnlockFile(const int filedes) {
  int retval = flock(filedes, LOCK_UN);
  assert(retval == 0);
  close(filedes);
}


/**
 * Wrapper around mkstemp.
 */
FILE *CreateTempFile(const string &path_prefix, const int mode,
                     const char *open_flags, string *final_path)
{
  *final_path = path_prefix + ".XXXXXX";
  char *tmp_file = strdupa(final_path->c_str());
  int tmp_fd = mkstemp(tmp_file);
  if (tmp_fd < 0)
    return NULL;
  if (fchmod(tmp_fd, mode) != 0) {
    close(tmp_fd);
    return NULL;
  }

  *final_path = tmp_file;
  FILE *tmp_fp = fdopen(tmp_fd, open_flags);
  if (!tmp_fp) {
    close(tmp_fd);
    unlink(tmp_file);
    return NULL;
  }

  return tmp_fp;
}


/**
 * Create the file but don't open.  Use only in non-public tmp directories.
 */
string CreateTempPath(const std::string &path_prefix, const int mode) {
  string result;
  FILE *f = CreateTempFile(path_prefix, mode, "w", &result);
  if (!f)
    return "";
  fclose(f);
  return result;
}


string StringifyInt(const int64_t value) {
  char buffer[48];
  snprintf(buffer, sizeof(buffer), "%"PRId64, value);
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

  const char *months[] = {"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul",
    "Aug", "Sep", "Oct", "Nov", "Dec"};
  char buffer[20];
  snprintf(buffer, sizeof(buffer), "%d %s %d %02d:%02d:%02d", timestamp.tm_mday,
           months[timestamp.tm_mon], timestamp.tm_year + 1900,
           timestamp.tm_hour, timestamp.tm_min, timestamp.tm_sec);

  return string(buffer);
}


string StringifyTimeval(const timeval value) {
  char buffer[64];
  int64_t msec = value.tv_sec * 1000;
  msec += value.tv_usec / 1000;
  snprintf(buffer, sizeof(buffer), "%"PRId64".%03d",
           msec, static_cast<int>(value.tv_usec % 1000));
  return string(buffer);
}


string StringifyIpv4(const uint32_t ip4_address) {
  struct in_addr in_addr;
  in_addr.s_addr = ip4_address;
  return string(inet_ntoa(in_addr));
}


int64_t String2Int64(const string &value) {
  int64_t result;
  sscanf(value.c_str(), "%"PRId64, &result);
  return result;
}


uint64_t String2Uint64(const string &value) {
  uint64_t result;
  sscanf(value.c_str(), "%"PRIu64, &result);
  return result;
}


void String2Uint64Pair(const string &value, uint64_t *a, uint64_t *b) {
  sscanf(value.c_str(), "%"PRIu64" %"PRIu64, a, b);
}


bool HasPrefix(const string &str, const string &prefix,
               const bool ignore_case)
{
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


vector<string> SplitString(const string &str, const char delim) {
  vector<string> result;

  const unsigned size = str.size();
  unsigned marker = 0;
  unsigned i;
  for (i = 0; i < size; ++i) {
    if (str[i] == delim) {
      result.push_back(str.substr(marker, i-marker));
      marker = i+1;
    }
  }
  result.push_back(str.substr(marker, i-marker));

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


double DiffTimeSeconds(struct timeval start, struct timeval end) {
  // Time substraction, from GCC documentation
  if (end.tv_usec < start.tv_usec) {
    int nsec = (end.tv_usec - start.tv_usec) / 1000000 + 1;
    start.tv_usec -= 1000000 * nsec;
    start.tv_sec += nsec;
  }
  if (end.tv_usec - start.tv_usec > 1000000) {
    int nsec = (end.tv_usec - start.tv_usec) / 1000000;
    start.tv_usec += 1000000 * nsec;
    start.tv_sec -= nsec;
  }

  // Compute the time remaining to wait in microseconds.
  // tv_usec is certainly positive.
  uint64_t elapsed_usec = ((end.tv_sec - start.tv_sec)*1000000) +
  (end.tv_usec - start.tv_usec);
  return static_cast<double>(elapsed_usec)/1000000.0;
}


string GetLine(const char *text, const int text_size) {
  int pos = 0;
  while ((pos < text_size) && (text[pos] != '\n'))
    pos++;
  return string(text, pos);
}


bool ParseKeyvalMem(const unsigned char *buffer, const unsigned buffer_size,
                    int *start_of_signature,
                    hash::Any *hash, map<char, string> *content)
{
  string line;
  unsigned pos = 0;
  while (pos < buffer_size) {
    if (static_cast<char>(buffer[pos]) == '\n') {
      if (line == "--") {
        pos++;
        break;
      }
      if (line != "") {
        const string tail = (line.length() == 1) ? "" : line.substr(1);
        (*content)[line[0]] = tail;
      }
      line = "";
    } else {
      line += static_cast<char>(buffer[pos]);
    }
    pos++;
  }

  *start_of_signature = pos;
  line = "";
  while ((pos < buffer_size) && (buffer[pos] != '\n')) {
    line += static_cast<char>(buffer[pos]);
    pos++;
  }
  if (line.length() == 2*hash::kDigestSizes[hash::kSha1]) {
    *hash = hash::Any(hash::kSha1, hash::HexPtr(line));
  } else {
    *start_of_signature = -1;
    *hash = hash::Any();
  }

  return true;
}


bool ParseKeyvalPath(const string &filename, map<char, string> *content) {
  int fd = open(filename.c_str(), O_RDONLY);
  if (fd < 0)
    return false;

  unsigned char buffer[4096];
  int num_bytes = read(fd, buffer, sizeof(buffer));
  close(fd);

  if ((num_bytes <= 0) || (unsigned(num_bytes) >= sizeof(buffer)))
    return false;

  int start_of_signature;
  hash::Any hash;
  return ParseKeyvalMem(buffer, unsigned(num_bytes),
                        &start_of_signature, &hash, content);
}


/**
 * Makes a daemon.  The daemon() call is deprecated on OS X
 */
void Daemonize() {
  pid_t pid;
  int statloc;
  if ((pid = fork()) == 0) {
    int retval = setsid();
    assert(retval != -1);
    if ((pid = fork()) == 0) {
      int null_read = open("/dev/null", O_RDONLY);
      int null_write = open("/dev/null", O_WRONLY);
      assert((null_read >= 0) && (null_write >= 0));
      retval = dup2(null_read, 0); 
      assert(retval == 0);
      retval = dup2(null_write, 1);
      assert(retval == 1);
      retval = dup2(null_write, 2);
      assert(retval == 2);
      close(null_read);
      close(null_write);
      LogCvmfs(kLogCvmfs, kLogDebug, "daemonized");
    } else {
      assert(pid > 0);
      _exit(0);
    }
  } else {
    assert(pid > 0);
    waitpid(pid, &statloc, 0);
    _exit(0);
  }
}


/**
 * Execve to the given command line, preserving the given file descriptors.
 * If stdin, stdout, stderr should be preserved, add 0, 1, 2.
 * Does a double fork to detach child.
 * The command_line parameter contains the binary at index 0 and the arguments
 * in the rest of the vector.
 */
bool ManagedExec(const vector<string> &command_line, 
                 const vector<int> &preserve_fildes)
{
  assert(command_line.size() >= 1);
  
  int pipe_fork[2];
  MakePipe(pipe_fork);
  pid_t pid = fork();
  assert(pid >= 0);
  if (pid == 0) {
    pid_t pid_grand_child;
    int max_fd;
    int fd_flags;
    char failed = 'U';
    const char *argv[command_line.size() + 1];
    for (unsigned i = 0; i < command_line.size(); ++i)
      argv[i] = command_line[i].c_str();
    argv[command_line.size()] = NULL;
    
    // Child, close file descriptors
    max_fd = sysconf(_SC_OPEN_MAX);
    if (max_fd < 0) {
      failed = 'C';
      goto fork_failure;
    }
    for (int fd = 0; fd < max_fd; fd++) {
      bool close_fd = true;
      for (unsigned i = 0; i < preserve_fildes.size(); ++i) {
        if (fd == preserve_fildes[i]) {
          close_fd = false;
          break;
        }
      }
      if (close_fd && (fd != pipe_fork[1]))
        close(fd);
    }
    
    // Double fork to disconnect from parent
    pid_grand_child = fork();
    assert(pid_grand_child >= 0);
    if (pid_grand_child != 0) _exit(0);
    
    fd_flags = fcntl(pipe_fork[1], F_GETFD);
    if (fd_flags < 0) {
      failed = 'G';
      goto fork_failure;
    }
    fd_flags |= FD_CLOEXEC;
    if (fcntl(pipe_fork[1], F_SETFD, fd_flags) < 0) {
      failed = 'S';
      goto fork_failure;
    }
    
    execvp(command_line[0].c_str(), const_cast<char **>(argv));
    
    failed = 'E';
    
   fork_failure:
    write(pipe_fork[1], &failed, 1);
    _exit(1);
  }
  int statloc;
  waitpid(pid, &statloc, 0);
  
  close(pipe_fork[1]);
  char buf;
  if (read(pipe_fork[0], &buf, 1) == 1) {
    LogCvmfs(kLogQuota, kLogDebug, "managed execve failed (%c)", buf);
    return false;
  }
  close(pipe_fork[0]);
  LogCvmfs(kLogCvmfs, kLogDebug, "execve'd %s", command_line[0].c_str());
  return true;
}
