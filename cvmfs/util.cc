/**
 * This file is part of the CernVM File System.
 *
 * Some common functions.
 */

#define __STDC_FORMAT_MACROS

#include "cvmfs_config.h"
#include "util.h"

#include <arpa/inet.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <inttypes.h>
#include <pthread.h>
#include <pwd.h>
#include <signal.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include <algorithm>
#include <cassert>
#include <cctype>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <set>
#include <string>

#include "fs_traversal.h"
#include "hash.h"
#include "logging.h"
#include "platform.h"
#include "smalloc.h"

using namespace std;  // NOLINT

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

const char b64_table[] = {'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K',
  'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
  'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o',
  'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '0', '1', '2', '3',
  '4', '5', '6', '7', '8', '9', '+', '/'};

/**
 * Decode Base64
 */
const signed char db64_table[] =
  { -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 62, -1, -1, -1, 63,
    52, 53, 54, 55, 56, 57, 58, 59, 60, 61, -1, -1, -1,  0, -1, -1,
    -1,  0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14,
    15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, -1, -1, -1, -1, -1,
    -1, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
    41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, -1, -1, -1, -1, -1,

    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
  };

static pthread_mutex_t getumask_mutex = PTHREAD_MUTEX_INITIALIZER;

namespace {

/**
 * Used for cas  insensitive HasSuffix
 */
struct IgnoreCaseComperator {
  IgnoreCaseComperator() { }
  bool operator() (const std::string::value_type a,
                   const std::string::value_type b) const
  {
    return std::tolower(a) == std::tolower(b);
  }
};

}  // anonymous namespace

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
 * Return both the file and directory name for a given path.
 *
 * NOTE: If only a filename is given, the directory is returned as "."
 */
void SplitPath(
  const std::string &path,
  std::string *dirname,
  std::string *filename)
{
  size_t dir_sep = path.rfind('/');
  if (dir_sep != std::string::npos) {
    *dirname = path.substr(0, dir_sep);
    *filename = path.substr(dir_sep+1);
  } else {
    *dirname = ".";
    *filename = path;
  }
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

  for (int i = length-1; i >= 0; --i) {
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


NameString GetFileName(const PathString &path) {
  NameString name;
  int length = path.GetLength();
  const char *chars  = path.GetChars();

  int i;
  for (i = length-1; i >= 0; --i) {
    if (chars[i] == '/')
      break;
  }
  i++;
  if (i < length) {
    name.Append(chars+i, length-i);
  }

  return name;
}


bool IsAbsolutePath(const std::string &path) {
  return (!path.empty() && path[0] == '/');
}


bool IsHttpUrl(const std::string &path) {
  if (path.length() < 7) {
    return false;
  }

  std::string prefix = path.substr(0, 7);
  std::transform(prefix.begin(), prefix.end(), prefix.begin(), ::tolower);

  return prefix == "http://";
}


/**
 * Abort() on failure
 */
void CreateFile(const std::string &path, const int mode) {
  int fd = open(path.c_str(), O_CREAT, mode);
  assert(fd >= 0);
  close(fd);
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
    goto make_socket_failure;
#endif

  if (bind(socket_fd, (struct sockaddr *)&sock_addr,
           sizeof(sock_addr.sun_family) + sizeof(sock_addr.sun_path)) < 0)
  {
    if ((errno == EADDRINUSE) && (unlink(path.c_str()) == 0)) {
      // Second try, perhaps the file was left over
      if (bind(socket_fd, (struct sockaddr *)&sock_addr,
               sizeof(sock_addr.sun_family) + sizeof(sock_addr.sun_path)) < 0)
      {
        LogCvmfs(kLogCvmfs, kLogDebug, "binding socket failed (%d)", errno);
        goto make_socket_failure;
      }
    } else {
      LogCvmfs(kLogCvmfs, kLogDebug, "binding socket failed (%d)", errno);
      goto make_socket_failure;
    }
  }

  return socket_fd;

 make_socket_failure:
  close(socket_fd);
  return -1;
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
    // LogCvmfs(kLogCvmfs, kLogStderr, "ERROR %d", errno);
    close(socket_fd);
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
  int num_bytes;
  do {
    num_bytes = write(fd, buf, nbyte);
  } while ((num_bytes < 0) && (errno == EINTR));
  assert((num_bytes >= 0) && (static_cast<size_t>(num_bytes) == nbyte));
}


/**
 * Reads from a pipe should always succeed.
 */
void ReadPipe(int fd, void *buf, size_t nbyte) {
  int num_bytes;
  do {
    num_bytes = read(fd, buf, nbyte);
  } while ((num_bytes < 0) && (errno == EINTR));
  assert((num_bytes >= 0) && (static_cast<size_t>(num_bytes) == nbyte));
}


/**
 * Reads from a pipe where writer's end is not yet necessarily connected
 */
void ReadHalfPipe(int fd, void *buf, size_t nbyte) {
  int num_bytes;
  do {
    num_bytes = read(fd, buf, nbyte);
    if ((num_bytes < 0) && (errno == EINTR))
      continue;
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
 * Changes a blocking file descriptor to a non-blocking one.
 */
void Block2Nonblock(int filedes) {
  int flags = fcntl(filedes, F_GETFL);
  assert(flags != -1);
  int retval = fcntl(filedes, F_SETFL, flags | O_NONBLOCK);
  assert(retval != -1);
}


/**
 * Drops the characters of string to a socket.  It doesn't matter
 * if the other side has hung up.
 */
void SendMsg2Socket(const int fd, const string &msg) {
  (void)send(fd, &msg[0], msg.length(), MSG_NOSIGNAL);
}


void LockMutex(pthread_mutex_t *mutex) {
  int retval = pthread_mutex_lock(mutex);
  assert(retval == 0);
}


void UnlockMutex(pthread_mutex_t *mutex) {
  int retval = pthread_mutex_unlock(mutex);
  assert(retval == 0);
}


/**
 * set(e){g/u}id wrapper.
 */
bool SwitchCredentials(const uid_t uid, const gid_t gid,
                       const bool temporarily)
{
  LogCvmfs(kLogCvmfs, kLogDebug, "current credentials uid %d gid %d "
           "euid %d egid %d, switching to %d %d (temp: %d)",
           getuid(), getgid(), geteuid(), getegid(), uid, gid, temporarily);
  int retval = 0;
  if (temporarily) {
    if (gid != getegid())
      retval = setegid(gid);
    if ((retval == 0) && (uid != geteuid()))
      retval = seteuid(uid);
  } else {
    // If effective uid is not root, we must first gain root access back
    if ((getuid() == 0) && (getuid() != geteuid())) {
      retval = SwitchCredentials(0, getgid(), true);
      if (!retval)
        return false;
    }
    retval = setgid(gid) || setuid(uid);
  }
  LogCvmfs(kLogCvmfs, kLogDebug, "switch credentials result %d (%d)",
           retval, errno);
  return retval == 0;
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
 * Checks if the symlink file path exists.
 */
bool SymlinkExists(const string &path) {
  platform_stat64 info;
  return ((platform_lstat(path.c_str(), &info) == 0) &&
          S_ISLNK(info.st_mode));
}


/**
 * Equivalent of `ln -sf $src $dest`
 */
bool SymlinkForced(const std::string &src, const std::string &dest) {
  int retval = unlink(dest.c_str());
  if ((retval != 0) && (errno != ENOENT))
    return false;
  retval = symlink(src.c_str(), dest.c_str());
  return retval == 0;
}


/**
 * The mkdir -p command.  Additionally checks if the directory is writable
 * if it exists.
 */
bool MkdirDeep(
  const std::string &path,
  const mode_t mode,
  bool verify_writable)
{
  if (path == "") return false;

  int retval = mkdir(path.c_str(), mode);
  if (retval == 0) return true;

  if ((errno == ENOENT) &&
      (MkdirDeep(GetParentPath(path), mode, verify_writable)))
  {
    return MkdirDeep(path, mode, verify_writable);
  }

  if (errno == EEXIST) {
    platform_stat64 info;
    if ((platform_stat(path.c_str(), &info) == 0) && S_ISDIR(info.st_mode)) {
      if (verify_writable) {
        retval = utimes(path.c_str(), NULL);
        if (retval == 0)
          return true;
      } else {
        return true;
      }
    }
  }

  return false;
}


/**
 * Creates the "hash cache" directory structure in path.
 */
bool MakeCacheDirectories(const string &path, const mode_t mode) {
  const string canonical_path = MakeCanonicalPath(path);

  string this_path = canonical_path + "/quarantaine";
  if (!MkdirDeep(this_path, mode, false)) return false;

  this_path = canonical_path + "/ff";

  platform_stat64 stat_info;
  if (platform_stat(this_path.c_str(), &stat_info) != 0) {
    this_path = canonical_path + "/txn";
    if (!MkdirDeep(this_path, mode, false))
      return false;
    for (int i = 0; i <= 0xff; i++) {
      char hex[3];
      snprintf(hex, sizeof(hex), "%02x", i);
      this_path = canonical_path + "/" + string(hex);
      if (!MkdirDeep(this_path, mode, false))
        return false;
    }
  }
  return true;
}


/**
 * Tries to locks file path, return an error if file is already locked.
 * Creates path if required.
 *
 * \return file descriptor, -1 on error, -2 if it would block
 */
int TryLockFile(const std::string &path) {
  const int fd_lockfile = open(path.c_str(), O_RDONLY | O_CREAT, 0600);
  if (fd_lockfile < 0)
    return -1;

  if (flock(fd_lockfile, LOCK_EX | LOCK_NB) != 0) {
    close(fd_lockfile);
    if (errno != EWOULDBLOCK)
      return -1;
    return -2;
  }

  return fd_lockfile;
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
    if (errno != EWOULDBLOCK) {
      close(fd_lockfile);
      return -1;
    }
    LogCvmfs(kLogCvmfs, kLogSyslog, "another process holds %s, waiting.",
             path.c_str());
    if (flock(fd_lockfile, LOCK_EX) != 0) {
      close(fd_lockfile);
      return -1;
    }
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
  if (tmp_fd < 0) {
    return NULL;
  }
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


/**
 * Create a directory with a unique name.
 */
string CreateTempDir(const std::string &path_prefix) {
  string dir = path_prefix + ".XXXXXX";
  char *tmp_dir = strdupa(dir.c_str());
  tmp_dir = mkdtemp(tmp_dir);
  if (tmp_dir == NULL)
    return "";
  return string(tmp_dir);
}


/**
 * Get the current working directory of the running process
 */
std::string GetCurrentWorkingDirectory() {
  char cwd[PATH_MAX];
  return (getcwd(cwd, sizeof(cwd)) != NULL) ? std::string(cwd) : std::string();
}


/**
 * Helper class that provides callback funtions for the file system traversal.
 */
class RemoveTreeHelper {
 public:
  bool success;
  RemoveTreeHelper() {
    success = true;
  }
  void RemoveFile(const string &parent_path, const string &name) {
    int retval = unlink((parent_path + "/" + name).c_str());
    if (retval != 0)
      success = false;
  }
  void RemoveDir(const string &parent_path, const string &name) {
    int retval = rmdir((parent_path + "/" + name).c_str());
    if (retval != 0)
      success = false;
  }
};


/**
 * Does rm -rf on path.
 */
bool RemoveTree(const string &path) {
  platform_stat64 info;
  int retval = platform_lstat(path.c_str(), &info);
  if (retval != 0)
    return errno == ENOENT;
  if (!S_ISDIR(info.st_mode))
    return false;

  RemoveTreeHelper *remove_tree_helper = new RemoveTreeHelper();
  FileSystemTraversal<RemoveTreeHelper> traversal(remove_tree_helper, "",
                                                  true);
  traversal.fn_new_file = &RemoveTreeHelper::RemoveFile;
  traversal.fn_new_symlink = &RemoveTreeHelper::RemoveFile;
  traversal.fn_new_socket = &RemoveTreeHelper::RemoveFile;
  traversal.fn_leave_dir = &RemoveTreeHelper::RemoveDir;
  traversal.Recurse(path);
  bool result = remove_tree_helper->success;
  delete remove_tree_helper;

  return result;
}


/**
 * Returns ls $dir/GLOB$suffix
 */
vector<string> FindFiles(const string &dir, const string &suffix) {
  vector<string> result;
  DIR *dirp = opendir(dir.c_str());
  if (!dirp)
    return result;

  platform_dirent64 *dirent;
  while ((dirent = platform_readdir(dirp))) {
    const string name(dirent->d_name);
    if ((name.length() >= suffix.length()) &&
        (name.substr(name.length()-suffix.length()) == suffix))
    {
      result.push_back(dir + "/" + name);
    }
  }
  closedir(dirp);
  sort(result.begin(), result.end());
  return result;
}


/**
 * Name -> UID from passwd database
 */
bool GetUidOf(const std::string &username, uid_t *uid, gid_t *main_gid) {
  char buf[16*1024];
  struct passwd pwd;
  struct passwd *result = NULL;
  getpwnam_r(username.c_str(), &pwd, buf, sizeof(buf), &result);
  if (result == NULL)
    return false;
  *uid = result->pw_uid;
  *main_gid = result->pw_gid;
  return true;
}


/**
 * Name -> GID from groups database
 */
bool GetGidOf(const std::string &groupname, gid_t *gid) {
  char buf[16*1024];
  struct group grp;
  struct group *result;
  getgrnam_r(groupname.c_str(), &grp, buf, sizeof(buf), &result);
  if (result == NULL)
    return false;
  *gid = result->gr_gid;
  return true;
}

/**
 * read the current umask of this process
 * Note: umask query is guarded by a global mutex. Hence, always use
 *       this function and beware of scalability bottlenecks
 */
mode_t GetUmask() {
  LockMutex(&getumask_mutex);
  const mode_t my_umask = umask(0);
  umask(my_umask);
  UnlockMutex(&getumask_mutex);
  return my_umask;
}


/**
 * Adds gid to the list of supplementary groups
 */
bool AddGroup2Persona(const gid_t gid) {
  int ngroups = getgroups(0, NULL);
  if (ngroups < 0)
    return false;
  gid_t *groups = static_cast<gid_t *>(smalloc((ngroups+1) * sizeof(gid_t)));
  int retval = getgroups(ngroups, groups);
  if (retval < 0) {
    free(groups);
    return false;
  }
  for (int i = 0; i < ngroups; ++i) {
    if (groups[i] == gid) {
      free(groups);
      return true;
    }
  }
  groups[ngroups] = gid;
  retval = setgroups(ngroups+1, groups);
  free(groups);
  return retval == 0;
}


string StringifyBool(const bool value) {
  return value ? "yes" : "no";
}


string StringifyInt(const int64_t value) {
  char buffer[48];
  snprintf(buffer, sizeof(buffer), "%"PRId64, value);
  return string(buffer);
}


string StringifyByteAsHex(const unsigned char value) {
  char buffer[3];
  snprintf(buffer, sizeof(buffer), "%02x", value);
  return string(buffer);
}


string StringifyDouble(const double value) {
  char buffer[64];
  snprintf(buffer, sizeof(buffer), "%.03f", value);
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
  char buffer[21];
  snprintf(buffer, sizeof(buffer), "%d %s %d %02d:%02d:%02d", timestamp.tm_mday,
           months[timestamp.tm_mon], timestamp.tm_year + 1900,
           timestamp.tm_hour, timestamp.tm_min, timestamp.tm_sec);

  return string(buffer);
}


/**
 * Current time in format Wed, 01 Mar 2006 12:00:00 GMT
 */
std::string RfcTimestamp() {
  const char *months[] = {"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul",
    "Aug", "Sep", "Oct", "Nov", "Dec"};
  const char *day_of_week[] =
    {"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"};

  struct tm timestamp;
  time_t now = time(NULL);
  gmtime_r(&now, &timestamp);

  char buffer[30];
  snprintf(buffer, sizeof(buffer), "%s, %02d %s %d %02d:%02d:%02d %s",
    day_of_week[timestamp.tm_wday], timestamp.tm_mday,
    months[timestamp.tm_mon], timestamp.tm_year + 1900,
    timestamp.tm_hour, timestamp.tm_min, timestamp.tm_sec,
    timestamp.tm_zone);
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


/**
 * Parses a timstamp of the form YYYY-MM-DDTHH:MM:SSZ
 * Return 0 on error
 */
time_t IsoTimestamp2UtcTime(const std::string &iso8601) {
  time_t utc_time = 0;
  unsigned length = iso8601.length();

  if (length != 20)
    return utc_time;
  if ((iso8601[4] != '-') || (iso8601[7] != '-') || (iso8601[10] != 'T') ||
      (iso8601[13] != ':') || (iso8601[16] != ':') || (iso8601[19] != 'Z'))
  {
    return utc_time;
  }

  struct tm tm_wl;
  memset(&tm_wl, 0, sizeof(struct tm));
  tm_wl.tm_year = String2Int64(iso8601.substr(0, 4))-1900;
  tm_wl.tm_mon = String2Int64(iso8601.substr(5, 2)) - 1;
  tm_wl.tm_mday = String2Int64(iso8601.substr(8, 2));
  tm_wl.tm_hour = String2Int64(iso8601.substr(11, 2));
  tm_wl.tm_min = String2Int64(iso8601.substr(14, 2));
  tm_wl.tm_sec = String2Int64(iso8601.substr(17, 2));
  utc_time = timegm(&tm_wl);
  if (utc_time < 0)
    return 0;

  return utc_time;
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


/**
 * Parse a string into a a uint64_t.
 *
 * Unlike String2Uint64, this:
 *   - Checks to make sure the full string is parsed
 *   - Can indicate an error occurred.
 *
 * If an error occurs, this returns false and sets errno appropriately.
 */
bool String2Uint64Parse(const std::string &value, uint64_t *result) {
  char *endptr = NULL;
  errno = 0;
  long long myval = strtoll(value.c_str(), &endptr, 10);  // NOLINT
  if ((value.size() == 0) || (endptr != (value.c_str() + value.size())) ||
      (myval < 0))
  {
    errno = EINVAL;
    return false;
  }
  if (errno) {
    return false;
  }
  if (result) {
    *result = myval;
  }
  return true;
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


bool HasSuffix(
  const std::string &str,
  const std::string &suffix,
  const bool ignore_case)
{
  if (suffix.size() > str.size()) return false;
  const IgnoreCaseComperator icmp;
  return (ignore_case)
    ? std::equal(suffix.rbegin(), suffix.rend(), str.rbegin(), icmp)
    : std::equal(suffix.rbegin(), suffix.rend(), str.rbegin());
}


vector<string> SplitString(const string &str,
                           const char delim,
                           const unsigned max_chunks) {
  vector<string> result;

  // edge case... one chunk is always the whole string
  if (1 == max_chunks) {
    result.push_back(str);
    return result;
  }

  // split the string
  const unsigned size = str.size();
  unsigned marker = 0;
  unsigned chunks = 1;
  unsigned i;
  for (i = 0; i < size; ++i) {
    if (str[i] == delim) {
      result.push_back(str.substr(marker, i-marker));
      marker = i+1;

      // we got what we want... good bye
      if (++chunks == max_chunks)
        break;
    }
  }

  // push the remainings of the string and return
  result.push_back(str.substr(marker));
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


void ParseKeyvalMem(const unsigned char *buffer, const unsigned buffer_size,
                    map<char, string> *content)
{
  string line;
  unsigned pos = 0;
  while (pos < buffer_size) {
    if (static_cast<char>(buffer[pos]) == '\n') {
      if (line == "--")
        return;

      if (line != "") {
        const string tail = (line.length() == 1) ? "" : line.substr(1);
        // Special handling of 'Z' key because it can exist multiple times
        if (line[0] != 'Z') {
          (*content)[line[0]] = tail;
        } else {
          if (content->find(line[0]) == content->end()) {
            (*content)[line[0]] = tail;
          } else {
            (*content)[line[0]] = (*content)[line[0]] + "|" + tail;
          }
        }
      }
      line = "";
    } else {
      line += static_cast<char>(buffer[pos]);
    }
    pos++;
  }
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

  ParseKeyvalMem(buffer, unsigned(num_bytes), content);
  return true;
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


string GetLineMem(const char *text, const int text_size) {
  int pos = 0;
  while ((pos < text_size) && (text[pos] != '\n'))
    pos++;
  return string(text, pos);
}


bool GetLineFile(FILE *f, std::string *line) {
  int retval;
  line->clear();
  while (true) {
    retval = fgetc(f);
    if (ferror(f) && (errno == EINTR)) {
      clearerr(f);
      continue;
    } else if (retval == EOF) {break;}
    char c = retval;
    if (c == '\n')
      break;
    line->push_back(c);
  }
  return (retval != EOF) || !line->empty();
}


bool GetLineFd(const int fd, std::string *line) {
  int retval;
  char c;
  line->clear();
  while (true) {
    retval = read(fd, &c, 1);
    if (retval == 0) {break;}
    if ((retval == -1) && (errno == EINTR)) {continue;}
    if (retval == -1) {break;}
    if (c == '\n')
      break;
    line->push_back(c);
  }
  return (retval == 1) || !line->empty();
}


/**
 * Removes leading and trailing whitespaces.
 */
string Trim(const string &raw) {
  if (raw.empty())
    return "";

  unsigned start_pos = 0;
  for (; (start_pos < raw.length()) &&
         (raw[start_pos] == ' ' || raw[start_pos] == '\t');
         ++start_pos) { }
  unsigned end_pos = raw.length()-1;  // at least one character in raw
  for (; (end_pos >= start_pos) &&
         (raw[end_pos] == ' ' || raw[end_pos] == '\t');
         --end_pos) { }

  return raw.substr(start_pos, end_pos-start_pos + 1);
}


/**
 * Converts all characters to upper case
 */
string ToUpper(const string &mixed_case) {
  string result(mixed_case);
  for (unsigned i = 0, l = result.length(); i < l; ++i) {
    result[i] = toupper(result[i]);
  }
  return result;
}


string ReplaceAll(const string &haystack, const string &needle,
                  const string &replace_by)
{
  string result(haystack);
  size_t pos = 0;
  const unsigned needle_size = needle.size();
  if (needle == "")
    return result;

  while ((pos = result.find(needle, pos)) != string::npos)
    result.replace(pos, needle_size, replace_by);
  return result;
}


/**
 * Blocks a signal for the calling thread.
 */
void BlockSignal(int signum) {
  sigset_t sigset;
  int retval = sigemptyset(&sigset);
  assert(retval == 0);
  retval = sigaddset(&sigset, signum);
  assert(retval == 0);
  retval = pthread_sigmask(SIG_BLOCK, &sigset, NULL);
  assert(retval == 0);
}


/**
 * Waits for a signal.  The signal should be blocked before for all threads.
 * Threads inherit their parent's signal mask.
 */
void WaitForSignal(int signum) {
  int retval;
  do {
    retval = platform_sigwait(signum);
  } while ((retval != signum) && (errno == EINTR));
  assert(retval == signum);
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


bool ExecuteBinary(
  int *fd_stdin,
  int *fd_stdout,
  int *fd_stderr,
  const std::string &binary_path,
  const std::vector<std::string> &argv,
  const bool double_fork,
  pid_t *child_pid
) {
  int pipe_stdin[2];
  int pipe_stdout[2];
  int pipe_stderr[2];
  MakePipe(pipe_stdin);
  MakePipe(pipe_stdout);
  MakePipe(pipe_stderr);

  set<int> preserve_fildes;
  preserve_fildes.insert(0);
  preserve_fildes.insert(1);
  preserve_fildes.insert(2);
  map<int, int> map_fildes;
  map_fildes[pipe_stdin[0]] = 0;  // Reading end of pipe_stdin
  map_fildes[pipe_stdout[1]] = 1;  // Writing end of pipe_stdout
  map_fildes[pipe_stderr[1]] = 2;  // Writing end of pipe_stderr
  vector<string> cmd_line;
  cmd_line.push_back(binary_path);
  cmd_line.insert(cmd_line.end(), argv.begin(), argv.end());

  if (!ManagedExec(cmd_line,
                   preserve_fildes,
                   map_fildes,
                   true,
                   double_fork,
                   child_pid))
  {
    ClosePipe(pipe_stdin);
    ClosePipe(pipe_stdout);
    ClosePipe(pipe_stderr);
    return false;
  }

  close(pipe_stdin[0]);
  close(pipe_stdout[1]);
  close(pipe_stderr[1]);
  *fd_stdin = pipe_stdin[1];
  *fd_stdout = pipe_stdout[0];
  *fd_stderr = pipe_stderr[0];
  return true;
}


/**
 * Opens /bin/sh and provides file descriptors to write into stdin and
 * read from stdout.  Quit shell simply by closing stderr, stdout, and stdin.
 */
bool Shell(int *fd_stdin, int *fd_stdout, int *fd_stderr) {
  const bool double_fork = true;
  return ExecuteBinary(fd_stdin, fd_stdout, fd_stderr, "/bin/sh",
                       vector<string>(), double_fork);
}

struct ForkFailures {  // TODO(rmeusel): C++11 (type safe enum)
  enum Names {
    kSendPid,
    kUnknown,
    kFailDupFd,
    kFailGetMaxFd,
    kFailGetFdFlags,
    kFailSetFdFlags,
    kFailDropCredentials,
    kFailExec,
  };

  static std::string ToString(const Names name) {
    switch (name) {
      case kSendPid:
        return "Sending PID";

      default:
      case kUnknown:
        return "Unknown Status";
      case kFailDupFd:
        return "Duplicate File Descriptor";
      case kFailGetMaxFd:
        return "Read maximal File Descriptor";
      case kFailGetFdFlags:
        return "Read File Descriptor Flags";
      case kFailSetFdFlags:
        return "Set File Descriptor Flags";
      case kFailDropCredentials:
        return "Lower User Permissions";
      case kFailExec:
        return "Invoking execvp()";
    }
  }
};

/**
 * Execve to the given command line, preserving the given file descriptors.
 * If stdin, stdout, stderr should be preserved, add 0, 1, 2.
 * File descriptors from the parent process can also be mapped to the new
 * process (dup2) using map_fildes.  Can be useful for
 * stdout/in/err redirection.
 * NOTE: The destination fildes have to be preserved!
 * Does a double fork to detach child.
 * The command_line parameter contains the binary at index 0 and the arguments
 * in the rest of the vector.
 * Using the optional parameter *pid it is possible to retrieve the process ID
 * of the spawned process.
 */
bool ManagedExec(const vector<string>  &command_line,
                 const set<int>        &preserve_fildes,
                 const map<int, int>   &map_fildes,
                 const bool             drop_credentials,
                 const bool             double_fork,
                       pid_t           *child_pid)
{
  assert(command_line.size() >= 1);

  Pipe pipe_fork;
  pid_t pid = fork();
  assert(pid >= 0);
  if (pid == 0) {
    pid_t pid_grand_child;
    int max_fd;
    int fd_flags;
    ForkFailures::Names failed = ForkFailures::kUnknown;

    const char *argv[command_line.size() + 1];
    for (unsigned i = 0; i < command_line.size(); ++i)
      argv[i] = command_line[i].c_str();
    argv[command_line.size()] = NULL;

    // Child, map file descriptors
    for (map<int, int>::const_iterator i = map_fildes.begin(),
         iEnd = map_fildes.end(); i != iEnd; ++i)
    {
      int retval = dup2(i->first, i->second);
      if (retval == -1) {
        failed = ForkFailures::kFailDupFd;
        goto fork_failure;
      }
    }

    // Child, close file descriptors
    max_fd = sysconf(_SC_OPEN_MAX);
    if (max_fd < 0) {
      failed = ForkFailures::kFailGetMaxFd;
      goto fork_failure;
    }
    for (int fd = 0; fd < max_fd; fd++) {
      if ((fd != pipe_fork.write_end) && (preserve_fildes.count(fd) == 0)) {
        close(fd);
      }
    }

    // Double fork to disconnect from parent
    if (double_fork) {
      pid_grand_child = fork();
      assert(pid_grand_child >= 0);
      if (pid_grand_child != 0) _exit(0);
    }

    fd_flags = fcntl(pipe_fork.write_end, F_GETFD);
    if (fd_flags < 0) {
      failed = ForkFailures::kFailGetFdFlags;
      goto fork_failure;
    }
    fd_flags |= FD_CLOEXEC;
    if (fcntl(pipe_fork.write_end, F_SETFD, fd_flags) < 0) {
      failed = ForkFailures::kFailSetFdFlags;
      goto fork_failure;
    }

#ifdef DEBUGMSG
    assert(setenv("__CVMFS_DEBUG_MODE__", "yes", 1) == 0);
#endif
    if (drop_credentials && !SwitchCredentials(geteuid(), getegid(), false)) {
      failed = ForkFailures::kFailDropCredentials;
      goto fork_failure;
    }

    // retrieve the PID of the new (grand) child process and send it to the
    // grand father
    pid_grand_child = getpid();
    pipe_fork.Write(ForkFailures::kSendPid);
    pipe_fork.Write(pid_grand_child);

    execvp(command_line[0].c_str(), const_cast<char **>(argv));

    failed = ForkFailures::kFailExec;

   fork_failure:
    pipe_fork.Write(failed);
    _exit(1);
  }
  if (double_fork) {
    int statloc;
    waitpid(pid, &statloc, 0);
  }

  close(pipe_fork.write_end);

  // Either the PID or a return value is sent
  ForkFailures::Names status_code;
  bool retcode = pipe_fork.Read(&status_code);
  assert(retcode);
  if (status_code != ForkFailures::kSendPid) {
    close(pipe_fork.read_end);
    LogCvmfs(kLogCvmfs, kLogDebug, "managed execve failed (%s)",
             ForkFailures::ToString(status_code).c_str());
    return false;
  }

  // read the PID of the spawned process if requested
  // (the actual read needs to be done in any case!)
  pid_t buf_child_pid = 0;
  retcode = pipe_fork.Read(&buf_child_pid);
  assert(retcode);
  if (child_pid != NULL)
    *child_pid = buf_child_pid;
  close(pipe_fork.read_end);
  LogCvmfs(kLogCvmfs, kLogDebug, "execve'd %s (PID: %d)",
           command_line[0].c_str(),
           static_cast<int>(buf_child_pid));
  return true;
}


// -----------------------------------------------------------------------------


void StopWatch::Start() {
  assert(!running_);

  gettimeofday(&start_, NULL);
  running_ = true;
}


void StopWatch::Stop() {
  assert(running_);

  gettimeofday(&end_, NULL);
  running_ = false;
}


void StopWatch::Reset() {
  start_ = timeval();
  end_   = timeval();
  running_ = false;
}


double StopWatch::GetTime() const {
  assert(!running_);

  return DiffTimeSeconds(start_, end_);
}


/**
 * Sleeps using select.  This is without signals and doesn't interfere with
 * other uses of the ALRM signal.
 */
void SafeSleepMs(const unsigned ms) {
  struct timeval wait_for;
  wait_for.tv_sec = ms / 1000;
  wait_for.tv_usec = (ms % 1000) * 1000;
  select(0, NULL, NULL, NULL, &wait_for);
}


/**
 * Deal with EINTR and partial writes.
 */
bool SafeWrite(int fd, const void *buf, size_t nbyte) {
  while (nbyte) {
    ssize_t retval = write(fd, buf, nbyte);
    if (retval < 0) {
      if (errno == EINTR)
        continue;
      return false;
    }
    assert(static_cast<size_t>(retval) <= nbyte);
    buf = reinterpret_cast<const char *>(buf) + retval;
    nbyte -= retval;
  }
  return true;
}


/**
 * Deal with EINTR and partial reads.
 */
ssize_t SafeRead(int fd, void *buf, size_t nbyte) {
  ssize_t total_bytes = 0;
  while (nbyte) {
    ssize_t retval = read(fd, buf, nbyte);
    if (retval < 0) {
      if (errno == EINTR)
        continue;
      return -1;
    } else if (retval == 0) {
      return total_bytes;
    }
    assert(static_cast<size_t>(retval) <= nbyte);
    buf = reinterpret_cast<char *>(buf) + retval;
    nbyte -= retval;
    total_bytes += retval;
  }
  return total_bytes;
}


/**
 * Pull file contents into a string
 */
bool SafeReadToString(int fd, std::string *final_result) {
  if (!final_result) {return false;}

  std::string tmp_result;
  static const int buf_size = 4096;
  char buf[4096];
  ssize_t total_bytes = -1;
  do {
    total_bytes = SafeRead(fd, buf, buf_size);
    if (total_bytes < 0) {return false;}
    tmp_result.append(buf, total_bytes);
  } while (total_bytes == buf_size);
  final_result->swap(tmp_result);
  return true;
}


static inline void Base64Block(const unsigned char input[3], const char *table,
                               char output[4])
{
  output[0] = table[(input[0] & 0xFD) >> 2];
  output[1] = table[((input[0] & 0x03) << 4) | ((input[1] & 0xF0) >> 4)];
  output[2] = table[((input[1] & 0x0F) << 2) | ((input[2] & 0xD0) >> 6)];
  output[3] = table[input[2] & 0x3F];
}


string Base64(const string &data) {
  string result;
  result.reserve((data.length()+3)*4/3);
  unsigned pos = 0;
  const unsigned char *data_ptr =
    reinterpret_cast<const unsigned char *>(data.data());
  const unsigned length = data.length();
  while (pos+2 < length) {
    char encoded_block[4];
    Base64Block(data_ptr+pos, b64_table, encoded_block);
    result.append(encoded_block, 4);
    pos += 3;
  }
  if (length % 3 != 0) {
    unsigned char input[3];
    input[0] = data_ptr[pos];
    input[1] = ((length % 3) == 2) ? data_ptr[pos+1] : 0;
    input[2] = 0;
    char encoded_block[4];
    Base64Block(input, b64_table, encoded_block);
    result.append(encoded_block, 2);
    result.push_back(((length % 3) == 2) ? encoded_block[2] : '=');
    result.push_back('=');
  }

  return result;
}


static bool Debase64Block(const unsigned char input[4],
                          const signed char *d_table,
                          unsigned char output[3])
{
  int32_t dec[4];
  for (int i = 0; i < 4; ++i) {
    dec[i] = db64_table[input[i]];
    if (dec[i] < 0) return false;
  }

  output[0] = (dec[0] << 2) | (dec[1] >> 4);
  output[1] = ((dec[1] & 0x0F) << 4) | (dec[2] >> 2);
  output[2] = ((dec[2] & 0x03) << 6) | dec[3];
  return true;
}


bool Debase64(const string &data, string *decoded) {
  decoded->clear();
  decoded->reserve((data.length()+4)*3/4);
  unsigned pos = 0;
  const unsigned char *data_ptr =
    reinterpret_cast<const unsigned char *>(data.data());
  const unsigned length = data.length();
  if (length == 0) return true;
  if ((length % 4) != 0)
    return false;

  while (pos < length) {
    unsigned char decoded_block[3];
    bool retval = Debase64Block(data_ptr+pos, db64_table, decoded_block);
    if (!retval)
      return false;
    decoded->append(reinterpret_cast<char *>(decoded_block), 3);
    pos += 4;
  }

  for (int i = 0; i < 2; ++i) {
    pos--;
    if (data[pos] == '=')
      decoded->erase(decoded->length()-1);
  }
  return true;
}


MemoryMappedFile::MemoryMappedFile(const std::string &file_path) :
  file_path_(file_path),
  file_descriptor_(-1),
  mapped_file_(NULL),
  mapped_size_(0),
  mapped_(false) {}

MemoryMappedFile::~MemoryMappedFile() {
  if (IsMapped()) {
    Unmap();
  }
}

bool MemoryMappedFile::Map() {
  assert(!mapped_);

  // open the file
  int fd;
  if ((fd = open(file_path_.c_str(), O_RDONLY, 0)) == -1) {
    LogCvmfs(kLogUtility, kLogStderr, "failed to open %s (%d)",
             file_path_.c_str(), errno);
    return false;
  }

  // get file size
  platform_stat64 filesize;
  if (platform_fstat(fd, &filesize) != 0) {
    LogCvmfs(kLogUtility, kLogStderr, "failed to fstat %s (%d)",
             file_path_.c_str(), errno);
    close(fd);
    return false;
  }

  // check if the file is empty and 'pretend' that the file is mapped
  // --> buffer will then look like a buffer without any size...
  void *mapping = NULL;
  if (filesize.st_size > 0) {
    // map the given file into memory
    mapping = mmap(NULL, filesize.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (mapping == MAP_FAILED) {
      LogCvmfs(kLogUtility, kLogStderr, "failed to mmap %s (file size: %d) "
                                        "(errno: %d)",
                file_path_.c_str(),
                filesize.st_size,
                errno);
      close(fd);
      return false;
    }
  }

  // save results
  mapped_file_     = static_cast<unsigned char*>(mapping);
  file_descriptor_ = fd;
  mapped_size_     = filesize.st_size;
  mapped_          = true;
  LogCvmfs(kLogUtility, kLogVerboseMsg, "mmap'ed %s", file_path_.c_str());
  return true;
}

void MemoryMappedFile::Unmap() {
  assert(mapped_);

  if (mapped_file_ == NULL) {
    return;
  }

  // unmap the previously mapped file
  if ((munmap(static_cast<void*>(mapped_file_), mapped_size_) != 0) ||
      (close(file_descriptor_) != 0))
  {
    LogCvmfs(kLogUtility, kLogStderr, "failed to unmap %s", file_path_.c_str());
    const bool munmap_failed = false;
    assert(munmap_failed);
  }

  // reset (resettable) data
  mapped_file_     = NULL;
  file_descriptor_ = -1;
  mapped_size_     = 0;
  mapped_          = false;
  LogCvmfs(kLogUtility, kLogVerboseMsg, "munmap'ed %s", file_path_.c_str());
}

#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif
