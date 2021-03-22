/**
 * This file is part of the CernVM File System.
 *
 * Some common functions.
 */

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include "cvmfs_config.h"
#include "posix.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <inttypes.h>
#include <netinet/in.h>
#include <pthread.h>
#include <pwd.h>
#include <signal.h>
#include <stdint.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/stat.h>
#ifdef __APPLE__
#include <sys/mount.h>  //  for statfs()
#else
#include <sys/statfs.h>
#endif
#include <sys/time.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <unistd.h>
// If valgrind headers are present on the build system, then we can detect
// valgrind at runtime.
#ifdef HAS_VALGRIND_HEADERS
#include <valgrind/valgrind.h>
#endif

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <cstring>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "fs_traversal.h"
#include "logging.h"
#include "platform.h"
#include "util/algorithm.h"
#include "util/exception.h"
#include "util/string.h"
#include "util_concurrency.h"

//using namespace std;  // NOLINT

#ifndef ST_RDONLY
// On Linux, this is in sys/statvfs.h
// On macOS, this flag is called MNT_RDONLY /usr/include/sys/mount.h
#define ST_RDONLY 1
#endif

// Older Linux glibc versions do not provide the f_flags member in struct statfs
#define CVMFS_HAS_STATFS_F_FLAGS
#ifndef __APPLE__
#ifdef __GLIBC_MINOR__
#if __GLIBC_MINOR__ < 12
#undef CVMFS_HAS_STATFS_F_FLAGS
#endif
#endif
#endif

// Work around missing clearenv()
#ifdef __APPLE__
extern "C" {
extern char **environ;
}
#endif

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

static pthread_mutex_t getumask_mutex = PTHREAD_MUTEX_INITIALIZER;


/**
 * Removes a trailing "/" from a path.
 */
std::string MakeCanonicalPath(const std::string &path) {
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
std::string GetParentPath(const std::string &path) {
  const std::string::size_type idx = path.find_last_of('/');
  if (idx != std::string::npos)
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
std::string GetFileName(const std::string &path) {
  const std::string::size_type idx = path.find_last_of('/');
  if (idx != std::string::npos)
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


std::string GetAbsolutePath(const std::string &path) {
  if (IsAbsolutePath(path))
    return path;

  return GetCurrentWorkingDirectory() + "/" + path;
}


bool IsHttpUrl(const std::string &path) {
  if (path.length() < 7) {
    return false;
  }

  std::string prefix = path.substr(0, 7);
  std::transform(prefix.begin(), prefix.end(), prefix.begin(), ::tolower);

  return prefix == "http://";
}


FileSystemInfo GetFileSystemInfo(const std::string &path) {
  FileSystemInfo result;

  struct statfs info;
  int retval = statfs(path.c_str(), &info);
  if (retval != 0)
    return result;

  switch (info.f_type) {
    case kFsTypeAutofs:
      result.type = kFsTypeAutofs;
      break;
    case kFsTypeNFS:
      result.type = kFsTypeNFS;
      break;
    case kFsTypeProc:
      result.type = kFsTypeProc;
      break;
    case kFsTypeBeeGFS:
      result.type = kFsTypeBeeGFS;
      break;
    default:
      result.type = kFsTypeUnknown;
  }

#ifdef CVMFS_HAS_STATFS_F_FLAGS
  if (info.f_flags & ST_RDONLY)
    result.is_rdonly = true;
#else
  // On old Linux systems, fall back to access()
  retval = access(path.c_str(), W_OK);
  result.is_rdonly = (retval != 0);
#endif



  return result;
}


std::string ReadSymlink(const std::string &path) {
  // TODO(jblomer): avoid PATH_MAX
  char buf[PATH_MAX + 1];
  ssize_t nchars = readlink(path.c_str(), buf, PATH_MAX);
  if (nchars >= 0) {
    buf[nchars] = '\0';
    return std::string(buf);
  }
  return "";
}


/**
 * Follow all symlinks if possible. Equivalent to
 * `readlink --canonicalize-missing`
 */
std::string ResolvePath(const std::string &path) {
  if (path.empty() || (path == "/"))
    return "/";
  std::string name = GetFileName(path);
  std::string result = name;
  if (name != path) {
    // There is a parent path of 'path'
    std::string parent = ResolvePath(GetParentPath(path));
    result = parent + (parent == "/" ? "" : "/") + name;
  }
  char *real_result = realpath(result.c_str(), NULL);
  if (real_result) {
    result = real_result;
    free(real_result);
  }
  if (SymlinkExists(result)) {
    char buf[PATH_MAX + 1];
    ssize_t nchars = readlink(result.c_str(), buf, PATH_MAX);
    if (nchars >= 0) {
      buf[nchars] = '\0';
      result = buf;
    }
  }
  return result;
}


bool IsMountPoint(const std::string &path) {
  std::vector<std::string> mount_list = platform_mountlist();
  std::string resolved_path = ResolvePath(path);
  for (unsigned i = 0; i < mount_list.size(); ++i) {
    if (mount_list[i] == resolved_path)
      return true;
  }
  return false;
}


/**
 * By default PANIC(NULL) on failure
 */
void CreateFile(
  const std::string &path,
  const int mode,
  const bool ignore_failure)
{
  int fd = open(path.c_str(), O_CREAT, mode);
  if (fd >= 0) {
    close(fd);
    return;
  }
  if (ignore_failure)
    return;
  PANIC(NULL);
}


/**
 * Symlinks /tmp/cvmfs.XYZ/l --> ParentPath(path) to make it shorter
 */
static std::string MakeShortSocketLink(const std::string &path) {
  struct sockaddr_un sock_addr;
  unsigned max_length = sizeof(sock_addr.sun_path);

  std::string result;
  std::string tmp_path = CreateTempDir("/tmp/cvmfs");
  if (tmp_path.empty())
    return "";
  std::string link = tmp_path + "/l";
  result = link + "/" + GetFileName(path);
  if (result.length() >= max_length) {
    rmdir(tmp_path.c_str());
    return "";
  }
  int retval = symlink(GetParentPath(path).c_str(), link.c_str());
  if (retval != 0) {
    rmdir(tmp_path.c_str());
    return "";
  }
  return result;
}

static void RemoveShortSocketLink(const std::string &short_path) {
  std::string link = GetParentPath(short_path);
  unlink(link.c_str());
  rmdir(GetParentPath(link).c_str());
}


/**
 * Creates and binds to a named socket.
 */
int MakeSocket(const std::string &path, const int mode) {
  std::string short_path(path);
  struct sockaddr_un sock_addr;
  if (path.length() >= sizeof(sock_addr.sun_path)) {
    // Socket paths are limited to 108 bytes (on some systems to 92 bytes),
    // try working around
    short_path = MakeShortSocketLink(path);
    if (short_path.empty())
      return -1;
  }
  sock_addr.sun_family = AF_UNIX;
  strncpy(sock_addr.sun_path, short_path.c_str(),
          sizeof(sock_addr.sun_path));

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

  if (short_path != path)
    RemoveShortSocketLink(short_path);

  return socket_fd;

 make_socket_failure:
  close(socket_fd);
  if (short_path != path)
    RemoveShortSocketLink(short_path);
  return -1;
}


/**
 * Creates and binds a TCP/IPv4 socket.  An empty address binds to the "any"
 * address.
 */
int MakeTcpEndpoint(const std::string &ipv4_address, int portno) {
  const int socket_fd = socket(AF_INET, SOCK_STREAM, 0);
  assert(socket_fd != -1);
  const int on = 1;
  int retval = setsockopt(socket_fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
  assert(retval == 0);

  struct sockaddr_in endpoint_addr;
  memset(&endpoint_addr, 0, sizeof(endpoint_addr));
  endpoint_addr.sin_family = AF_INET;
  if (ipv4_address.empty()) {
    endpoint_addr.sin_addr.s_addr = INADDR_ANY;
  } else {
    retval = inet_aton(ipv4_address.c_str(), &(endpoint_addr.sin_addr));
    if (retval == 0) {
      LogCvmfs(kLogCvmfs, kLogDebug, "invalid IPv4 address");
      close(socket_fd);
      return -1;
    }
  }
  endpoint_addr.sin_port = htons(portno);

  retval = bind(socket_fd, (struct sockaddr *)&endpoint_addr,
                sizeof(endpoint_addr));
  if (retval < 0) {
    LogCvmfs(kLogCvmfs, kLogDebug, "binding TCP endpoint failed (%d)", errno);
    close(socket_fd);
    return -1;
  }
  return socket_fd;
}


/**
 * Connects to a named socket.
 *
 * \return socket file descriptor on success, -1 else
 */
int ConnectSocket(const std::string &path) {
  std::string short_path(path);
  struct sockaddr_un sock_addr;
  if (path.length() >= sizeof(sock_addr.sun_path)) {
    // Socket paths are limited to 108 bytes (on some systems to 92 bytes),
    // try working around
    short_path = MakeShortSocketLink(path);
    if (short_path.empty())
      return -1;
  }
  sock_addr.sun_family = AF_UNIX;
  strncpy(sock_addr.sun_path, short_path.c_str(), sizeof(sock_addr.sun_path));

  const int socket_fd = socket(AF_UNIX, SOCK_STREAM, 0);
  assert(socket_fd != -1);

  int retval =
    connect(socket_fd, (struct sockaddr *)&sock_addr,
            sizeof(sock_addr.sun_family) + sizeof(sock_addr.sun_path));
  if (short_path != path)
    RemoveShortSocketLink(short_path);

  if (retval < 0) {
    close(socket_fd);
    return -1;
  }

  return socket_fd;
}


/**
 * Connects to a (remote) TCP server
 */
int ConnectTcpEndpoint(const std::string &ipv4_address, int portno) {
  const int socket_fd = socket(AF_INET, SOCK_STREAM, 0);
  assert(socket_fd != -1);

  struct sockaddr_in endpoint_addr;
  memset(&endpoint_addr, 0, sizeof(endpoint_addr));
  endpoint_addr.sin_family = AF_INET;
  int retval = inet_aton(ipv4_address.c_str(), &(endpoint_addr.sin_addr));
  if (retval == 0) {
    LogCvmfs(kLogCvmfs, kLogDebug, "invalid IPv4 address");
    close(socket_fd);
    return -1;
  }
  endpoint_addr.sin_port = htons(portno);

  retval = connect(socket_fd, (struct sockaddr *)&endpoint_addr,
                   sizeof(endpoint_addr));
  if (retval != 0) {
    LogCvmfs(kLogCvmfs, kLogDebug, "failed to connect to TCP endpoint (%d)",
             errno);
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
  unsigned i = 0;
  unsigned backoff_ms = 1;
  const unsigned max_backoff_ms = 256;
  do {
    // When the writer is not connected, this takes ~200-300ns per call as per
    // micro benchmarks
    num_bytes = read(fd, buf, nbyte);
    if ((num_bytes < 0) && (errno == EINTR))
      continue;
    i++;
    // Start backing off when the busy loop reaches the ballpark of 1ms
    if ((i > 3000) && (num_bytes == 0)) {
      // The BackoffThrottle would pull in too many dependencies
      SafeSleepMs(backoff_ms);
      if (backoff_ms < max_backoff_ms) backoff_ms *= 2;
    }
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
 * Compares two directory trees on the meta-data level. Returns true iff the
 * trees have identical content.
 */
bool DiffTree(const std::string &path_a, const std::string &path_b) {
  int retval;
  std::vector<std::string> ls_a;
  std::vector<std::string> ls_b;
  std::vector<std::string> subdirs;

  DIR *dirp_a = opendir(path_a.c_str());
  if (dirp_a == NULL) return false;
  DIR *dirp_b = opendir(path_b.c_str());
  if (dirp_b == NULL) {
    closedir(dirp_a);
    return false;
  }

  platform_dirent64 *dirent;
  while ((dirent = platform_readdir(dirp_a))) {
    const std::string name(dirent->d_name);
    if ((name == ".") || (name == ".."))
      continue;
    const std::string path = path_a + "/" + name;
    ls_a.push_back(path);

    platform_stat64 info;
    retval = platform_lstat(path.c_str(), &info);
    if (retval != 0) {
      closedir(dirp_a);
      closedir(dirp_b);
      return false;
    }
    if (S_ISDIR(info.st_mode)) subdirs.push_back(name);
  }
  while ((dirent = platform_readdir(dirp_b))) {
    const std::string name(dirent->d_name);
    if ((name == ".") || (name == ".."))
      continue;
    const std::string path = path_b + "/" + name;
    ls_b.push_back(path);
  }
  closedir(dirp_a);
  closedir(dirp_b);

  sort(ls_a.begin(), ls_a.end());
  sort(ls_b.begin(), ls_b.end());
  if (ls_a.size() != ls_b.size())
    return false;
  for (unsigned i = 0; i < ls_a.size(); ++i) {
    if (GetFileName(ls_a[i]) != GetFileName(ls_b[i])) return false;
    platform_stat64 info_a;
    platform_stat64 info_b;
    retval = platform_lstat(ls_a[i].c_str(), &info_a);
    if (retval != 0) return false;
    retval = platform_lstat(ls_b[i].c_str(), &info_b);
    if (retval != 0) return false;
    if ((info_a.st_mode != info_b.st_mode) ||
        (info_a.st_uid != info_b.st_uid) ||
        (info_a.st_gid != info_b.st_gid) ||
        (info_a.st_size != info_b.st_size))
    {
      return false;
    }
  }

  for (unsigned i = 0; i < subdirs.size(); ++i) {
    bool retval_subtree = DiffTree(path_a + "/" + subdirs[i],
                                   path_b + "/" + subdirs[i]);
    if (!retval_subtree) return false;
  }

  return true;
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
void SendMsg2Socket(const int fd, const std::string &msg) {
  (void)send(fd, &msg[0], msg.length(), MSG_NOSIGNAL);
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
bool FileExists(const std::string &path) {
  platform_stat64 info;
  return ((platform_lstat(path.c_str(), &info) == 0) &&
          S_ISREG(info.st_mode));
}


/**
 * Returns -1 on failure.
 */
int64_t GetFileSize(const std::string &path) {
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
bool SymlinkExists(const std::string &path) {
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
bool MakeCacheDirectories(const std::string &path, const mode_t mode) {
  const std::string canonical_path = MakeCanonicalPath(path);

  std::string this_path = canonical_path + "/quarantaine";
  if (!MkdirDeep(this_path, mode, false)) return false;

  this_path = canonical_path + "/ff";

  platform_stat64 stat_info;
  if (platform_stat(this_path.c_str(), &stat_info) != 0) {
    this_path = canonical_path + "/txn";
    if (!MkdirDeep(this_path, mode, false))
      return false;
    for (int i = 0; i <= 0xff; i++) {
      char hex[4];
      snprintf(hex, sizeof(hex), "%02x", i);
      this_path = canonical_path + "/" + std::string(hex);
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
 * Tries to write the process id in a /var/run/progname.pid like file.  Returns
 * the same as TryLockFile.
 *
 * \return file descriptor, -1 on error, -2 if it would block
 */
int WritePidFile(const std::string &path) {
  const int fd = open(path.c_str(), O_CREAT | O_RDWR, 0600);
  if (fd < 0)
    return -1;
  if (flock(fd, LOCK_EX | LOCK_NB) != 0) {
    close(fd);
    if (errno != EWOULDBLOCK)
      return -1;
    return -2;
  }

  // Don't leak the file descriptor to exec'd children
  int flags = fcntl(fd, F_GETFD);
  assert(flags != -1);
  flags |= FD_CLOEXEC;
  flags = fcntl(fd, F_SETFD, flags);
  assert(flags != -1);

  char buf[64];

  snprintf(buf, sizeof(buf), "%" PRId64 "\n", static_cast<uint64_t>(getpid()));
  bool retval =
    (ftruncate(fd, 0) == 0) && SafeWrite(fd, buf, strlen(buf));
  if (!retval) {
    UnlockFile(fd);
    return -1;
  }
  return fd;
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
FILE *CreateTempFile(const std::string &path_prefix, const int mode,
                     const char *open_flags, std::string *final_path)
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
std::string CreateTempPath(const std::string &path_prefix, const int mode) {
  std::string result;
  FILE *f = CreateTempFile(path_prefix, mode, "w", &result);
  if (!f)
    return "";
  fclose(f);
  return result;
}


/**
 * Create a directory with a unique name.
 */
std::string CreateTempDir(const std::string &path_prefix) {
  std::string dir = path_prefix + ".XXXXXX";
  char *tmp_dir = strdupa(dir.c_str());
  tmp_dir = mkdtemp(tmp_dir);
  if (tmp_dir == NULL)
    return "";
  return std::string(tmp_dir);
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
  void RemoveFile(const std::string &parent_path, const std::string &name) {
    int retval = unlink((parent_path + "/" + name).c_str());
    if (retval != 0)
      success = false;
  }
  void RemoveDir(const std::string &parent_path, const std::string &name) {
    int retval = rmdir((parent_path + "/" + name).c_str());
    if (retval != 0)
      success = false;
  }
  bool TryRemoveDir(const std::string &parent_path, const std::string &name) {
    int retval = rmdir((parent_path + "/" + name).c_str());
    return (retval != 0);
  }
};


/**
 * Does rm -rf on path.
 */
bool RemoveTree(const std::string &path) {
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
  traversal.fn_new_character_dev = &RemoveTreeHelper::RemoveFile;
  traversal.fn_new_symlink = &RemoveTreeHelper::RemoveFile;
  traversal.fn_new_socket = &RemoveTreeHelper::RemoveFile;
  traversal.fn_new_fifo = &RemoveTreeHelper::RemoveFile;
  traversal.fn_leave_dir = &RemoveTreeHelper::RemoveDir;
  traversal.fn_new_dir_prefix = &RemoveTreeHelper::TryRemoveDir;
  traversal.Recurse(path);
  bool result = remove_tree_helper->success;
  delete remove_tree_helper;

  return result;
}


/**
 * Returns ls $dir/GLOB$suffix
 */
std::vector<std::string> FindFilesBySuffix(
  const std::string &dir,
  const std::string &suffix)
{
  std::vector<std::string> result;
  DIR *dirp = opendir(dir.c_str());
  if (!dirp)
    return result;

  platform_dirent64 *dirent;
  while ((dirent = platform_readdir(dirp))) {
    const std::string name(dirent->d_name);
    if ((name.length() >= suffix.length()) &&
        (name.substr(name.length()-suffix.length()) == suffix))
    {
      result.push_back(dir + "/" + name);
    }
  }
  closedir(dirp);
  std::sort(result.begin(), result.end());
  return result;
}


/**
 * Returns ls $dir/$prefixGLOB
 */
std::vector<std::string> FindFilesByPrefix(
  const std::string &dir,
  const std::string &prefix)
{
  std::vector<std::string> result;
  DIR *dirp = opendir(dir.c_str());
  if (!dirp)
    return result;

  platform_dirent64 *dirent;
  while ((dirent = platform_readdir(dirp))) {
    const std::string name(dirent->d_name);
    if ((name.length() >= prefix.length()) &&
        (name.substr(0, prefix.length()) == prefix))
    {
      result.push_back(dir + "/" + name);
    }
  }
  closedir(dirp);
  std::sort(result.begin(), result.end());
  return result;
}


/**
 * Finds all direct subdirectories under parent_dir (except ., ..).  Used,
 * for instance, to parse /etc/cvmfs/repositories.d/<reponoame>
 */
std::vector<std::string> FindDirectories(const std::string &parent_dir) {
  std::vector<std::string> result;
  DIR *dirp = opendir(parent_dir.c_str());
  if (!dirp)
    return result;

  platform_dirent64 *dirent;
  while ((dirent = platform_readdir(dirp))) {
    const std::string name(dirent->d_name);
    if ((name == ".") || (name == ".."))
      continue;
    const std::string path = parent_dir + "/" + name;

    platform_stat64 info;
    int retval = platform_stat(path.c_str(), &info);
    if (retval != 0)
      continue;
    if (S_ISDIR(info.st_mode))
      result.push_back(path);
  }
  closedir(dirp);
  sort(result.begin(), result.end());
  return result;
}


/**
 * Finds all files and direct subdirectories under directory (except ., ..).
 */
bool ListDirectory(const std::string &directory,
                   std::vector<std::string> *names,
                   std::vector<mode_t> *modes)
{
  DIR *dirp = opendir(directory.c_str());
  if (!dirp)
    return false;

  platform_dirent64 *dirent;
  while ((dirent = platform_readdir(dirp))) {
    const std::string name(dirent->d_name);
    if ((name == ".") || (name == ".."))
      continue;
    const std::string path = directory + "/" + name;

    platform_stat64 info;
    int retval = platform_lstat(path.c_str(), &info);
    if (retval != 0) {
      closedir(dirp);
      return false;
    }

    names->push_back(name);
    modes->push_back(info.st_mode);
  }
  closedir(dirp);

  SortTeam(names, modes);
  return true;
}


/**
 * Looks whether exe is an executable file.  If exe is not an absolute path,
 * searches the PATH environment.
 */
std::string FindExecutable(const std::string &exe) {
  if (exe.empty())
    return "";

  std::vector<std::string> search_paths;
  if (exe[0] == '/') {
    search_paths.push_back(GetParentPath(exe));
  } else {
    char *path_env = getenv("PATH");
    if (path_env) {
      search_paths = SplitString(path_env, ':');
    }
  }

  for (unsigned i = 0; i < search_paths.size(); ++i) {
    if (search_paths[i].empty())
      continue;
    if (search_paths[i][0] != '/')
      continue;

    std::string path = search_paths[i] + "/" + GetFileName(exe);
    platform_stat64 info;
    int retval = platform_stat(path.c_str(), &info);
    if (retval != 0)
      continue;
    if (!S_ISREG(info.st_mode))
      continue;
    retval = access(path.c_str(), X_OK);
    if (retval != 0)
      continue;

    return path;
  }

  return "";
}


std::string GetUserName() {
  struct passwd pwd;
  struct passwd *result = NULL;
  int bufsize = 16 * 1024;
  char *buf = static_cast<char *>(smalloc(bufsize));
  while (getpwuid_r(geteuid(), &pwd, buf, bufsize, &result) == ERANGE) {
    bufsize *= 2;
    buf = static_cast<char *>(srealloc(buf, bufsize));
  }
  if (result == NULL) {
    free(buf);
    return "";
  }
  std::string user_name = pwd.pw_name;
  free(buf);
  return user_name;
}

std::string GetShell() {
  struct passwd pwd;
  struct passwd *result = NULL;
  int bufsize = 16 * 1024;
  char *buf = static_cast<char *>(smalloc(bufsize));
  while (getpwuid_r(geteuid(), &pwd, buf, bufsize, &result) == ERANGE) {
    bufsize *= 2;
    buf = static_cast<char *>(srealloc(buf, bufsize));
  }
  if (result == NULL) {
    free(buf);
    return "";
  }
  std::string shell = pwd.pw_shell;
  free(buf);
  return shell;
}

/**
 * UID -> Name from passwd database
 */
bool GetUserNameOf(uid_t uid, std::string *username) {
  struct passwd pwd;
  struct passwd *result = NULL;
  int bufsize = 16 * 1024;
  char *buf = static_cast<char *>(smalloc(bufsize));
  while (getpwuid_r(uid, &pwd, buf, bufsize, &result) == ERANGE) {
    bufsize *= 2;
    buf = static_cast<char *>(srealloc(buf, bufsize));
  }
  if (result == NULL) {
    free(buf);
    return false;
  }
  if (username)
    *username = result->pw_name;
  free(buf);
  return true;
}


/**
 * Name -> UID from passwd database
 */
bool GetUidOf(const std::string &username, uid_t *uid, gid_t *main_gid) {
  struct passwd pwd;
  struct passwd *result = NULL;
  int bufsize = 16 * 1024;
  char *buf = static_cast<char *>(smalloc(bufsize));
  while (getpwnam_r(username.c_str(), &pwd, buf, bufsize, &result) == ERANGE) {
    bufsize *= 2;
    buf = static_cast<char *>(srealloc(buf, bufsize));
  }
  if (result == NULL) {
    free(buf);
    return false;
  }
  *uid = result->pw_uid;
  *main_gid = result->pw_gid;
  free(buf);
  return true;
}


/**
 * Name -> GID from groups database
 */
bool GetGidOf(const std::string &groupname, gid_t *gid) {
  struct group grp;
  struct group *result = NULL;
  int bufsize = 16 * 1024;
  char *buf = static_cast<char *>(smalloc(bufsize));
  while (getgrnam_r(groupname.c_str(), &grp, buf, bufsize, &result) == ERANGE) {
    bufsize *= 2;
    buf = static_cast<char *>(srealloc(buf, bufsize));
  }
  if (result == NULL) {
    free(buf);
    return false;
  }
  *gid = result->gr_gid;
  free(buf);
  return true;
}

/**
 * read the current umask of this process
 * Note: umask query is guarded by a global mutex. Hence, always use
 *       this function and beware of scalability bottlenecks
 */
mode_t GetUmask() {
  MutexLockGuard m(&getumask_mutex);
  const mode_t my_umask = umask(0);
  umask(my_umask);
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


std::string GetHomeDirectory() {
  uid_t uid = getuid();
  struct passwd pwd;
  struct passwd *result = NULL;
  int bufsize = 16 * 1024;
  char *buf = static_cast<char *>(smalloc(bufsize));
  while (getpwuid_r(uid, &pwd, buf, bufsize, &result) == ERANGE) {
    bufsize *= 2;
    buf = static_cast<char *>(srealloc(buf, bufsize));
  }
  if (result == NULL) {
    free(buf);
    return "";
  }
  std::string home_dir = result->pw_dir;
  free(buf);
  return home_dir;
}


/**
 * Sets soft and hard limit for maximum number of open file descriptors.
 * Returns 0 on success, -1 on failure, -2 if running under valgrind.
 */
int SetLimitNoFile(unsigned limit_nofile) {
  struct rlimit rpl;
  memset(&rpl, 0, sizeof(rpl));
  getrlimit(RLIMIT_NOFILE, &rpl);
  if (rpl.rlim_max < limit_nofile)
    rpl.rlim_max = limit_nofile;
  rpl.rlim_cur = limit_nofile;
  int retval = setrlimit(RLIMIT_NOFILE, &rpl);
  if (retval == 0)
    return 0;

#ifdef HAS_VALGRIND_HEADERS
  return RUNNING_ON_VALGRIND ? -2 : -1;
#else
  return -1;
#endif
}


/**
 * Get the file descriptor limits
 */
void GetLimitNoFile(unsigned *soft_limit, unsigned *hard_limit) {
  *soft_limit = 0;
  *hard_limit = 0;

  struct rlimit rpl;
  memset(&rpl, 0, sizeof(rpl));
  getrlimit(RLIMIT_NOFILE, &rpl);
  *soft_limit = rpl.rlim_cur;

#ifdef __APPLE__
  int value = sysconf(_SC_OPEN_MAX);
  assert(value > 0);
  *hard_limit = value;
#else
  *hard_limit = rpl.rlim_max;
#endif
}


std::vector<LsofEntry> Lsof(const std::string &path) {
  std::vector<LsofEntry> result;

  std::vector<std::string> proc_names;
  std::vector<mode_t> proc_modes;
  ListDirectory("/proc", &proc_names, &proc_modes);

  for (unsigned i = 0; i < proc_names.size(); ++i) {
    if (!S_ISDIR(proc_modes[i]))
      continue;
    if (proc_names[i].find_first_not_of("1234567890") != std::string::npos)
      continue;

    std::vector<std::string> fd_names;
    std::vector<mode_t> fd_modes;
    std::string proc_dir = "/proc/" + proc_names[i];
    std::string fd_dir   = proc_dir + "/fd";
    bool rvb = ListDirectory(fd_dir, &fd_names, &fd_modes);
    uid_t proc_uid = 0;

    // The working directory of the process requires special handling
    if (rvb) {
      platform_stat64 info;
      platform_stat(proc_dir.c_str(), &info);
      proc_uid = info.st_uid;

      std::string cwd = ReadSymlink(proc_dir + "/cwd");
      if (HasPrefix(cwd + "/", path + "/", false /* ignore_case */)) {
        LsofEntry entry;
        entry.pid = String2Uint64(proc_names[i]);
        entry.owner = proc_uid;
        entry.read_only = true;  // A bit sloppy but good enough for the moment
        entry.executable = ReadSymlink(proc_dir + "/exe");
        entry.path = cwd;
        result.push_back(entry);
      }
    }

    for (unsigned j = 0; j < fd_names.size(); ++j) {
      if (!S_ISLNK(fd_modes[j]))
        continue;
      if (fd_names[j].find_first_not_of("1234567890") != std::string::npos)
        continue;

      std::string target = ReadSymlink(fd_dir + "/" + fd_names[j]);
      if (!HasPrefix(target + "/", path + "/", false /* ignore_case */))
        continue;

      LsofEntry entry;
      entry.pid = String2Uint64(proc_names[i]);
      entry.owner = proc_uid;
      entry.read_only = !((fd_modes[j] & S_IWUSR) == S_IWUSR);
      entry.executable = ReadSymlink(proc_dir + "/exe");
      entry.path = target;
      result.push_back(entry);
    }
  }

  return result;
}


bool ProcessExists(pid_t pid) {
  assert(pid > 0);
  int retval = kill(pid, 0);
  if (retval == 0)
    return true;
  return (errno != ESRCH);
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
 * Returns -1 of the child crashed or the exit code otherwise
 */
int WaitForChild(pid_t pid) {
  assert(pid > 0);
  int statloc;
  while (true) {
    pid_t retval = waitpid(pid, &statloc, 0);
    if (retval == -1) {
      if (errno == EINTR)
        continue;
      PANIC(NULL);
    }
    assert(retval == pid);
    break;
  }
  if (WIFEXITED(statloc))
    return WEXITSTATUS(statloc);
  return -1;
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

  std::set<int> preserve_fildes;
  preserve_fildes.insert(0);
  preserve_fildes.insert(1);
  preserve_fildes.insert(2);
  std::map<int, int> map_fildes;
  map_fildes[pipe_stdin[0]] = 0;  // Reading end of pipe_stdin
  map_fildes[pipe_stdout[1]] = 1;  // Writing end of pipe_stdout
  map_fildes[pipe_stderr[1]] = 2;  // Writing end of pipe_stderr
  std::vector<std::string> cmd_line;
  cmd_line.push_back(binary_path);
  cmd_line.insert(cmd_line.end(), argv.begin(), argv.end());

  if (!ManagedExec(cmd_line,
                   preserve_fildes,
                   map_fildes,
                   true /* drop_credentials */,
                   false /* clear_env */,
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
                       std::vector<std::string>(), double_fork);
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
bool ManagedExec(const std::vector<std::string>  &command_line,
                 const std::set<int>        &preserve_fildes,
                 const std::map<int, int>   &map_fildes,
                 const bool             drop_credentials,
                 const bool             clear_env,
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

    if (clear_env) {
#ifdef __APPLE__
      environ = NULL;
#else
      int retval = clearenv();
      assert(retval == 0);
#endif
    }

    const char *argv[command_line.size() + 1];
    for (unsigned i = 0; i < command_line.size(); ++i)
      argv[i] = command_line[i].c_str();
    argv[command_line.size()] = NULL;

    // Child, map file descriptors
    for (std::map<int, int>::const_iterator i = map_fildes.begin(),
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
    failed = ForkFailures::kSendPid;
    pipe_fork.Write(&failed, sizeof(failed));
    pipe_fork.Write(pid_grand_child);

    execvp(command_line[0].c_str(), const_cast<char **>(argv));

    failed = ForkFailures::kFailExec;

   fork_failure:
    pipe_fork.Write(&failed, sizeof(failed));
    _exit(1);
  }
  if (double_fork) {
    int statloc;
    waitpid(pid, &statloc, 0);
  }

  close(pipe_fork.write_end);

  // Either the PID or a return value is sent
  ForkFailures::Names status_code;
  bool retcode = pipe_fork.Read(&status_code, sizeof(status_code));
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
 * The contents of the iov vector might be modified by the function.
 */
bool SafeWriteV(int fd, struct iovec *iov, unsigned iovcnt) {
  unsigned nbytes = 0;
  for (unsigned i = 0; i < iovcnt; ++i)
    nbytes += iov[i].iov_len;
  unsigned iov_idx = 0;

  while (nbytes) {
    ssize_t retval = writev(fd, &iov[iov_idx], iovcnt - iov_idx);
    if (retval < 0) {
      if (errno == EINTR)
        continue;
      return false;
    }
    assert(static_cast<size_t>(retval) <= nbytes);
    nbytes -= retval;

    unsigned sum_written_blocks = 0;
    while ((sum_written_blocks + iov[iov_idx].iov_len) <=
           static_cast<size_t>(retval))
    {
      sum_written_blocks += iov[iov_idx].iov_len;
      iov_idx++;
      if (iov_idx == iovcnt) {
        assert(sum_written_blocks == static_cast<size_t>(retval));
        return true;
      }
    }
    unsigned offset = retval - sum_written_blocks;
    iov[iov_idx].iov_len -= offset;
    iov[iov_idx].iov_base =
      reinterpret_cast<char *>(iov[iov_idx].iov_base) + offset;
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

bool SafeWriteToFile(const std::string &content,
                     const std::string &path,
                     int mode) {
  int fd = open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, mode);
  if (fd < 0) return false;
  bool retval = SafeWrite(fd, content.data(), content.size());
  close(fd);
  return retval;
}


#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif
