/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UTIL_POSIX_H_
#define CVMFS_UTIL_POSIX_H_

#include <pthread.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

#include <cassert>
#include <cerrno>
#include <cstddef>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "util/export.h"
#include "util/pointer.h"
#include "util/single_copy.h"

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

const unsigned kPageSize = 4096;
const size_t kMaxPathLength = 256;
const int kDefaultFileMode = S_IWUSR | S_IRUSR | S_IRGRP | S_IROTH;
const int kDefaultDirMode = S_IXUSR | S_IWUSR | S_IRUSR | S_IRGRP | S_IXGRP
                            | S_IROTH | S_IXOTH;
const int kPrivateFileMode = S_IWUSR | S_IRUSR;
const int kPrivateDirMode = S_IXUSR | S_IWUSR | S_IRUSR;

/**
 * The magic numbers that identify a file system in statfs()
 * Adjust GetFileSystemInfo() when new file systems are added
 */
enum EFileSystemTypes {
  kFsTypeUnknown = 0,
  kFsTypeAutofs = 0x0187,
  kFsTypeNFS = 0x6969,
  kFsTypeProc = 0x9fa0,
  kFsTypeBeeGFS = 0x19830326,
  // TMPFS_MAGIC from linux/magic.h; does not exist on mac
  // TODO(heretherebedragons): Might need some check for apple in the future
  kFsTypeTmpfs = 0x01021994
};

struct CVMFS_EXPORT FileSystemInfo {
  FileSystemInfo() : type(kFsTypeUnknown), is_rdonly(false) { }
  EFileSystemTypes type;
  bool is_rdonly;
};

struct CVMFS_EXPORT LsofEntry {
  pid_t pid;
  uid_t owner;
  bool read_only;
  std::string executable;
  std::string path;

  LsofEntry() : pid(0), owner(0), read_only(false) { }
};

CVMFS_EXPORT std::string MakeCanonicalPath(const std::string &path);
CVMFS_EXPORT std::string GetParentPath(const std::string &path);
CVMFS_EXPORT std::string GetFileName(const std::string &path);
CVMFS_EXPORT void SplitPath(const std::string &path,
                            std::string *dirname,
                            std::string *filename);
CVMFS_EXPORT bool IsAbsolutePath(const std::string &path);
CVMFS_EXPORT std::string GetAbsolutePath(const std::string &path);
CVMFS_EXPORT bool IsHttpUrl(const std::string &path);

CVMFS_EXPORT std::string ReadSymlink(const std::string &path);
CVMFS_EXPORT std::string ResolvePath(const std::string &path);
CVMFS_EXPORT bool IsMountPoint(const std::string &path);
CVMFS_EXPORT FileSystemInfo GetFileSystemInfo(const std::string &path);

CVMFS_EXPORT void CreateFile(const std::string &path, const int mode,
                             const bool ignore_failure = false);
CVMFS_EXPORT int MakeSocket(const std::string &path, const int mode);
CVMFS_EXPORT int MakeTcpEndpoint(const std::string &ipv4_address, int portno);
CVMFS_EXPORT int ConnectSocket(const std::string &path);
CVMFS_EXPORT int ConnectTcpEndpoint(const std::string &ipv4_address,
                                    int portno);
CVMFS_EXPORT void MakePipe(int pipe_fd[2]);
CVMFS_EXPORT void WritePipe(int fd, const void *buf, size_t nbyte);
CVMFS_EXPORT void ReadPipe(int fd, void *buf, size_t nbyte);
CVMFS_EXPORT bool ReadHalfPipe(int fd, void *buf, size_t nbyte,
                               unsigned timeout_ms = 0);
CVMFS_EXPORT void ClosePipe(int pipe_fd[2]);
CVMFS_EXPORT bool DiffTree(const std::string &path_a,
                           const std::string &path_b);

CVMFS_EXPORT void Nonblock2Block(int filedes);
CVMFS_EXPORT void Block2Nonblock(int filedes);
CVMFS_EXPORT void SendMsg2Socket(const int fd, const std::string &msg);
CVMFS_EXPORT bool SendFd2Socket(int socket_fd, int passing_fd);
CVMFS_EXPORT int RecvFdFromSocket(int msg_fd);
CVMFS_EXPORT std::string GetHostname();

CVMFS_EXPORT bool SwitchCredentials(const uid_t uid, const gid_t gid,
                                    const bool temporarily);

CVMFS_EXPORT bool FileExists(const std::string &path);
CVMFS_EXPORT int64_t GetFileSize(const std::string &path);
CVMFS_EXPORT bool DirectoryExists(const std::string &path);
CVMFS_EXPORT bool SymlinkExists(const std::string &path);
CVMFS_EXPORT bool SymlinkForced(const std::string &src,
                                const std::string &dest);
CVMFS_EXPORT bool MkdirDeep(const std::string &path, const mode_t mode,
                            bool verify_writable = true);
CVMFS_EXPORT bool MakeCacheDirectories(const std::string &path,
                                       const mode_t mode);
CVMFS_EXPORT FILE *CreateTempFile(const std::string &path_prefix,
                                  const int mode,
                                  const char *open_flags,
                                  std::string *final_path);
CVMFS_EXPORT std::string CreateTempPath(const std::string &path_prefix,
                                        const int mode);
CVMFS_EXPORT std::string CreateTempDir(const std::string &path_prefix);
CVMFS_EXPORT std::string GetCurrentWorkingDirectory();
CVMFS_EXPORT int TryLockFile(const std::string &path);
CVMFS_EXPORT int LockFile(const std::string &path);
CVMFS_EXPORT int WritePidFile(const std::string &path);
CVMFS_EXPORT void UnlockFile(const int filedes);
CVMFS_EXPORT bool RemoveTree(const std::string &path);
CVMFS_EXPORT
std::vector<std::string> FindFilesBySuffix(const std::string &dir,
                                           const std::string &suffix);
CVMFS_EXPORT
std::vector<std::string> FindFilesByPrefix(const std::string &dir,
                                           const std::string &prefix);
CVMFS_EXPORT
std::vector<std::string> FindDirectories(const std::string &parent_dir);
CVMFS_EXPORT std::string FindExecutable(const std::string &exe);
CVMFS_EXPORT bool ListDirectory(const std::string &directory,
                                std::vector<std::string> *names,
                                std::vector<mode_t> *modes);

CVMFS_EXPORT std::string GetUserName();
CVMFS_EXPORT std::string GetShell();
CVMFS_EXPORT bool GetUserNameOf(uid_t uid, std::string *username);
CVMFS_EXPORT bool GetUidOf(const std::string &username,
                           uid_t *uid,
                           gid_t *main_gid);
CVMFS_EXPORT bool GetGidOf(const std::string &groupname, gid_t *gid);
CVMFS_EXPORT mode_t GetUmask();
CVMFS_EXPORT bool AddGroup2Persona(const gid_t gid);
CVMFS_EXPORT std::string GetHomeDirectory();

CVMFS_EXPORT std::string GetArch();

CVMFS_EXPORT int SetLimitNoFile(unsigned limit_nofile);
CVMFS_EXPORT void GetLimitNoFile(unsigned *soft_limit, unsigned *hard_limit);

/**
 * Searches for open file descriptors on the subtree starting at path.
 * For the time being works only on Linux, not on macOS.
 */
CVMFS_EXPORT std::vector<LsofEntry> Lsof(const std::string &path);

CVMFS_EXPORT bool ProcessExists(pid_t pid);
CVMFS_EXPORT void BlockSignal(int signum);
CVMFS_EXPORT void WaitForSignal(int signum);
CVMFS_EXPORT
int WaitForChild(pid_t pid,
                 const std::vector<int> &sig_ok = std::vector<int>());
CVMFS_EXPORT void Daemonize();
CVMFS_EXPORT bool ExecAsDaemon(const std::vector<std::string> &command_line,
                               pid_t *child_pid = NULL);
CVMFS_EXPORT bool Shell(int *pipe_stdin, int *pipe_stdout, int *pipe_stderr);
CVMFS_EXPORT bool ExecuteBinary(int *fd_stdin,
                                int *fd_stdout,
                                int *fd_stderr,
                                const std::string &binary_path,
                                const std::vector<std::string> &argv,
                                const bool double_fork = true,
                                pid_t *child_pid = NULL);
CVMFS_EXPORT bool ManagedExec(const std::vector<std::string> &command_line,
                              const std::set<int> &preserve_fildes,
                              const std::map<int, int> &map_fildes,
                              const bool drop_credentials,
                              const bool clear_env = false,
                              const bool double_fork = true,
                              pid_t *child_pid = NULL);
CVMFS_EXPORT bool CloseAllFildes(const std::set<int> &preserve_fildes);

CVMFS_EXPORT void SafeSleepMs(const unsigned ms);
// Note that SafeWrite cannot return partial results but
// SafeRead can (as we may have hit the EOF).
CVMFS_EXPORT ssize_t SafeRead(int fd, void *buf, size_t nbyte);
CVMFS_EXPORT bool SafeWrite(int fd, const void *buf, size_t nbyte);
CVMFS_EXPORT bool SafeWriteV(int fd, struct iovec *iov, unsigned iovcnt);

// Read the contents of a file descriptor to a string.
CVMFS_EXPORT bool SafeReadToString(int fd, std::string *final_result);
CVMFS_EXPORT bool SafeWriteToFile(const std::string &content,
                                  const std::string &path, int mode);

#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif

#endif  // CVMFS_UTIL_POSIX_H_
