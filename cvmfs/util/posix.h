/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UTIL_POSIX_H_
#define CVMFS_UTIL_POSIX_H_

#include <pthread.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <cassert>
#include <cstddef>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "shortstring.h"
#include "util/pointer.h"
#include "util/single_copy.h"

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

const unsigned kPageSize = 4096;
const size_t kMaxPathLength = 256;
const int kDefaultFileMode = S_IWUSR | S_IRUSR | S_IRGRP | S_IROTH;
const int kDefaultDirMode = S_IXUSR | S_IWUSR | S_IRUSR |
                            S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH;

std::string MakeCanonicalPath(const std::string &path);
std::string GetParentPath(const std::string &path);
PathString GetParentPath(const PathString &path);
std::string GetFileName(const std::string &path);
NameString GetFileName(const PathString &path);
void SplitPath(const std::string &path,
               std::string *dirname,
               std::string *filename);
bool IsAbsolutePath(const std::string &path);
bool IsHttpUrl(const std::string &path);

void CreateFile(const std::string &path, const int mode);
int MakeSocket(const std::string &path, const int mode);
int ConnectSocket(const std::string &path);
void MakePipe(int pipe_fd[2]);
void WritePipe(int fd, const void *buf, size_t nbyte);
void ReadPipe(int fd, void *buf, size_t nbyte);
void ReadHalfPipe(int fd, void *buf, size_t nbyte);
void ClosePipe(int pipe_fd[2]);

void Nonblock2Block(int filedes);
void Block2Nonblock(int filedes);
void SendMsg2Socket(const int fd, const std::string &msg);
void LockMutex(pthread_mutex_t *mutex);
void UnlockMutex(pthread_mutex_t *mutex);

bool SwitchCredentials(const uid_t uid, const gid_t gid,
                       const bool temporarily);

bool FileExists(const std::string &path);
int64_t GetFileSize(const std::string &path);
bool DirectoryExists(const std::string &path);
bool SymlinkExists(const std::string &path);
bool SymlinkForced(const std::string &src, const std::string &dest);
bool MkdirDeep(const std::string &path, const mode_t mode,
               bool verify_writable = true);
bool MakeCacheDirectories(const std::string &path, const mode_t mode);
FILE *CreateTempFile(const std::string &path_prefix, const int mode,
                     const char *open_flags, std::string *final_path);
std::string CreateTempPath(const std::string &path_prefix, const int mode);
std::string CreateTempDir(const std::string &path_prefix);
std::string GetCurrentWorkingDirectory();
int TryLockFile(const std::string &path);
int LockFile(const std::string &path);
void UnlockFile(const int filedes);
bool RemoveTree(const std::string &path);
std::vector<std::string> FindFiles(const std::string &dir,
                                   const std::string &suffix);

bool GetUidOf(const std::string &username, uid_t *uid, gid_t *main_gid);
bool GetGidOf(const std::string &groupname, gid_t *gid);
mode_t GetUmask();
bool AddGroup2Persona(const gid_t gid);

void BlockSignal(int signum);
void WaitForSignal(int signum);
void Daemonize();
bool Shell(int *pipe_stdin, int *pipe_stdout, int *pipe_stderr);
bool ExecuteBinary(int *fd_stdin,
                   int *fd_stdout,
                   int *fd_stderr,
                   const std::string &binary_path,
                   const std::vector<std::string>  &argv,
                   const bool double_fork = true,
                   pid_t *child_pid = NULL);
bool ManagedExec(const std::vector<std::string> &command_line,
                 const std::set<int> &preserve_fildes,
                 const std::map<int, int> &map_fildes,
                 const bool drop_credentials,
                 const bool double_fork = true,
                 pid_t *child_pid = NULL);

void SafeSleepMs(const unsigned ms);
// Note that SafeWrite cannot return partial results but
// SafeRead can (as we may have hit the EOF).
ssize_t SafeRead(int fd, void *buf, size_t nbyte);
bool SafeWrite(int fd, const void *buf, size_t nbyte);

// Read the contents of a file descriptor to a string.
bool SafeReadToString(int fd, std::string *final_result);


struct Pipe : public SingleCopy {
  Pipe() {
    int pipe_fd[2];
    MakePipe(pipe_fd);
    read_end = pipe_fd[0];
    write_end = pipe_fd[1];
  }

  Pipe(const int fd_read, const int fd_write) :
    read_end(fd_read), write_end(fd_write) {}

  void Close() {
    close(read_end);
    close(write_end);
  }

  template<typename T>
  bool Write(const T &data) {
    assert(!IsPointer<T>::value);  // TODO(rmeusel): C++11 static_assert
    const int num_bytes = write(write_end, &data, sizeof(T));
    return (num_bytes >= 0) && (static_cast<size_t>(num_bytes) == sizeof(T));
  }

  template<typename T>
  bool Read(T *data) {
    assert(!IsPointer<T>::value);  // TODO(rmeusel): C++11 static_assert
    int num_bytes = read(read_end, data, sizeof(T));
    return (num_bytes >= 0) && (static_cast<size_t>(num_bytes) == sizeof(T));
  }

  bool Write(const void *buf, size_t nbyte) {
    WritePipe(write_end, buf, nbyte);
    return true;
  }

  bool Read(void *buf, size_t nbyte) {
    ReadPipe(read_end, buf, nbyte);
    return true;
  }

  int read_end;
  int write_end;
};


#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif

#endif  // CVMFS_UTIL_POSIX_H_
