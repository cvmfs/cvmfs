/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UTIL_H_
#define CVMFS_UTIL_H_

#include <sys/time.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <time.h>
#include <fcntl.h>

#include <cstdio>

#include <string>
#include <map>
#include <vector>

#include "platform.h"
#include "hash.h"
#include "shortstring.h"

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

const int kDefaultFileMode = S_IWUSR | S_IRUSR | S_IRGRP | S_IROTH;
const int kDefaultDirMode = S_IXUSR | S_IWUSR | S_IRUSR |
                            S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH;

std::string MakeCanonicalPath(const std::string &path);
std::string GetParentPath(const std::string &path);
PathString GetParentPath(const PathString &path);
std::string GetFileName(const std::string &path);

void CreateFile(const std::string &path, const int mode);
int MakeSocket(const std::string &path, const int mode);
int ConnectSocket(const std::string &path);
void MakePipe(int pipe_fd[2]);
void WritePipe(int fd, const void *buf, size_t nbyte);
void ReadPipe(int fd, void *buf, size_t nbyte);
void ReadHalfPipe(int fd, void *buf, size_t nbyte);
void ClosePipe(int pipe_fd[2]);
void Nonblock2Block(int filedes);
void SendMsg2Socket(const int fd, const std::string &msg);

bool SwitchCredentials(const uid_t uid, const gid_t gid,
                       const bool temporarily);

bool FileExists(const std::string &path);
int64_t GetFileSize(const std::string &path);
bool DirectoryExists(const std::string &path);
bool MkdirDeep(const std::string &path, const mode_t mode);
bool MakeCacheDirectories(const std::string &path, const mode_t mode);
FILE *CreateTempFile(const std::string &path_prefix, const int mode,
                     const char *open_flags, std::string *final_path);
std::string CreateTempPath(const std::string &path_prefix, const int mode);
int LockFile(const std::string &path);
void UnlockFile(const int filedes);
bool RemoveTree(const std::string &path);

std::string StringifyInt(const int64_t value);
std::string StringifyTime(const time_t seconds, const bool utc);
std::string StringifyTimeval(const timeval value);
std::string StringifyIpv4(const uint32_t ip_address);
int64_t String2Int64(const std::string &value);
uint64_t String2Uint64(const std::string &value);
void String2Uint64Pair(const std::string &value, uint64_t *a, uint64_t *b);
bool HasPrefix(const std::string &str, const std::string &prefix,
               const bool ignore_case);

std::vector<std::string> SplitString(const std::string &str, const char delim);
std::string JoinStrings(const std::vector<std::string> &strings,
                        const std::string &joint);

double DiffTimeSeconds(struct timeval start, struct timeval end);

std::string GetLineMem(const char *text, const int text_size);
bool GetLineFile(FILE *f, std::string *line);
bool GetLineFd(const int fd, std::string *line);
std::string Trim(const std::string &raw);
std::string ToUpper(const std::string &mixed_case);
std::string ReplaceAll(const std::string &haystack, const std::string &needle,
                       const std::string &replace_by);

void BlockSignal(int signum);
void WaitForSignal(int signum);
void Daemonize();
bool Shell(int *pipe_stdin, int *pipe_stdout, int *pipe_stderr);
bool ManagedExec(const std::vector<std::string> &command_line,
                 const std::vector<int> &preserve_fildes,
                 const std::map<int, int> &map_fildes);

void SafeSleepMs(const unsigned ms);

/**
 * Generic base class to mark an inheriting class as 'non-copyable'
 */
class SingleCopy {
 protected:
  // Prevent DontCopy from being instantiated on its own
  SingleCopy() {}

 private:
  // Provoke a linker error by not implementing copy constructor and
  // assignment operator.
  SingleCopy(const SingleCopy &other);
  SingleCopy& operator=(const SingleCopy &rhs);
};

#ifdef CVMFS_NAMESPACE_GUARD
}
#endif

#endif  // CVMFS_UTIL_H_
