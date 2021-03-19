/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <alloca.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <pthread.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <cstring>
#include <ctime>
#include <limits>
#include <vector>

#include "atomic.h"
#include "shortstring.h"
#include "smalloc.h"
#include "testutil.h"
#include "util/algorithm.h"
#include "util/file_guard.h"
#include "util/mmap_file.h"
#include "util/posix.h"
#include "util/string.h"

using namespace std;  // NOLINT


class T_Util : public ::testing::Test {
 protected:
  virtual void SetUp() {
    empty = "";
    path_with_slash = "/my/path/";
    path_without_slash = "/my/path";
    fake_path = "mypath";
    to_write = "Hello, world!\n";
    while (to_write_large.size() < 1024*1024) {
      to_write_large += to_write;
    }
    sandbox = CreateTempDir(GetCurrentWorkingDirectory() + "/cvmfs_ut_util");
    socket_address = sandbox + "/mysocket";
    string long_dir = sandbox +
      "/path_path_path_path_path_path_path_path_path_path_path_path_path_"
        "path_path_path_path_path_path_path_path_path_path_path_path_path";
    int retval = mkdir(long_dir.c_str(), 0700);
    ASSERT_EQ(0, retval);
    long_path = long_dir + "/deepsocket";
    too_long_path = long_dir +
      "/socket_socket_socket_socket_socket_socket_socket_socket_socket_socket_"
      "socket_socket_socket_socket_socket_socket_socket_socket";

    struct sockaddr_un sock_addr;
    EXPECT_GT(sizeof(sock_addr.sun_path),
              socket_address.length()) << "Socket path '" << socket_address
                                       << "' seems to be bogus";
  }

  virtual void TearDown() {
    const bool retval = RemoveTree(sandbox);
    ASSERT_TRUE(retval) << "failed to remove sandbox";
  }

 protected:
  static string GetDebugger() {
    // check if we have a GDB installed
    const std::string gdb = GetExecutablePath("gdb");
    if (!gdb.empty()) {
      return gdb;
    }

    // maybe we are on a recent OS X and we find LLDB?
    return GetExecutablePath("lldb");
  }

  string CreateFileWithContent(const string &filename,
      const string &content) {
    string complete_path = sandbox + "/" + filename;
    FILE *myfile = fopen(complete_path.c_str(), "w");
    fprintf(myfile, "%s", content.c_str());
    fclose(myfile);
    return complete_path;
  }

  static string GetTimeString(time_t seconds, const bool utc) {
    char buf[32];
    struct tm ts;
    if (utc) {
      localtime_r(&seconds, &ts);
    } else {
      gmtime_r(&seconds, &ts);
    }
    strftime(buf, sizeof(buf), "%-d %b %Y %H:%M:%S", &ts);
    return string(buf);
  }

  static timeval CreateTimeval(int64_t tv_sec, int64_t tv_usec) {
    timeval t;
    t.tv_sec = tv_sec;
    t.tv_usec = tv_usec;
    return t;
  }

  static void UnBlockSignal(int signum) {
    sigset_t sigset;
    int retval = sigemptyset(&sigset);
    assert(retval == 0);
    retval = sigaddset(&sigset, signum);
    assert(retval == 0);
    retval = pthread_sigmask(SIG_UNBLOCK, &sigset, NULL);
    assert(retval == 0);
  }

  string sandbox;
  string socket_address;
  string long_path;
  string too_long_path;
  string empty;
  string path_with_slash;
  string path_without_slash;
  string fake_path;
  string to_write;
  std::string to_write_large;
};


TEST_F(T_Util, GetUserName) {
  EXPECT_FALSE(GetUserName().empty());
  if (getenv("USER") != NULL) {
    EXPECT_STREQ(getenv("USER"), GetUserName().c_str());
  }
}


TEST_F(T_Util, GetShell) {
  if (getenv("SHELL") != NULL) {
    EXPECT_STREQ(getenv("SHELL"), GetShell().c_str());
  }
}


TEST_F(T_Util, GetUserNameOf) {
  std::string name;
  EXPECT_TRUE(GetUserNameOf(0, &name));
  EXPECT_EQ("root", name);
}


TEST_F(T_Util, GetUidOf) {
  uid_t uid;
  gid_t gid;
  EXPECT_TRUE(GetUidOf("root", &uid, &gid));
  EXPECT_EQ(0U, uid);
  EXPECT_EQ(0U, gid);
  EXPECT_FALSE(GetUidOf("no-such-user", &uid, &gid));
}


TEST_F(T_Util, GetHomeDirectory) {
  EXPECT_FALSE(GetHomeDirectory().empty());
  EXPECT_TRUE(DirectoryExists(GetHomeDirectory()));
}


TEST_F(T_Util, GetGidOf) {
#ifdef __APPLE__
  const std::string group_name = "wheel";
#else
  const std::string group_name = "root";
#endif

  gid_t gid;
  EXPECT_TRUE(GetGidOf(group_name, &gid));
  EXPECT_EQ(0U, gid);
  EXPECT_FALSE(GetGidOf("no-such-group", &gid));
}


TEST_F(T_Util, IsAbsolutePath) {
  const bool empty = IsAbsolutePath("");
  EXPECT_FALSE(empty) << "empty path string treated as absolute";

  const bool relative = IsAbsolutePath("foo.bar");
  EXPECT_FALSE(relative) << "relative path treated as absolute";
  const bool absolute = IsAbsolutePath("/tmp/foo.bar");
  EXPECT_TRUE(absolute) << "absolute path not recognized";
}


TEST_F(T_Util, HasSuffix) {
  EXPECT_TRUE(HasSuffix("abc-foo", "-foo", false));
  EXPECT_FALSE(HasSuffix("abc-foo", "-FOO", false));
  EXPECT_TRUE(HasSuffix("abc-foo", "-FOO", true));
  EXPECT_TRUE(HasSuffix("", "", false));
  EXPECT_TRUE(HasSuffix("abc", "", false));
  EXPECT_TRUE(HasSuffix("-foo", "-foo", false));
  EXPECT_FALSE(HasSuffix("abc+foo", "-foo", false));
  EXPECT_FALSE(HasSuffix("foo", "-foo", false));
}


TEST_F(T_Util, RemoveTree) {
  string tmp_path_ = CreateTempDir(sandbox + "/cvmfs_test");
  ASSERT_NE("", tmp_path_);
  ASSERT_TRUE(DirectoryExists(tmp_path_));
  EXPECT_TRUE(RemoveTree(tmp_path_));
  EXPECT_FALSE(DirectoryExists(tmp_path_));

  tmp_path_ = CreateTempDir(sandbox + "/cvmfs_test");
  ASSERT_NE("", tmp_path_);
  EXPECT_TRUE(MkdirDeep(tmp_path_ + "/subdir", 0700));
  int fd = open((tmp_path_ + "/subdir/file").c_str(),
                O_CREAT | O_TRUNC | O_WRONLY, 0600);
  EXPECT_GE(fd, 0);
  if (fd >= 0)
    close(fd);
  EXPECT_TRUE(RemoveTree(tmp_path_));
  EXPECT_FALSE(DirectoryExists(tmp_path_));

  EXPECT_TRUE(RemoveTree(tmp_path_));
}


TEST_F(T_Util, Shuffle) {
  vector<int> v;
  Prng prng;
  vector<int> shuffled = Shuffle(v, &prng);
  EXPECT_EQ(v, shuffled);

  v.push_back(2);
  v.push_back(3);
  v.push_back(5);
  v.push_back(7);
  shuffled = Shuffle(v, &prng);
  ASSERT_EQ(v.size(), shuffled.size());
  EXPECT_NE(v, shuffled);
  int prod_v = 1;
  int prod_shuffled = 1;
  for (unsigned i = 0; i < shuffled.size(); ++i) {
    prod_v *= v[i];
    prod_shuffled *= shuffled[i];
  }
  EXPECT_EQ(prod_shuffled, prod_v);
}


TEST_F(T_Util, SortTeam) {
  vector<int> tractor;
  vector<string> towed;

  SortTeam(&tractor, &towed);
  ASSERT_TRUE(tractor.empty());
  ASSERT_TRUE(towed.empty());

  tractor.push_back(1);
  towed.push_back("one");
  SortTeam(&tractor, &towed);
  ASSERT_EQ(tractor.size(), towed.size());
  ASSERT_EQ(tractor.size(), 1U);
  EXPECT_EQ(tractor[0], 1);
  EXPECT_EQ(towed[0], "one");

  tractor.push_back(2);
  towed.push_back("two");
  SortTeam(&tractor, &towed);
  ASSERT_EQ(tractor.size(), towed.size());
  ASSERT_EQ(tractor.size(), 2U);
  EXPECT_EQ(tractor[0], 1);
  EXPECT_EQ(tractor[1], 2);
  EXPECT_EQ(towed[0], "one");
  EXPECT_EQ(towed[1], "two");

  tractor.push_back(3);
  towed.push_back("three");
  SortTeam(&tractor, &towed);
  ASSERT_EQ(tractor.size(), towed.size());
  ASSERT_EQ(tractor.size(), 3U);
  EXPECT_EQ(tractor[0], 1);
  EXPECT_EQ(tractor[1], 2);
  EXPECT_EQ(tractor[2], 3);
  EXPECT_EQ(towed[0], "one");
  EXPECT_EQ(towed[1], "two");
  EXPECT_EQ(towed[2], "three");

  tractor.clear();
  towed.clear();
  tractor.push_back(3);
  tractor.push_back(2);
  tractor.push_back(1);
  towed.push_back("three");
  towed.push_back("two");
  towed.push_back("one");
  SortTeam(&tractor, &towed);
  ASSERT_EQ(tractor.size(), towed.size());
  ASSERT_EQ(tractor.size(), 3U);
  EXPECT_EQ(tractor[0], 1);
  EXPECT_EQ(tractor[1], 2);
  EXPECT_EQ(tractor[2], 3);
  EXPECT_EQ(towed[0], "one");
  EXPECT_EQ(towed[1], "two");
  EXPECT_EQ(towed[2], "three");
}


TEST_F(T_Util, String2Uint64) {
  EXPECT_EQ(String2Uint64("0"), 0U);
  EXPECT_EQ(String2Uint64("10"), 10U);
  EXPECT_EQ(String2Uint64("18446744073709551615000"), 18446744073709551615LLU);
  EXPECT_EQ(String2Uint64("1a"), 1U);
  EXPECT_EQ(static_cast<uint64_t>(0), String2Uint64("-0"));
  EXPECT_EQ(static_cast<uint64_t>(0), String2Uint64("0"));
  EXPECT_EQ(static_cast<uint64_t>(234), String2Uint64("234"));
  EXPECT_EQ(static_cast<uint64_t>(234), String2Uint64("234.034"));
  EXPECT_EQ(static_cast<uint64_t>(234), String2Uint64("234.999"));
  EXPECT_EQ(static_cast<uint64_t>(234), String2Uint64("0234"));
  EXPECT_EQ(numeric_limits<uint64_t>::max(), String2Uint64("-1"));
}


TEST_F(T_Util, IsHttpUrl) {
  EXPECT_TRUE(IsHttpUrl("http://cvmfs-stratum-one.cern.ch/cvmfs/cms.cern.ch"));
  EXPECT_TRUE(IsHttpUrl("http://"));
  EXPECT_TRUE(IsHttpUrl("http://foobar"));
  EXPECT_TRUE(IsHttpUrl("HTTP://www.google.com"));
  EXPECT_TRUE(IsHttpUrl("HTtP://cvmfs-stratum-zero.cern.ch/ot/atlas"));
  EXPECT_FALSE(IsHttpUrl("http:/foobar"));
  EXPECT_FALSE(IsHttpUrl("http"));
  EXPECT_FALSE(IsHttpUrl("/srv/cvmfs/cms.cern.ch"));
  EXPECT_FALSE(IsHttpUrl("srv/cvmfs/cms.cern.ch"));
  EXPECT_FALSE(IsHttpUrl("http//foobar"));
}

TEST_F(T_Util, MakeCannonicalPath) {
  EXPECT_EQ(empty, MakeCanonicalPath(empty));
  EXPECT_EQ(path_without_slash, MakeCanonicalPath(path_with_slash));
  EXPECT_EQ(path_without_slash, MakeCanonicalPath(path_without_slash));
}

TEST_F(T_Util, GetParentPath) {
  PathString pstr_path_with_slash(path_with_slash);
  PathString pstr_path_without_slash(path_without_slash);
  PathString pstr_empty("");
  PathString pstr_fake_path(fake_path);

  EXPECT_EQ("", GetParentPath(fake_path));
  EXPECT_EQ(path_without_slash, GetParentPath(path_with_slash));
  EXPECT_EQ("/my", GetParentPath(path_without_slash));

  EXPECT_EQ(pstr_empty, GetParentPath(pstr_empty));
  EXPECT_EQ(PathString("/my"), GetParentPath(pstr_path_without_slash));
  EXPECT_EQ(pstr_path_without_slash, GetParentPath(pstr_path_with_slash));
  EXPECT_EQ(pstr_fake_path, GetParentPath(pstr_fake_path));
}

TEST_F(T_Util, GetFileName) {
  EXPECT_EQ(empty, GetFileName(path_with_slash));
  EXPECT_EQ("path", GetFileName(path_without_slash));
  EXPECT_EQ(fake_path, GetFileName(fake_path));

  EXPECT_EQ(NameString(empty), GetFileName(PathString(path_with_slash)));
  EXPECT_EQ(NameString(NameString("path")),
      GetFileName(PathString(path_without_slash)));
  EXPECT_EQ(NameString(fake_path), GetFileName(PathString(fake_path)));
}


TEST_F(T_Util, GetFileSystemInfo) {
  if (!DirectoryExists("/proc")) {
    printf("Skipping\n");
    return;
  }

  FileSystemInfo fs_info;
  fs_info = GetFileSystemInfo("/proc");
  EXPECT_EQ(kFsTypeProc, fs_info.type);
  fs_info = GetFileSystemInfo("/");
  EXPECT_EQ(kFsTypeUnknown, fs_info.type);
}


TEST_F(T_Util, ReadSymlink) {
  EXPECT_TRUE(ReadSymlink(".").empty());
  EXPECT_EQ(0, symlink(".", "cvmfs_read_symlink_test"));
  EXPECT_EQ(".", ReadSymlink("cvmfs_read_symlink_test"));
}


TEST_F(T_Util, ResolvePath) {
  EXPECT_EQ("/", ResolvePath("/"));
  EXPECT_EQ("/", ResolvePath(""));
  EXPECT_EQ("/no/such/path", ResolvePath("/no/such/path"));
  EXPECT_EQ("/", ResolvePath("/.//././."));
  EXPECT_EQ("/", ResolvePath("/usr/.."));

  EXPECT_EQ(0, symlink(".", "cvmfs_test_link"));
  EXPECT_EQ(GetCurrentWorkingDirectory(), ResolvePath("cvmfs_test_link"));

  EXPECT_EQ(0, symlink("/no/such/path", "cvmfs_test_link_dangling"));
  EXPECT_EQ("/no/such/path", ResolvePath("cvmfs_test_link_dangling"));
}


TEST_F(T_Util, IsMountPoint) {
  EXPECT_TRUE(IsMountPoint(""));
  EXPECT_TRUE(IsMountPoint("/"));
  EXPECT_FALSE(IsMountPoint("/no/such/file"));
}


TEST_F(T_Util, SplitPath) {
  string dirname;
  string filename;
  SplitPath("/a/b/c", &dirname, &filename);
  EXPECT_EQ("/a/b", dirname);  EXPECT_EQ("c", filename);
  SplitPath("a/b/c", &dirname, &filename);
  EXPECT_EQ("a/b", dirname);  EXPECT_EQ("c", filename);
  SplitPath("a/b", &dirname, &filename);
  EXPECT_EQ("a", dirname);  EXPECT_EQ("b", filename);
  SplitPath("b", &dirname, &filename);
  EXPECT_EQ(".", dirname);  EXPECT_EQ("b", filename);
  SplitPath("a//b", &dirname, &filename);
  EXPECT_EQ("a/", dirname);  EXPECT_EQ("b", filename);
  SplitPath("/a", &dirname, &filename);
  EXPECT_EQ("", dirname);  EXPECT_EQ("a", filename);
  SplitPath("/", &dirname, &filename);
  EXPECT_EQ("", dirname);  EXPECT_EQ("", filename);
  SplitPath("", &dirname, &filename);
  EXPECT_EQ(".", dirname);  EXPECT_EQ("", filename);
}


TEST_F(T_Util, CreateFile) {
  ASSERT_DEATH(CreateFile("myfakepath/otherfakepath.txt", 0777), ".*");
  string filename = sandbox + "/createfile.txt";
  CreateFile(filename, 0600);
  FILE* myfile = fopen(filename.c_str(), "w");
  EXPECT_NE(static_cast<FILE*>(NULL), myfile);
  fclose(myfile);
}

TEST_F(T_Util, MakeSocket) {
  int socket_fd0;
  int socket_fd1;
  int socket_fd2;

  EXPECT_EQ(-1, MakeSocket(too_long_path, 0600));
  EXPECT_GE(socket_fd0 = MakeSocket(long_path, 0600), 0);
  EXPECT_NE(-1, socket_fd1 = MakeSocket(socket_address, 0777));
  // the second time it should work as well (no socket-already-in-use error)
  EXPECT_NE(-1, socket_fd2 = MakeSocket(socket_address, 0777));
  close(socket_fd0);
  close(socket_fd1);
  close(socket_fd2);
}

TEST_F(T_Util, ConnectSocket) {
  int server_fd0 = MakeSocket(socket_address, 0777);
  int server_fd1 = MakeSocket(long_path, 0777);
  EXPECT_GE(server_fd0, 0);
  EXPECT_GE(server_fd1, 0);
  EXPECT_EQ(0, listen(server_fd0, 1));
  EXPECT_EQ(0, listen(server_fd1, 1));
  int client_fd0 = ConnectSocket(socket_address);
  EXPECT_GE(client_fd0, 0);
  int client_fd1 = ConnectSocket(long_path);
  EXPECT_GE(client_fd1, 0);

  close(client_fd0);
  close(client_fd1);
  close(server_fd0);
  close(server_fd1);

  EXPECT_EQ(-1, ConnectSocket(too_long_path));
  EXPECT_EQ(-1, ConnectSocket(sandbox + "/fake_socket"));
}

TEST_F(T_Util, MakePipe) {
  int fd[2];
  void *buffer_output = scalloc(100, sizeof(char));
  MakePipe(fd);
  int bytes_write = to_write.length();
  EXPECT_EQ(bytes_write, write(fd[1], to_write.c_str(), bytes_write));
  ssize_t bytes_read = read(fd[0], buffer_output, to_write.length());
  EXPECT_EQ(static_cast<size_t>(bytes_read), to_write.length());

  EXPECT_STREQ(to_write.c_str(), static_cast<const char*>(buffer_output));
  ASSERT_DEATH(MakePipe(static_cast<int*>(NULL)), ".*");
  free(buffer_output);
  ClosePipe(fd);
}

TEST_F(T_Util, WritePipe) {
  int fd[2];
  void *buffer_output = scalloc(20, sizeof(char));
  MakePipe(fd);
  WritePipe(fd[1], to_write.c_str(), to_write.length());
  ssize_t bytes_read = read(fd[0], buffer_output, to_write.length());
  EXPECT_EQ(static_cast<size_t>(bytes_read), to_write.length());

  EXPECT_STREQ(to_write.c_str(), static_cast<const char*>(buffer_output));
  ASSERT_DEATH(WritePipe(-1, to_write.c_str(), to_write.length()),
      ".*");
  free(buffer_output);
  ClosePipe(fd);
}

TEST_F(T_Util, ReadPipe) {
  int fd[2];
  void *buffer_output = scalloc(20, sizeof(char));
  MakePipe(fd);
  int bytes_write = to_write.length();
  EXPECT_EQ(bytes_write, write(fd[1], to_write.c_str(), bytes_write));
  ReadPipe(fd[0], buffer_output, to_write.length());

  EXPECT_STREQ(to_write.c_str(), static_cast<const char*>(buffer_output));
  ASSERT_DEATH(ReadPipe(-1, buffer_output, to_write.length()), ".*");
  free(buffer_output);
  ClosePipe(fd);
}



TEST_F(T_Util, ReadHalfPipe) {
  int fd[2];
  void *buffer_output = scalloc(20, sizeof(char));
  MakePipe(fd);

  int size = to_write.length();
  EXPECT_EQ(size, write(fd[1], to_write.data(), size));
  ReadHalfPipe(fd[0], buffer_output, to_write.length());

  EXPECT_EQ(0,
    memcmp(const_cast<char *>(to_write.data()), buffer_output, size));
  ASSERT_DEATH(ReadHalfPipe(-1, buffer_output, to_write.length()), ".*");
  free(buffer_output);
  ClosePipe(fd);
}

TEST_F(T_Util, ClosePipe) {
  int fd[2];
  UniquePtr<void> buffer_output(scalloc(20, sizeof(char)));
  MakePipe(fd);
  ClosePipe(fd);
  ASSERT_DEATH(WritePipe(fd[1], to_write.c_str(), to_write.length()), ".*");
  ASSERT_DEATH(ReadPipe(fd[0], buffer_output, to_write.length()), ".*");
}


static void *MainReadPipe(void *data) {
  int fd = *(reinterpret_cast<int *>(data));
  char buf = '\0';
  do {
    ReadPipe(fd, &buf, 1);
  } while (buf != 's');
  return NULL;
}

TEST_F(T_Util, SafeWrite) {
  int fd[2];
  void *buffer_output = scalloc(20, sizeof(char));
  MakePipe(fd);
  SafeWrite(fd[1], to_write.c_str(), to_write.length());
  ssize_t bytes_read = read(fd[0], buffer_output, to_write.length());
  EXPECT_EQ(static_cast<size_t>(bytes_read), to_write.length());
  EXPECT_STREQ(to_write.c_str(), static_cast<const char*>(buffer_output));
  free(buffer_output);

  // Large write
  int size = 1024*1024;  // 1M
  buffer_output = scalloc(size, 1);
  pthread_t thread;
  int retval = pthread_create(&thread, NULL, MainReadPipe, &fd[0]);
  EXPECT_EQ(0, retval);
  EXPECT_TRUE(SafeWrite(fd[1], buffer_output, size));
  char stop = 's';
  WritePipe(fd[1], &stop, 1);
  pthread_join(thread, NULL);
  free(buffer_output);
  ClosePipe(fd);

  EXPECT_FALSE(SafeWrite(-1, &stop, 1));

  EXPECT_TRUE(SafeWriteToFile("abc", sandbox + "/new_file", 0600));
  string result;
  int fd_file = open((sandbox + "/new_file").c_str(), O_RDONLY);
  EXPECT_GE(fd_file, 0);
  EXPECT_TRUE(SafeReadToString(fd_file, &result));
  close(fd_file);
  EXPECT_EQ("abc", result);
}


TEST_F(T_Util, SafeWriteV) {
  int fd[2];
  void *buffer_output = scalloc(20, sizeof(char));
  MakePipe(fd);

  struct iovec iov[3];
  iov[0].iov_base = const_cast<char *>(to_write.data());
  iov[0].iov_len = to_write.length();
  SafeWriteV(fd[1], iov, 1);
  ssize_t bytes_read = read(fd[0], buffer_output, to_write.length());
  EXPECT_EQ(static_cast<size_t>(bytes_read), to_write.length());
  EXPECT_STREQ(to_write.c_str(), static_cast<const char*>(buffer_output));
  free(buffer_output);

  buffer_output = scalloc(60, sizeof(char));
  iov[2].iov_base = iov[1].iov_base = iov[0].iov_base;
  iov[2].iov_len = iov[1].iov_len = iov[0].iov_len;
  SafeWriteV(fd[1], iov, 3);
  bytes_read = read(fd[0], buffer_output, 3 * to_write.length());
  EXPECT_EQ(3 * to_write.length(), static_cast<size_t>(bytes_read));
  free(buffer_output);

  // Large write
  int size = 1024*1024;  // 1M
  buffer_output = scalloc(size, 1);
  pthread_t thread;
  int retval = pthread_create(&thread, NULL, MainReadPipe, &fd[0]);
  EXPECT_EQ(0, retval);
  iov[0].iov_base = buffer_output;
  iov[0].iov_len = size;
  iov[2].iov_base = iov[1].iov_base = iov[0].iov_base;
  iov[2].iov_len = iov[1].iov_len = iov[0].iov_len;
  EXPECT_TRUE(SafeWriteV(fd[1], iov, 3));
  char stop = 's';
  WritePipe(fd[1], &stop, 1);
  pthread_join(thread, NULL);
  free(buffer_output);
  ClosePipe(fd);

  EXPECT_FALSE(SafeWrite(-1, iov, 1));
}


struct write_pipe_data {
  int fd;
  const char *data;
  size_t dlen;
};

static void *MainWritePipe(void *void_data) {
  struct write_pipe_data *data =
    reinterpret_cast<struct write_pipe_data *>(void_data);
  EXPECT_TRUE(SafeWrite(data->fd, data->data, data->dlen));
  close(data->fd);
  return NULL;
}


TEST_F(T_Util, SafeRead) {
  // Small read
  int fd[2];
  void *buffer_output = scalloc(40, sizeof(char));
  MakePipe(fd);
  SafeWrite(fd[1], to_write.c_str(), to_write.length());
  close(fd[1]);
  EXPECT_EQ(SafeRead(fd[0], buffer_output, 2*to_write.length()),
                     static_cast<ssize_t>(to_write.length()));
  EXPECT_STREQ(to_write.c_str(), static_cast<const char*>(buffer_output));
  free(buffer_output);
  close(fd[0]);

  // Large read
  int size = to_write_large.size() + 1024;
  EXPECT_GE(size, 1024*1024);
  MakePipe(fd);
  buffer_output = scalloc(size, 1);
  pthread_t thread;
  struct write_pipe_data pdata;
  pdata.fd = fd[1];
  pdata.data = to_write_large.c_str();
  pdata.dlen = to_write_large.size();
  int retval = pthread_create(&thread, NULL, MainWritePipe, &pdata);
  EXPECT_EQ(0, retval);
  EXPECT_EQ(SafeRead(fd[0], buffer_output, size),
            static_cast<ssize_t>(to_write_large.size()));
  pthread_join(thread, NULL);
  free(buffer_output);
  close(fd[0]);

  // Read to string
  buffer_output = scalloc(40, sizeof(char));
  MakePipe(fd);
  SafeWrite(fd[1], to_write.c_str(), to_write.length());
  close(fd[1]);
  std::string read_str;
  EXPECT_TRUE(SafeReadToString(fd[0], &read_str));
  EXPECT_EQ(to_write, read_str);
  free(buffer_output);
  close(fd[0]);

  char fail;
  EXPECT_EQ(-1, SafeRead(-1, &fail, 1));
  std::string fail_str;
  EXPECT_FALSE(SafeReadToString(-1, &fail_str));
}


TEST_F(T_Util, Nonblock2Block) {
  int fd[2];
  MakePipe(fd);

  Nonblock2Block(fd[1]);
  int flags = fcntl(fd[1], F_GETFL);
  EXPECT_EQ(0, flags & O_NONBLOCK);
  ASSERT_DEATH(Nonblock2Block(-1), ".*");
  ClosePipe(fd);
}

TEST_F(T_Util, Block2Nonblock) {
  int fd[2];
  MakePipe(fd);

  Block2Nonblock(fd[1]);
  int flags = fcntl(fd[1], F_GETFL);
  EXPECT_EQ(O_NONBLOCK, flags & O_NONBLOCK);
  ASSERT_DEATH(Block2Nonblock(-1), ".*");
  ClosePipe(fd);
}

TEST_F(T_Util, SendMes2Socket) {
  void *buffer = alloca(20);
  memset(buffer, 0, 20);
  struct sockaddr_in client_addr;
  unsigned int client_length = sizeof(client_addr);

  int server_fd = MakeSocket(socket_address, 0777);
  ASSERT_LT(0, server_fd);
  FdGuard fd_guard_server(server_fd);
  listen(server_fd, 1);

  int client_fd = ConnectSocket(socket_address);
  ASSERT_LE(0, client_fd);
  FdGuard fd_guard_client(client_fd);
  SendMsg2Socket(client_fd, to_write);
  int new_connection = accept(server_fd, (struct sockaddr *) &client_addr,
                              &client_length);
  ASSERT_LE(0, new_connection);
  FdGuard fd_guard_connection(new_connection);
  ssize_t bytes_read = read(new_connection, buffer, to_write.length());
  EXPECT_EQ(static_cast<size_t>(bytes_read), to_write.length());

  EXPECT_STREQ(to_write.c_str(), static_cast<const char*>(buffer));
}


TEST_F(T_Util, TcpEndpoints) {
  EXPECT_EQ(-1, MakeTcpEndpoint("foobar", 0));
  int fd_server = MakeTcpEndpoint("", 12345);
  EXPECT_GE(fd_server, 0);
  close(fd_server);

  fd_server = MakeTcpEndpoint("127.0.0.1", 12345);
  EXPECT_GE(fd_server, 0);
  EXPECT_EQ(0, listen(fd_server, 1));
  int fd_server2 = MakeTcpEndpoint("127.0.0.1", 12345);
  EXPECT_LT(fd_server2, 0);
  EXPECT_NE(0, listen(fd_server2, 1));

  EXPECT_EQ(-1, ConnectTcpEndpoint("foobar", 12345));
  EXPECT_EQ(-1, ConnectTcpEndpoint("127.0.0.1", 12346));
  int fd_client = ConnectTcpEndpoint("127.0.0.1", 12345);
  EXPECT_GE(fd_client, 0);
  SendMsg2Socket(fd_client, to_write);
  struct sockaddr_in client_addr;
  unsigned client_addr_len = sizeof(client_addr);
  int fd_conn = accept(fd_server, (struct sockaddr *) &client_addr,
                       &client_addr_len);
  EXPECT_GE(fd_conn, 0);
  void *buffer = alloca(20);
  memset(buffer, 0, 20);
  ssize_t bytes_read = read(fd_conn, buffer, to_write.length());
  EXPECT_EQ(static_cast<size_t>(bytes_read), to_write.length());
  EXPECT_STREQ(to_write.c_str(), static_cast<const char*>(buffer));

  close(fd_conn);
  close(fd_client);
  close(fd_server);
}


TEST_F(T_Util, SwitchCredentials) {
  // if I am root
  if (getuid() == 0) {
    SwitchCredentials(1, 1, true);
    EXPECT_EQ(1u, geteuid());
    EXPECT_EQ(1u, getegid());
    SwitchCredentials(0, 0, false);
    EXPECT_EQ(0u, geteuid());
    EXPECT_EQ(0u, getegid());
  } else {
    SwitchCredentials(0, 0, false);
    EXPECT_NE(0u, geteuid());
    EXPECT_NE(0u, getegid());
  }
}

TEST_F(T_Util, FileExists) {
  string filename = sandbox + "/" + "fileexists.txt";
  FILE* myfile = fopen(filename.c_str(), "w");
  fclose(myfile);

  EXPECT_FALSE(FileExists("/my/fake/path/to/file.txt"));
  EXPECT_TRUE(FileExists(filename));
}

TEST_F(T_Util, GetFileSize) {
  string file = CreateFileWithContent("filesize.txt", to_write);

  EXPECT_EQ(-1, GetFileSize("/my/fake/path/to/file.txt"));
  EXPECT_LT(0, GetFileSize(file));
}

TEST_F(T_Util, DirectoryExists) {
  EXPECT_TRUE(DirectoryExists("/usr"));
  EXPECT_FALSE(DirectoryExists("/fakepath/myfakepath"));
}

TEST_F(T_Util, SymlinkExists) {
  string symlinkname = sandbox + "/mysymlink";
  string filename = CreateFileWithContent("mysymlinkfile.txt", to_write);
  EXPECT_EQ(0, symlink(filename.c_str(), symlinkname.c_str()));

  EXPECT_TRUE(SymlinkExists(symlinkname));
  EXPECT_FALSE(SymlinkExists("/fakepath/myfakepath"));
}

TEST_F(T_Util, SymlinkForced) {
  string symlinkname = sandbox + "/myfile";
  string filename = CreateFileWithContent("mysymlinkfile.txt", to_write);
  EXPECT_TRUE(SymlinkForced(filename, symlinkname));
  EXPECT_TRUE(SymlinkExists(symlinkname));
  EXPECT_TRUE(SymlinkForced(filename, symlinkname));
  EXPECT_TRUE(SymlinkExists(symlinkname));
  EXPECT_FALSE(SymlinkForced(filename, "/no/such/directory"));
}

TEST_F(T_Util, MkdirDeep) {
  string mydirectory = sandbox + "/mydirectory";
  string myfile = CreateFileWithContent("myfile.txt", to_write);

  EXPECT_FALSE(MkdirDeep(empty, 0777));
  EXPECT_TRUE(MkdirDeep(mydirectory, 0777));
  EXPECT_TRUE(MkdirDeep(sandbox + "/another/another/mydirectory",
      0777));
  EXPECT_TRUE(MkdirDeep(mydirectory, 0777));  // already exists, but writable
  EXPECT_FALSE(MkdirDeep(myfile, 0777));
}

TEST_F(T_Util, MakeCacheDirectories) {
  const string path = sandbox + "/cache";
  MakeCacheDirectories(path, 0777);

  EXPECT_TRUE(DirectoryExists(path + "/txn"));
  EXPECT_TRUE(DirectoryExists(path + "/quarantaine"));
  for (int i = 0; i <= 0xff; i++) {
    char hex[4];
    snprintf(hex, sizeof(hex), "%02x", i);
    string current_dir = path + "/" + string(hex);
    ASSERT_TRUE(DirectoryExists(current_dir));
  }
}

TEST_F(T_Util, TryLockFile) {
  string filename = sandbox + "/trylockfile.txt";
  int fd;

  EXPECT_EQ(-1, TryLockFile("/fakepath/fakefile.txt"));
  EXPECT_LE(0, fd = TryLockFile(filename));
  EXPECT_EQ(-2, TryLockFile(filename));  // second time it fails
  close(fd);
}


TEST_F(T_Util, WritePidFile) {
  string filename = sandbox + "/pid";
  int fd;

  EXPECT_EQ(-1, WritePidFile("/fakepath/fakefile.txt"));
  fd = WritePidFile(filename);
  EXPECT_GE(fd, 0);
  EXPECT_EQ(-2, WritePidFile(filename));
  UnlockFile(fd);
  fd = WritePidFile(filename);
  EXPECT_GE(fd, 0);
  FILE *f = fopen(filename.c_str(), "r");
  EXPECT_TRUE(f != NULL);
  string pid_str;
  EXPECT_TRUE(GetLineFile(f, &pid_str));
  EXPECT_EQ(getpid(), String2Int64(pid_str));
  fclose(f);
  UnlockFile(fd);
}

namespace {
int g_test_lock_file_retval = -42;
static void *MainTestLockFile(void *data) {
  const char *path = reinterpret_cast<char *>(data);
  g_test_lock_file_retval = LockFile(path);
  return NULL;
}
}  // anonymous namespace

TEST_F(T_Util, LockFile) {
  string filename = sandbox + "/lockfile.txt";
  int retval;
  int fd;

  EXPECT_EQ(-1, LockFile("/fakepath/fakefile.txt"));
  EXPECT_LE(0, fd = LockFile(filename));

  pthread_t thread_lock;
  retval = pthread_create(&thread_lock, NULL, MainTestLockFile,
                          const_cast<char *>(filename.c_str()));
  assert(retval == 0);
  SafeSleepMs(100);
  close(fd);  // releases the lock
  pthread_join(thread_lock, NULL);
  close(g_test_lock_file_retval);

  EXPECT_LE(0, g_test_lock_file_retval);
  EXPECT_NE(fd, g_test_lock_file_retval);
}

TEST_F(T_Util, UnlockFile) {
  string filename = sandbox + "/unlockfile.txt";
  int fd1 = TryLockFile(filename);
  int fd2;

  EXPECT_EQ(-1, LockFile("/fakepath/fakefile.txt"));
  EXPECT_LE(0, fd1);
  EXPECT_EQ(-2, TryLockFile(filename));
  UnlockFile(fd1);
  EXPECT_LE(0, fd2 = TryLockFile(filename));  // can be locked again

  // no need to close fd1
  if (fd2 >= 0) {
    close(fd2);
  }
}

TEST_F(T_Util, CreateTempFile) {
  FILE* file = NULL;
  string final_path;

  EXPECT_EQ(static_cast<FILE*>(NULL), CreateTempFile("/fakepath/fakefile.txt",
      0600, "w+", &final_path));
  EXPECT_FALSE(FileExists(final_path));
  EXPECT_NE(static_cast<FILE*>(NULL), file = CreateTempFile(
      sandbox + "/mytempfile.txt", 0600, "w+", &final_path));
  EXPECT_TRUE(FileExists(final_path));
  fclose(file);
}

TEST_F(T_Util, CreateTempPath) {
  string file;

  EXPECT_EQ("", file = CreateTempPath("/fakepath/myfakefile.txt", 0600));
  EXPECT_FALSE(FileExists(file));
  EXPECT_NE("", file = CreateTempPath(sandbox + "/createmppath.txt",
      0600));
  EXPECT_TRUE(FileExists(file));
}

TEST_F(T_Util, CreateTempDir) {
  string directory;

  EXPECT_EQ("", directory = CreateTempDir("/fakepath/myfakedirectory"));
  EXPECT_FALSE(DirectoryExists(directory));
  EXPECT_NE("", directory = CreateTempDir(sandbox + "/creatempdirectory"))
    << errno;
  EXPECT_TRUE(DirectoryExists(directory)) << errno;
}

TEST_F(T_Util, FindFilesBySuffix) {
  vector<string> result;
  string files[] = { "file1.txt", "file2.txt", "file3.conf" };
  const unsigned size = 3;
  for (unsigned i = 0; i < size; ++i)
    CreateFileWithContent(files[i], files[i]);

  result = FindFilesBySuffix("/fakepath/fakedir", "");
  EXPECT_TRUE(result.empty());

  result = FindFilesBySuffix(sandbox, "");  // find them all
  // FindFiles includes . and .. and the precreated large directory
  EXPECT_EQ(size + 3, result.size());
  for (unsigned i = 0; i < size; ++i)
    EXPECT_EQ(sandbox + "/" + files[i], result[i + 2]);

  result = FindFilesBySuffix(sandbox, ".fake");
  EXPECT_EQ(0u, result.size());

  result = FindFilesBySuffix(sandbox, ".conf");
  EXPECT_EQ(1u, result.size());
  EXPECT_EQ(sandbox + "/" + files[2], result[0]);

  result = FindFilesBySuffix(sandbox, ".txt");
  EXPECT_EQ(2u, result.size());
  EXPECT_EQ(sandbox + "/" + files[0], result[0]);
  EXPECT_EQ(sandbox + "/" + files[1], result[1]);
}


TEST_F(T_Util, FindFilesByPrefix) {
  vector<string> result;
  string files[] = { "file.txt", "file.conf", "foo.conf" };
  const unsigned size = 3;
  for (unsigned i = 0; i < size; ++i)
    CreateFileWithContent(files[i], files[i]);

  result = FindFilesByPrefix("/fakepath/fakedir", "");
  EXPECT_TRUE(result.empty());

  result = FindFilesByPrefix(sandbox, "");  // find them all
  // FindFiles includes . and .. and the precreated large directory
  EXPECT_EQ(size + 3, result.size());
  EXPECT_EQ(sandbox + "/" + files[1], result[2]);
  EXPECT_EQ(sandbox + "/" + files[0], result[3]);
  EXPECT_EQ(sandbox + "/" + files[2], result[4]);

  result = FindFilesByPrefix(sandbox, "none");
  EXPECT_EQ(0u, result.size());

  result = FindFilesByPrefix(sandbox, "file.");
  EXPECT_EQ(2u, result.size());
  EXPECT_EQ(sandbox + "/" + files[1], result[0]);
  EXPECT_EQ(sandbox + "/" + files[0], result[1]);

  result = FindFilesByPrefix(sandbox, "f");
  EXPECT_EQ(3u, result.size());
  EXPECT_EQ(sandbox + "/" + files[1], result[0]);
  EXPECT_EQ(sandbox + "/" + files[0], result[1]);
  EXPECT_EQ(sandbox + "/" + files[2], result[2]);
}


TEST_F(T_Util, FindDirectories) {
  string parent = sandbox + "/test-find-directories";
  ASSERT_TRUE(MkdirDeep(parent, 0700));

  vector<string> result = FindDirectories(parent);
  EXPECT_TRUE(result.empty());

  ASSERT_TRUE(MkdirDeep(parent + "/dir1/sub", 0700));
  ASSERT_TRUE(MkdirDeep(parent + "/dir2", 0700));
  result = FindDirectories(parent);
  ASSERT_EQ(2U, result.size());
  EXPECT_EQ(parent + "/dir1", result[0]);
  EXPECT_EQ(parent + "/dir2", result[1]);

  string temp_file = CreateTempPath(parent + "/tempfile", 0600);
  EXPECT_FALSE(temp_file.empty());
  result = FindDirectories(parent);
  ASSERT_EQ(2U, result.size());
  EXPECT_EQ(parent + "/dir1", result[0]);
  EXPECT_EQ(parent + "/dir2", result[1]);

  EXPECT_TRUE(SymlinkForced(parent + "/dir1", parent + "/dirX"));
  result = FindDirectories(parent);
  ASSERT_EQ(3U, result.size());
  EXPECT_EQ(parent + "/dir1", result[0]);
  EXPECT_EQ(parent + "/dir2", result[1]);
  EXPECT_EQ(parent + "/dirX", result[2]);
}


TEST_F(T_Util, ListDirectory) {
  std::vector<std::string> names;
  std::vector<mode_t> modes;

  EXPECT_FALSE(ListDirectory("/no/such/dir", &names, &modes));
  string dir = sandbox + "/test-listdir";
  ASSERT_TRUE(MkdirDeep(dir, 0700));

  EXPECT_TRUE(ListDirectory(dir, &names, &modes));
  EXPECT_TRUE(names.empty());
  EXPECT_TRUE(modes.empty());

  ASSERT_TRUE(MkdirDeep(dir + "/dir1/sub", 0700));
  ASSERT_TRUE(MkdirDeep(dir + "/dir2", 0700));
  EXPECT_TRUE(SymlinkForced(dir + "/dir1", dir + "/dirX"));
  string temp_file = CreateTempPath(dir + "/tempfile", 0600);
  ASSERT_FALSE(temp_file.empty());

  EXPECT_TRUE(ListDirectory(dir, &names, &modes));
  ASSERT_EQ(4U, names.size());
  EXPECT_EQ("dir1", names[0]);
  EXPECT_EQ("dir2", names[1]);
  EXPECT_EQ("dirX", names[2]);
  EXPECT_EQ(GetFileName(temp_file), names[3]);
  EXPECT_TRUE(S_ISDIR(modes[0]));
  EXPECT_TRUE(S_ISDIR(modes[1]));
  EXPECT_TRUE(S_ISLNK(modes[2]));
  EXPECT_TRUE(S_ISREG(modes[3]));
}


TEST_F(T_Util, FindExecutable) {
  std::string ls = FindExecutable("ls");
  ASSERT_FALSE(ls.empty());
  EXPECT_EQ('/', ls[0]);
  std::string ls_abs = FindExecutable(ls);
  EXPECT_EQ(ls, ls_abs);
  std::string fail = FindExecutable("no-such-exe");
  EXPECT_TRUE(fail.empty());
}


TEST_F(T_Util, GetUmask) {
  unsigned test_umask = 0755;
  mode_t original_mask = umask(test_umask);

  EXPECT_EQ(test_umask, GetUmask());
  umask(original_mask);  // set once again the original mask
}

TEST_F(T_Util, StringifyBool) {
  EXPECT_EQ("yes", StringifyBool(34));
  EXPECT_EQ("yes", StringifyBool(19000));
  EXPECT_EQ("yes", StringifyBool(-10));
  EXPECT_EQ("yes", StringifyBool(true));
  EXPECT_EQ("no", StringifyBool(false));
  EXPECT_EQ("no", StringifyBool(0));
}

TEST_F(T_Util, StringifyInt) {
  EXPECT_EQ("34", StringifyInt(34));
  EXPECT_EQ("19000", StringifyInt(19000));
  EXPECT_EQ("-10", StringifyInt(-10));
  EXPECT_EQ("0", StringifyInt(0));
}

TEST_F(T_Util, StringifyByteAsHex) {
  EXPECT_EQ("aa", StringifyByteAsHex(0xaa));
  EXPECT_EQ("10", StringifyByteAsHex(0x10));
  EXPECT_EQ("00", StringifyByteAsHex(0x00));
  EXPECT_EQ("ff", StringifyByteAsHex(0xff));
}

TEST_F(T_Util, StringifyDouble) {
  EXPECT_EQ("3.140", StringifyDouble(3.14));
  EXPECT_EQ("3.142", StringifyDouble(3.1415));
  EXPECT_EQ("0.000", StringifyDouble(0.0));
  EXPECT_EQ("-904567.896", StringifyDouble(-904567.8956023455));
}

TEST_F(T_Util, StringifyTime) {
  time_t now = time(NULL);
  time_t other = 1263843;

  EXPECT_EQ(GetTimeString(now, true), StringifyTime(now, true));
  EXPECT_EQ(GetTimeString(now, false), StringifyTime(now, false));
  EXPECT_EQ(GetTimeString(other, true), StringifyTime(other, true));
  EXPECT_EQ(GetTimeString(other, false), StringifyTime(other, false));
}

TEST_F(T_Util, RfcTimestamp) {
  char *curr_locale = setlocale(LC_TIME, NULL);
  const char *format = "%a, %e %h %Y %H:%M:%S %Z";
  setlocale(LC_TIME, "C");
  struct tm tm;
  time_t time1 = time(NULL);
  string str = RfcTimestamp();
  strptime(str.c_str(), format, &tm);
  time_t time2 = mktime(&tm) - timezone;
  if (tm.tm_isdst > 0) {
    time2 -= 3600;
  }
  EXPECT_GT(2, time2 - time1);
  setlocale(LC_TIME, curr_locale);
}

TEST_F(T_Util, IsoTimestamp) {
  time_t now = time(NULL);
  string timestamp = IsoTimestamp();
  timestamp = timestamp.substr(0, 4) + "-" +
              timestamp.substr(4, 2) + "-" +
              timestamp.substr(6, 2) + "T" +
              timestamp.substr(9, 2) + ":" +
              timestamp.substr(11, 2) + ":" +
              timestamp.substr(13, 2) + "Z";
  time_t converted = IsoTimestamp2UtcTime(timestamp);
  EXPECT_GT(converted, 0);
  EXPECT_GE(converted, now - 5);
  EXPECT_LE(converted, now + 5);
}

TEST_F(T_Util, WhitelistTimestamp) {
  string timestamp = WhitelistTimestamp(0);
  EXPECT_STREQ("19700101000000", timestamp.c_str());
}

TEST_F(T_Util, StringifyTimeval) {
  timeval t1 = CreateTimeval(12, 3123123);
  timeval t2 = CreateTimeval(0, 0);
  timeval t3 = CreateTimeval(1461826, 9001);

  EXPECT_EQ("15123.123", StringifyTimeval(t1));
  EXPECT_EQ("0.000", StringifyTimeval(t2));
  EXPECT_EQ("1461826009.001", StringifyTimeval(t3));
}

TEST_F(T_Util, IsoTimestamp2UtcTime) {
  EXPECT_EQ(0, IsoTimestamp2UtcTime(""));
  EXPECT_EQ(0, IsoTimestamp2UtcTime("1995/12/23T23:01:05"));
  EXPECT_EQ(0, IsoTimestamp2UtcTime("1789-07-11T05:32:59Z"));
  EXPECT_EQ(819759665, IsoTimestamp2UtcTime("1995-12-23T23:01:05Z"));
  EXPECT_EQ(931671179, IsoTimestamp2UtcTime("1999-07-11T05:32:59Z"));
}

TEST_F(T_Util, String2Int64) {
  EXPECT_EQ(static_cast<int64_t>(0), String2Int64("-0"));
  EXPECT_EQ(static_cast<int64_t>(0), String2Int64("0"));
  EXPECT_EQ(static_cast<int64_t>(-234), String2Int64("-234"));
  EXPECT_EQ(static_cast<int64_t>(234), String2Int64("234"));
  EXPECT_EQ(static_cast<int64_t>(234), String2Int64("234.034"));
  EXPECT_EQ(static_cast<int64_t>(-234), String2Int64("-234.034"));
  EXPECT_EQ(static_cast<int64_t>(234), String2Int64("234.999"));
  EXPECT_EQ(static_cast<int64_t>(234), String2Int64("0234"));
}

TEST_F(T_Util, String2Uint64Parse) {
  uint64_t result;
  EXPECT_TRUE(String2Uint64Parse("0", NULL));
  EXPECT_TRUE(String2Uint64Parse("0", &result));
  EXPECT_EQ(0U, result);
  EXPECT_TRUE(String2Uint64Parse("-0", &result));
  EXPECT_EQ(0U, result);
  EXPECT_TRUE(String2Uint64Parse("1234567890", &result));
  EXPECT_EQ(1234567890U, result);
  EXPECT_FALSE(String2Uint64Parse("", &result));
  EXPECT_FALSE(String2Uint64Parse("1a", &result));
  EXPECT_FALSE(String2Uint64Parse("a1", &result));
  EXPECT_FALSE(String2Uint64Parse("-1", &result));
}

TEST_F(T_Util, String2Uint64Pair) {
  uint64_t a;
  uint64_t b;

  String2Uint64Pair("00060 8", &a, &b);
  EXPECT_EQ(60u, a);
  EXPECT_EQ(8u, b);

  String2Uint64Pair("4000 0", &a, &b);
  EXPECT_EQ(4000u, a);
  EXPECT_EQ(0u, b);

  String2Uint64Pair("1;2", &a, &b);
  EXPECT_EQ(1u, a);
  EXPECT_NE(2u, b);

  String2Uint64Pair("1-2 6", &a, &b);
  EXPECT_EQ(1u, a);
  EXPECT_NE(2u, b);
  EXPECT_NE(6u, b);
}

TEST_F(T_Util, HasPrefix) {
  EXPECT_FALSE(HasPrefix("str", "longprefix", true));
  EXPECT_FALSE(HasPrefix("str", "longprefix", false));
  EXPECT_TRUE(HasPrefix("hasprefix", "has", false));
  EXPECT_TRUE(HasPrefix("hasprefix", "has", true));
  EXPECT_FALSE(HasPrefix("hasprefix", "HAs", false));
  EXPECT_TRUE(HasPrefix("hasprefix", "HAs", true));
  EXPECT_TRUE(HasPrefix("HAsprefix", "HAs", true));
  EXPECT_TRUE(HasPrefix("HAsprefix", "", true));
  EXPECT_TRUE(HasPrefix("", "", false));
  EXPECT_TRUE(HasPrefix("X", "X", false));
}

TEST_F(T_Util, SplitString) {
  string str1 = "the string that will be cut in peaces";
  string str2 = "my::string:by:colons";
  vector<string> result;

  result = SplitString(str1, ' ', 1u);
  EXPECT_EQ(1u, result.size());
  EXPECT_EQ(str1, result[0]);

  result = SplitString(str1, ' ', 2u);
  EXPECT_EQ(2u, result.size());
  EXPECT_EQ("the", result[0]);
  EXPECT_EQ("string that will be cut in peaces", result[1]);

  result = SplitString(str1, ';', 200u);
  EXPECT_EQ(1u, result.size());
  EXPECT_EQ(str1, result[0]);

  result = SplitString(str2, ':', 200u);
  EXPECT_EQ(5u, result.size());
  EXPECT_EQ("", result[1]);
  EXPECT_EQ(SplitString(str2, ':', 5u), SplitString(str2, ':', 5000u));
}

TEST_F(T_Util, JoinStrings) {
  vector<string> result;
  result.push_back("my");
  result.push_back("beautiful");
  result.push_back("string");

  EXPECT_EQ("mybeautifulstring", JoinStrings(result, ""));
  EXPECT_EQ("my beautiful string", JoinStrings(result, " "));
  EXPECT_EQ("my;beautiful;string", JoinStrings(result, ";"));

  result.push_back("");
  EXPECT_EQ("mybeautifulstring", JoinStrings(result, ""));
  EXPECT_EQ("my beautiful string ", JoinStrings(result, " "));
  EXPECT_EQ("my;beautiful;string;", JoinStrings(result, ";"));
}

TEST_F(T_Util, ParseKeyvalMem) {
  map<char, string> map;
  string cvmfs_published =
      "Cf3bc68897a32278da5b5b0e4b5e4711a9102dde5\n"
      "B75834368\n"
      "Zfirst\n"
      "Zsecond\n"
      "Rd41d8cd98f00b204e9800998ecf8427e\n"
      "D900\n"
      "S8418\n"
      "Natlas.cern.ch\n"
      "Ncms.cern.ch\n"
      "X0b457ac12225018e0a15330364c20529e15012ab\n"
      "H70a5de156ee5eaf4f8e191591b6ade378f1120bd\n"
      "T1431669806\n"
      "--\n"
      "EEXTRA-CONTENT\n";
  const unsigned char *buffer =
      reinterpret_cast<const unsigned char *>(cvmfs_published.c_str());
  ParseKeyvalMem(buffer, cvmfs_published.length(), &map);

  EXPECT_EQ(10u, map.size());  // including the end() node in the map
  EXPECT_EQ("f3bc68897a32278da5b5b0e4b5e4711a9102dde5", map['C']);
  EXPECT_EQ("75834368", map['B']);
  EXPECT_EQ("first|second", map['Z']);  // "first" shouldn't be overwritten
  EXPECT_EQ("d41d8cd98f00b204e9800998ecf8427e", map['R']);
  EXPECT_EQ("900", map['D']);
  EXPECT_EQ("8418", map['S']);
  EXPECT_EQ("cms.cern.ch", map['N']);  // "atlas.cern.ch" should be overwritten
  EXPECT_EQ("0b457ac12225018e0a15330364c20529e15012ab", map['X']);
  EXPECT_EQ("70a5de156ee5eaf4f8e191591b6ade378f1120bd", map['H']);
  EXPECT_EQ("1431669806", map['T']);
  EXPECT_TRUE(map.find('E') == map.end());
}

TEST_F(T_Util, ParseKeyvalPath) {
  map<char, string> map;
  char *big_buffer = reinterpret_cast<char *>(scalloc(8000, sizeof(char)));
  string big_file = "bigfile.txt";
  string content_file = "contentfile.txt";
  string cvmfs_published =
      "Cf3bc68897a32278da5b5b0e4b5e4711a9102dde5\n"
      "B75834368\n"
      "Zfirst\n"
      "Zsecond\n"
      "Rd41d8cd98f00b204e9800998ecf8427e\n"
      "D900\n"
      "S8418\n"
      "Natlas.cern.ch\n"
      "Ncms.cern.ch\n"
      "X0b457ac12225018e0a15330364c20529e15012ab\n"
      "H70a5de156ee5eaf4f8e191591b6ade378f1120bd\n"
      "T1431669806\n"
      "--\n"
      "EEXTRA-CONTENT\n";
  CreateFileWithContent(big_file, string(big_buffer));
  CreateFileWithContent(content_file, cvmfs_published);

  EXPECT_FALSE(ParseKeyvalPath("/path/that/does/not/exists.txt", &map));
  EXPECT_FALSE(ParseKeyvalPath(sandbox + "/" + big_file, &map));
  EXPECT_TRUE(ParseKeyvalPath(sandbox + "/" + content_file, &map));
  free(big_buffer);
}

TEST_F(T_Util, DiffTimeSeconds) {
  struct timeval start = CreateTimeval(2, 900000);
  struct timeval end = CreateTimeval(3, 200000);
  EXPECT_DOUBLE_EQ(0.3, DiffTimeSeconds(start, end));

  start = CreateTimeval(8, 400000);
  end = CreateTimeval(1, 800000);
  EXPECT_LT(0.0, DiffTimeSeconds(start, end));  // positive difference

  start = CreateTimeval(0, 0);
  end = CreateTimeval(4, 0);
  EXPECT_DOUBLE_EQ(4.0, DiffTimeSeconds(start, end));
}

TEST_F(T_Util, GetLineMem) {
  string line1 = "first\nsecond\nthird\n";
  string line2 = "\ncontent\ncontent2\n";
  string line3 = "mycompletestring";

  EXPECT_EQ("first", GetLineMem(line1.c_str(), line1.length()));
  EXPECT_EQ("", GetLineMem(line2.c_str(), line2.length()));
  EXPECT_EQ(line3, GetLineMem(line3.c_str(), line3.length()));
}

TEST_F(T_Util, GetLineFile) {
  string result = "";
  string file1 = CreateFileWithContent("file1.txt", "first\nsecond\nthird\n");
  string file2 = CreateFileWithContent("file2.txt", "\ncontent\ncontent2\n");
  string file3 = CreateFileWithContent("file3.txt", "mycompletestring");
  string file4 = CreateFileWithContent("file4.txt", "");
  FILE* fd1 = fopen(file1.c_str(), "r");
  FILE* fd2 = fopen(file2.c_str(), "r");
  FILE* fd3 = fopen(file3.c_str(), "r");
  FILE* fd4 = fopen(file4.c_str(), "r");

  EXPECT_DEATH(GetLineFile(NULL, &result), ".*");
  EXPECT_TRUE(GetLineFile(fd1, &result));
  EXPECT_EQ("first", result);
  EXPECT_TRUE(GetLineFile(fd2, &result));
  EXPECT_EQ("", result);
  EXPECT_TRUE(GetLineFile(fd3, &result));
  EXPECT_EQ("mycompletestring", result);
  EXPECT_FALSE(GetLineFile(fd4, &result));  // it has no content
  EXPECT_EQ("", result);
  fclose(fd1);
  fclose(fd2);
  fclose(fd3);
  fclose(fd4);
}

TEST_F(T_Util, GetLineFd) {
  string result = "";
  string file1 = CreateFileWithContent("file1.txt", "first\nsecond\nthird\n");
  string file2 = CreateFileWithContent("file2.txt", "\ncontent\ncontent2\n");
  string file3 = CreateFileWithContent("file3.txt", "mycompletestring");
  string file4 = CreateFileWithContent("file4.txt", "");

  int fd1 = open(file1.c_str(), O_RDONLY);
  int fd2 = open(file2.c_str(), O_RDONLY);
  int fd3 = open(file3.c_str(), O_RDONLY);
  int fd4 = open(file4.c_str(), O_RDONLY);
  FdGuard fd_guard_1(fd1);
  FdGuard fd_guard_2(fd2);
  FdGuard fd_guard_3(fd3);
  FdGuard fd_guard_4(fd4);

  ASSERT_LE(0, fd1);
  ASSERT_LE(0, fd2);
  ASSERT_LE(0, fd3);
  ASSERT_LE(0, fd4);

  EXPECT_TRUE(GetLineFd(fd1, &result));
  EXPECT_EQ("first", result);
  EXPECT_TRUE(GetLineFd(fd2, &result));
  EXPECT_EQ("", result);
  EXPECT_TRUE(GetLineFd(fd3, &result));
  EXPECT_EQ("mycompletestring", result);
  EXPECT_FALSE(GetLineFd(fd4, &result));  // no content
  EXPECT_EQ("", result);
}

TEST_F(T_Util, Trim) {
  EXPECT_EQ("", Trim(""));
  EXPECT_EQ("hello", Trim("  hello"));
  EXPECT_EQ("hello", Trim("        hello    "));
  EXPECT_EQ("he llo how are you", Trim("    he llo how are you   "));
  EXPECT_EQ("hell o", Trim("  hell o"));
  EXPECT_EQ("hell o\n", Trim("  hell o\n"));
  EXPECT_EQ("hell o", Trim(" \r  hell o   \n", true));
}

TEST_F(T_Util, ToUpper) {
  EXPECT_EQ("", ToUpper(""));
  EXPECT_EQ("HELLO", ToUpper("hello"));
  EXPECT_EQ("HELLO", ToUpper("Hello"));
  EXPECT_EQ("HELLO HOW ARE YOU?", ToUpper("hElLo HOW are yOu?"));
  EXPECT_EQ(" 123 ", ToUpper(" 123 "));
  EXPECT_EQ("HELLO123 456", ToUpper("hEllo123 456"));
}

TEST_F(T_Util, ReplaceAll) {
  EXPECT_EQ("atlas.cern.ch", ReplaceAll("@repo@.cern.ch", "@repo@", "atlas"));
  EXPECT_EQ("aa", ReplaceAll("a@remove@a", "@remove@", ""));
  EXPECT_EQ("mystring", ReplaceAll("mystring", "", ""));
  EXPECT_EQ("??? ???my?string???", ReplaceAll("... ...my.string...", ".", "?"));
  EXPECT_EQ("it's for REPLACED or for @not-replace@",
      ReplaceAll("it's for @replace@ or for @not-replace@", "@replace@",
          "REPLACED"));
}

TEST_F(T_Util, ProcessExists) {
  EXPECT_TRUE(ProcessExists(getpid()));
  EXPECT_TRUE(ProcessExists(1));
  EXPECT_FALSE(ProcessExists(999999999));
  EXPECT_DEATH(ProcessExists(0), ".*");
}

TEST_F(T_Util, BlockSignal) {
  EXPECT_DEATH(kill(getpid(), SIGUSR1), ".*");
  BlockSignal(SIGUSR1);
  kill(getpid(), SIGUSR1);  // it doesn't crash after blocking

#ifndef __APPLE__
  EXPECT_DEATH(BlockSignal(-2000), ".*");
#endif
}

TEST_F(T_Util, WaitForSignal) {
  BlockSignal(SIGUSR1);
  kill(getpid(), SIGUSR1);
  WaitForSignal(SIGUSR1);
  UnBlockSignal(SIGUSR1);
}


TEST_F(T_Util, WaitForChild) {
  ASSERT_DEATH(WaitForChild(0), ".*");
  ASSERT_DEATH(WaitForChild(getpid()), ".*");

  pid_t pid = fork();
  switch (pid) {
    case -1: ASSERT_TRUE(false);
    case 0: while (true) { }
    default:
      kill(pid, SIGTERM);
      EXPECT_EQ(-1, WaitForChild(pid));
  }

  pid = fork();
  switch (pid) {
    case -1: ASSERT_TRUE(false);
    case 0: _exit(0);
    default:
      EXPECT_EQ(0, WaitForChild(pid));
  }

  pid = fork();
  switch (pid) {
    case -1: ASSERT_TRUE(false);
    case 0: _exit(1);
    default:
      EXPECT_EQ(1, WaitForChild(pid));
  }

  pid = fork();
  switch (pid) {
    case -1: ASSERT_TRUE(false);
    case 0: {
      int max_fd = sysconf(_SC_OPEN_MAX);
      for (int fd = 0; fd < max_fd; fd++)
        close(fd);
      char *argv[1];
      argv[0] = NULL;
#ifdef __APPLE__
      execvp("/usr/bin/true", argv);
#else
      execvp("/bin/true", argv);
#endif
      exit(1);
    }
    default:
      EXPECT_EQ(0, WaitForChild(pid));
  }
}


TEST_F(T_Util, Daemonize) {
  int pid;
  int statloc;
  int child_pid;
  int pipe_pid[2];
  MakePipe(pipe_pid);
  if ((pid = fork()) == 0) {
    Daemonize();
    child_pid = getpid();
    WritePipe(pipe_pid[1], &child_pid, sizeof(int));
    _exit(0);
  } else {
    waitpid(pid, &statloc, 0);
    ReadPipe(pipe_pid[0], &child_pid, sizeof(int));
    EXPECT_NE(getpid(), child_pid);
    ClosePipe(pipe_pid);
  }
}

TEST_F(T_Util, ExecuteBinary) {
  int fd_stdin;
  int fd_stdout;
  int fd_stderr;
  char buffer[20];
  bool result;
  string message = "CVMFS";
  vector<string> argv;
  argv.push_back(message);
  pid_t gdb_pid = 0;

  result = ExecuteBinary(
      &fd_stdin,
      &fd_stdout,
      &fd_stderr,
      "/bin/echo",
      argv,
      false,
      &gdb_pid);
  EXPECT_TRUE(result);
  ssize_t bytes_read = read(fd_stdout, buffer, message.length());
  EXPECT_EQ(static_cast<size_t>(bytes_read), message.length());
  string response(buffer, message.length());
  EXPECT_EQ(message, response);
}

TEST_F(T_Util, Shell) {
  int fd_stdin;
  int fd_stdout;
  int fd_stderr;
  const int buffer_size = 100;
  char *buffer = static_cast<char*>(scalloc(buffer_size, sizeof(char)));

  EXPECT_TRUE(Shell(&fd_stdin, &fd_stdout, &fd_stderr));
  string path = sandbox + "/newfolder";
  string command = "mkdir -p " + path +  " && cd " + path + " && pwd\n";
  WritePipe(fd_stdin, command.c_str(), command.length());
  ReadPipe(fd_stdout, buffer, path.length());
  string result(buffer, 0, path.length());

  EXPECT_EQ(path, result);
  close(fd_stdin);
  close(fd_stdout);
  close(fd_stderr);
  free(buffer);
}

TEST_F(T_Util, ManagedExecCommandLine) {
  bool success;
  pid_t pid;
  int fd_stdout[2];
  int fd_stdin[2];
  UniquePtr<unsigned char> buffer(static_cast<unsigned char*>(
    scalloc(100, 1)));
  MakePipe(fd_stdout);
  MakePipe(fd_stdin);
  string message = "CVMFS";
  vector<string> command_line;
  command_line.push_back("/bin/echo");
  command_line.push_back(message);

  set<int> preserve_filedes;
  preserve_filedes.insert(1);

  map<int, int> fd_map;
  fd_map[fd_stdout[1]] = 1;

  success = ManagedExec(command_line, preserve_filedes, fd_map,
                        true /* drop_credentials */, false /* clear_env */,
                        true /* double_fork */,
                        &pid);
  ASSERT_TRUE(success);
  close(fd_stdout[1]);
  ssize_t bytes_read = read(fd_stdout[0], buffer, message.length());
  EXPECT_EQ(static_cast<size_t>(bytes_read), message.length());
  string result(reinterpret_cast<char *>(buffer.weak_ref()));
  ASSERT_EQ(message, result);
  close(fd_stdout[0]);
}


TEST_F(T_Util, ManagedExecClearEnv) {
  bool success;
  pid_t pid;
  int fd_stdout[2];
  int fd_stdin[2];
  UniquePtr<unsigned char> buffer(static_cast<unsigned char*>(
    scalloc(100, 1)));
  MakePipe(fd_stdout);
  MakePipe(fd_stdin);
  vector<string> command_line;
  command_line.push_back("/usr/bin/env");

  set<int> preserve_filedes;
  preserve_filedes.insert(1);

  map<int, int> fd_map;
  fd_map[fd_stdout[1]] = 1;

  success = ManagedExec(command_line, preserve_filedes, fd_map,
                        true /* drop_credentials */, true /* clear_env */,
                        true /* double_fork */,
                        &pid);
  close(fd_stdout[1]);
  ASSERT_TRUE(success);
  ssize_t bytes_read = read(fd_stdout[0], buffer, 64);
  EXPECT_EQ(bytes_read, 0);
  close(fd_stdout[0]);
}


TEST_F(T_Util, ManagedExecRunShell) {
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


TEST_F(T_Util, ManagedExecExecuteBinaryDoubleFork) {
  int fd_stdin, fd_stdout, fd_stderr;
  pid_t child_pid;

  // find gdb in the $PATH of this system
  const std::string gdb = GetDebugger();
  ASSERT_NE("", gdb) << "no debugger found, but needed by this test case";

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
  const unsigned int timeout = 120000;  // 2 minutes
  unsigned int counter = 0;
  while (counter < timeout && kill(child_pid, 0) == 0) {
    SafeSleepMs(50);
    counter += 50;
  }
  EXPECT_LT(counter, timeout) << "detached process did not terminate in time";
}


TEST_F(T_Util, ManagedExecExecuteBinaryAsChild) {
  int fd_stdin, fd_stdout, fd_stderr;
  pid_t child_pid;
  pid_t my_pid = getpid();

  // find gdb in the $PATH of this system
  const std::string gdb = GetDebugger();
  ASSERT_NE("", gdb) << "no debugger found, but needed by this test case";

  // spawn a child process (not double forked)
  const bool double_fork = false;
  bool retval = ExecuteBinary(&fd_stdin,
                              &fd_stdout,
                              &fd_stderr,
                               gdb,
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

TEST_F(T_Util, StopWatch) {
  StopWatch sw;
  ASSERT_DEATH(sw.Stop(), ".*");
  sw.Start();
  ASSERT_DEATH(sw.Start(), ".*");
  sw.Stop();
  ASSERT_LT(0.0, sw.GetTime());
  ASSERT_DEATH(sw.Stop(), ".*");
  sw.Reset();
  ASSERT_DOUBLE_EQ(0.0, sw.GetTime());
}

TEST_F(T_Util, SafeSleepMs) {
  StopWatch sw;
  unsigned time = 100;
  sw.Start();
  SafeSleepMs(time);
  sw.Stop();
  ASSERT_LE(static_cast<double>(time / 1000)-50, sw.GetTime());
  ASSERT_GE(static_cast<double>(time / 1000)+50, sw.GetTime());
}

TEST_F(T_Util, Base64) {
  string original =
      "Man is distinguished, not only by his reason, but by this singular "
      "passion from other animals, which is a lust of the mind, that by a "
      "perseverance of delight in the continued and indefatigable generation of"
      " knowledge, exceeds the short vehemence of any carnal pleasure.";
  string b64 =
      "TWFuIGlzIGRpc3Rpbmd1aXNoZWQsIG5vdCBvbmx5IGJ5IGhpcyByZWFzb24sIGJ1dCBieSB0"
      "aGlzIHNpbmd1bGFyIHBhc3Npb24gZnJvbSBvdGhlciBhbmltYWxzLCB3aGljaCBpcyBhIGx1"
      "c3Qgb2YgdGhlIG1pbmQsIHRoYXQgYnkgYSBwZXJzZXZlcmFuY2Ugb2YgZGVsaWdodCBpbiB0"
      "aGUgY29udGludWVkIGFuZCBpbmRlZmF0aWdhYmxlIGdlbmVyYXRpb24gb2Yga25vd2xlZGdl"
      "LCBleGNlZWRzIHRoZSBzaG9ydCB2ZWhlbWVuY2Ugb2YgYW55IGNhcm5hbCBwbGVh"
      "c3VyZS4=";

  EXPECT_EQ(b64, Base64(original));
}

TEST_F(T_Util, Debase64) {
  string decoded;
  string original =
      "Man is distinguished, not only by his reason, but by this singular "
      "passion from other animals, which is a lust of the mind, that by a "
      "perseverance of delight in the continued and indefatigable generation of"
      " knowledge, exceeds the short vehemence of any carnal pleasure.";
  string b64 =
      "TWFuIGlzIGRpc3Rpbmd1aXNoZWQsIG5vdCBvbmx5IGJ5IGhpcyByZWFzb24sIGJ1dCBieSB0"
      "aGlzIHNpbmd1bGFyIHBhc3Npb24gZnJvbSBvdGhlciBhbmltYWxzLCB3aGljaCBpcyBhIGx1"
      "c3Qgb2YgdGhlIG1pbmQsIHRoYXQgYnkgYSBwZXJzZXZlcmFuY2Ugb2YgZGVsaWdodCBpbiB0"
      "aGUgY29udGludWVkIGFuZCBpbmRlZmF0aWdhYmxlIGdlbmVyYXRpb24gb2Yga25vd2xlZGdl"
      "LCBleGNlZWRzIHRoZSBzaG9ydCB2ZWhlbWVuY2Ugb2YgYW55IGNhcm5hbCBwbGVh"
      "c3VyZS4=";

  EXPECT_TRUE(Debase64(b64, &decoded));
  EXPECT_EQ(original, decoded);
}


TEST_F(T_Util, Tail) {
  EXPECT_EQ("", Tail("", 0));
  EXPECT_EQ("", Tail("", 1));
  EXPECT_EQ("", Tail("abc", 0));
  EXPECT_EQ("abc", Tail("abc", 1));
  EXPECT_EQ("abc\n", Tail("abc\n", 1));
  EXPECT_EQ("abc\n", Tail("abc\n", 2));
  EXPECT_EQ("b\nc\n", Tail("a\nb\nc\n", 2));
}


TEST_F(T_Util, MemoryMappedFile) {
  string filepath = CreateFileWithContent("mappedfile.txt",
      "some dummy content\n");
  MemoryMappedFile mf(filepath);

  EXPECT_FALSE(mf.IsMapped());
  EXPECT_DEATH(mf.Unmap(), ".*");
  ASSERT_TRUE(mf.Map());
  EXPECT_TRUE(mf.IsMapped());
  mf.Unmap();
}


TEST_F(T_Util, SetLimitNoFile) {
  EXPECT_EQ(-1, SetLimitNoFile(100000000));

  struct rlimit rpl;
  memset(&rpl, 0, sizeof(rpl));
  getrlimit(RLIMIT_NOFILE, &rpl);
  EXPECT_EQ(0, SetLimitNoFile(rpl.rlim_cur));
}


TEST_F(T_Util, GetLimitNoFile) {
  unsigned soft_limit = 0;
  unsigned hard_limit = 0;
  GetLimitNoFile(&soft_limit, &hard_limit);
  EXPECT_LT(0U, soft_limit);
  EXPECT_LE(soft_limit, hard_limit);
  EXPECT_LT(hard_limit, 10000000U);
}


TEST_F(T_Util, Lsof) {
  std::vector<LsofEntry> list;
  CreateFile("cvmfs_test_lsof", 0600, false /* ignore_failure */);
  int fd = open("cvmfs_test_lsof", O_RDWR);
  EXPECT_GE(fd, 0);
  list = Lsof(GetCurrentWorkingDirectory());
  close(fd);
#ifndef __APPLE__
  EXPECT_GT(list.size(), 0U);

  bool found = false;
  for (unsigned i = 0; i < list.size(); ++i) {
    if (list[i].pid != getpid())
      continue;
    if (list[i].path != GetCurrentWorkingDirectory())
      continue;

    found = true;
    EXPECT_EQ(geteuid(), list[i].owner);
    EXPECT_EQ(ReadSymlink("/proc/self/exe"), list[i].executable);
    EXPECT_TRUE(list[i].read_only);
    break;
  }
  EXPECT_TRUE(found);

  found = false;
  for (unsigned i = 0; i < list.size(); ++i) {
    if (list[i].pid != getpid())
      continue;
    if (list[i].path != GetCurrentWorkingDirectory() + "/cvmfs_test_lsof")
      continue;

    found = true;
    EXPECT_EQ(geteuid(), list[i].owner);
    EXPECT_EQ(ReadSymlink("/proc/self/exe"), list[i].executable);
    EXPECT_FALSE(list[i].read_only);
    break;
  }
  EXPECT_TRUE(found);
#else
  EXPECT_TRUE(list.empty());
#endif
}


TEST_F(T_Util, GetAbsolutePath) {
  bool ignore_failure = false;
  EXPECT_EQ("/xxx", GetAbsolutePath("/xxx"));
  EXPECT_NE("xxx", GetAbsolutePath("xxx"));

  EXPECT_FALSE(FileExists(GetAbsolutePath("xxx")));
  CreateFile("xxx", 0600, ignore_failure);
  EXPECT_TRUE(FileExists(GetAbsolutePath("xxx")));
}

TEST_F(T_Util, DiffTree) {
  MkdirDeep("./subdir", 0600);
  MkdirDeep("./subdir2", 0600);
  CreateFile("./file", 0600);
  EXPECT_TRUE(DiffTree(".", "."));
  EXPECT_TRUE(DiffTree("./.", "."));
  EXPECT_FALSE(DiffTree(".", "/"));
}

TEST(Log2Histogram, 2BinEmpty) {
  Log2Histogram log2hist(2);
  log2hist.Add(10);
  log2hist.Add(11);
  log2hist.Add(12);

  UTLog2Histogram unit_test;

  std::vector<atomic_int32> bins = unit_test.GetBins(log2hist);
  int res[3] = {3, 0, 0};
  for (int i = 0; i < 3; i++) {
    EXPECT_EQ(res[i], atomic_read32(&bins[i]));
  }
}

TEST(Log2Histogram, 2Bins) {
  Log2Histogram log2hist(2);
  log2hist.Add(0);
  log2hist.Add(1);
  log2hist.Add(1);
  log2hist.Add(2);
  log2hist.Add(3);
  log2hist.Add(4);

  UTLog2Histogram unit_test;

  std::vector<atomic_int32> bins = unit_test.GetBins(log2hist);
  int res[3] = {1, 3, 2};
  for (int i = 0; i < 3; i++) {
    EXPECT_EQ(res[i], atomic_read32(&bins[i]));
  }
}

TEST(Log2Histogram, 3Bins) {
  Log2Histogram log2hist(3);
  log2hist.Add(0);
  log2hist.Add(0);
  log2hist.Add(1);
  log2hist.Add(1);
  log2hist.Add(1);
  log2hist.Add(2);
  log2hist.Add(3);
  log2hist.Add(4);
  log2hist.Add(5);
  log2hist.Add(5);
  log2hist.Add(7);
  log2hist.Add(8);

  UTLog2Histogram unit_test;

  std::vector<atomic_int32> bins = unit_test.GetBins(log2hist);
  int res[4] = {1, 5, 2, 4};
  for (int i = 0; i < 4; i++) {
    EXPECT_EQ(res[i], atomic_read32(&bins[i]));
  }
}

TEST(Log2Histogram, Quantiles) {
  int N = 16;
  int64_t max = 1 << N;
  Log2Histogram log2hist(N + 1);

  // the quantile computation to fail with tolerance ~0.001 (~0.1%) we add a
  // safety factor of 50 (~5%)
  float tolerance = 0.05;

  Prng rng = Prng();
  rng.InitLocaltime();

  // we are oversampling to test the quantiles
  int64_t i = 1 << (N + 4);
  for (; i >= 0; i--) {
    log2hist.Add(rng.Next(max));
  }
  float qs[12] = {0.15, 0.20, 0.3,   0.5,   0.75,   0.9,
                  0.95, 0.99, 0.995, 0.999, 0.9995, 0.9999};
  for (int i = 0; i < 12; i++) {
    double expected = max * qs[i];
    double max_difference = expected * tolerance;
    unsigned int q = log2hist.GetQuantile(qs[i]);
    EXPECT_NEAR(q, expected, max_difference);
  }
}
