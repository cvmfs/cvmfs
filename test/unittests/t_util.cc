/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <fcntl.h>
#include <pthread.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <tbb/tbb_thread.h>
#include <ctime>
#include <unistd.h>
#include <vector>

#include "../../cvmfs/util.h"
#include "../../cvmfs/shortstring.h"

using namespace std;  // NOLINT

class ThreadDummy {
 public:
  explicit ThreadDummy(int canary_value)
    : result_value(0)
    , value_(canary_value)
  { }

  void OtherThread() {
    result_value = value_;
  }

  int result_value;

 private:
  const int value_;
};

const string empty = "";
const string path_with_slash = "/my/path/";
const string path_without_slash = "/my/path";
const string fake_path = "mypath";
const string socket_address = "/tmp/mysocket";
const string long_path = "/path_path_path_path_path_path_path_path_path_path_"
    "path_path_path_path_path_path_path_path_path_path_path_path_path_path";
const string to_write = "Hello, world!\n";


void WriteBuffer(int fd) {
  sleep(1);
  write(fd, to_write.c_str(), to_write.length());
}

void CreateFile(const string &filename, const string &content) {
  FILE* myfile = fopen(filename.c_str(), "w");
  fprintf(myfile, content.c_str());
  fclose(myfile);
}

void LockFileTest(const string &filename, int &retval) {
  retval = LockFile(filename);
}


TEST(T_Util, ThreadProxy) {
  const int canary = 1337;

  ThreadDummy dummy(canary);
  tbb::tbb_thread thread(&ThreadProxy<ThreadDummy>,
                         &dummy,
                         &ThreadDummy::OtherThread);
  thread.join();

  EXPECT_EQ(canary, dummy.result_value);
}


TEST(T_Util, GetUidOf) {
  uid_t uid;
  gid_t gid;
  EXPECT_TRUE(GetUidOf("root", &uid, &gid));
  EXPECT_EQ(0U, uid);
  EXPECT_EQ(0U, gid);
  EXPECT_FALSE(GetUidOf("no-such-user", &uid, &gid));
}


TEST(T_Util, GetGidOf) {
  gid_t gid;
  EXPECT_TRUE(GetGidOf("root", &gid));
  EXPECT_EQ(0U, gid);
  EXPECT_FALSE(GetGidOf("no-such-group", &gid));
}


TEST(T_Util, IsAbsolutePath) {
  const bool empty = IsAbsolutePath("");
  EXPECT_FALSE(empty) << "empty path string treated as absolute";

  const bool relative = IsAbsolutePath("foo.bar");
  EXPECT_FALSE(relative) << "relative path treated as absolute";
  const bool absolute = IsAbsolutePath("/tmp/foo.bar");
  EXPECT_TRUE(absolute) << "absolute path not recognized";
}


TEST(T_Util, HasSuffix) {
  EXPECT_TRUE(HasSuffix("abc-foo", "-foo", false));
  EXPECT_FALSE(HasSuffix("abc-foo", "-FOO", false));
  EXPECT_TRUE(HasSuffix("abc-foo", "-FOO", true));
  EXPECT_TRUE(HasSuffix("", "", false));
  EXPECT_TRUE(HasSuffix("abc", "", false));
  EXPECT_TRUE(HasSuffix("-foo", "-foo", false));
  EXPECT_FALSE(HasSuffix("abc+foo", "-foo", false));
  EXPECT_FALSE(HasSuffix("foo", "-foo", false));
}


TEST(T_Util, RemoveTree) {
  string tmp_path_ = CreateTempDir("/tmp/cvmfs_test", 0700);
  ASSERT_NE("", tmp_path_);
  ASSERT_TRUE(DirectoryExists(tmp_path_));
  EXPECT_TRUE(RemoveTree(tmp_path_));
  EXPECT_FALSE(DirectoryExists(tmp_path_));

  tmp_path_ = CreateTempDir("/tmp/cvmfs_test", 0700);
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


TEST(T_Util, Shuffle) {
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


TEST(T_Util, SortTeam) {
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


TEST(T_Util, String2Uint64) {
  EXPECT_EQ(String2Uint64("0"), 0U);
  EXPECT_EQ(String2Uint64("10"), 10U);
  EXPECT_EQ(String2Uint64("18446744073709551615000"), 18446744073709551615LLU);
  EXPECT_EQ(String2Uint64("1a"), 1U);
}


TEST(T_Util, IsHttpUrl) {
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

TEST(T_Util, MakeCannonicalPath) {
  EXPECT_EQ(empty, MakeCanonicalPath(empty));
  EXPECT_EQ(path_without_slash, MakeCanonicalPath(path_with_slash));
  EXPECT_EQ(path_without_slash, MakeCanonicalPath(path_without_slash));
}

TEST(T_Util, GetParentPath) {
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

TEST(T_Util, GetFileName) {
  EXPECT_EQ(empty, GetFileName(path_with_slash));
  EXPECT_EQ("path", GetFileName(path_without_slash));
  EXPECT_EQ(fake_path, GetFileName(fake_path));

  EXPECT_EQ(NameString(empty), GetFileName(PathString(path_with_slash)));
  EXPECT_EQ(NameString(NameString("path")),
      GetFileName(PathString(path_without_slash)));
  EXPECT_EQ(NameString(fake_path), GetFileName(PathString(fake_path)));
}

TEST(T_Util, CreateFile) {
  ASSERT_DEATH(CreateFile("myfakepath/otherfakepath", 0777), ".*");
  string filename = "/tmp/myfile.txt";
  CreateFile(filename, 0600);
  FILE* myfile = fopen(filename.c_str(), "w");
  EXPECT_NE(static_cast<FILE*>(NULL), myfile);
  fclose(myfile);
}

TEST(T_Util, MakeSocket) {
  int socket_fd;
  ASSERT_DEATH(MakeSocket(long_path, 0600), ".*");
  EXPECT_NE(-1, socket_fd = MakeSocket(socket_address, 0660));
  // the second time it should work as well (non socket-alrady-in-use error)
  EXPECT_NE(-1, socket_fd = MakeSocket(socket_address, 0777));
  close(socket_fd);
}

TEST(T_Util, ConnectSocket) {
  ASSERT_DEATH(ConnectSocket(long_path), ".*");
  ASSERT_EQ(-1, ConnectSocket("/tmp/fake_socket"));
  int server_fd = MakeSocket(socket_address, 0777);
  listen(server_fd, 1);
  int client_fd = ConnectSocket(socket_address);
  ASSERT_NE(-1, client_fd);
  close(client_fd);
  close(server_fd);
}

TEST(T_Util, MakePipe) {
  int fd[2];
  void *buffer_output = calloc(100, sizeof(char));
  MakePipe(fd);
  write(fd[1], to_write.c_str(), to_write.length());
  read(fd[0], buffer_output, to_write.length());

  EXPECT_STREQ(to_write.c_str(), static_cast<const char*>(buffer_output));
  ASSERT_DEATH(MakePipe(static_cast<int*>(NULL)), ".*");
  free(buffer_output);
  ClosePipe(fd);
}

TEST(T_Util, WritePipe) {
  int fd[2];
  void *buffer_output = calloc(20, sizeof(char));
  MakePipe(fd);
  WritePipe(fd[1], to_write.c_str(), to_write.length());
  read(fd[0], buffer_output, to_write.length());

  EXPECT_STREQ(to_write.c_str(), static_cast<const char*>(buffer_output));
  ASSERT_DEATH(WritePipe(-1, to_write.c_str(), to_write.length()),
      ".*");
  free(buffer_output);
  ClosePipe(fd);
}

TEST(T_Util, ReadPipe) {
  int fd[2];
  void *buffer_output = calloc(20, sizeof(char));
  MakePipe(fd);
  write(fd[1], to_write.c_str(), to_write.length());
  ReadPipe(fd[0], buffer_output, to_write.length());

  EXPECT_STREQ(to_write.c_str(), static_cast<const char*>(buffer_output));
  ASSERT_DEATH(ReadPipe(-1, buffer_output, to_write.length()), ".*");
  free(buffer_output);
  ClosePipe(fd);
}

TEST(T_Util, ReadHalfPipe) {
  int fd[2];
  void *buffer_output = calloc(20, sizeof(char));
  MakePipe(fd);

  tbb::tbb_thread writer(WriteBuffer, fd[1]);
  ReadHalfPipe(fd[0], buffer_output, to_write.length());
  writer.join();

  EXPECT_STREQ(to_write.c_str(), static_cast<const char*>(buffer_output));
  ASSERT_DEATH(ReadHalfPipe(-1, buffer_output, to_write.length()),
      ".*");
  free(buffer_output);
  ClosePipe(fd);
}

TEST(T_Util, ClosePipe) {
  int fd[2];
  void *buffer_output = calloc(20, sizeof(char));
  MakePipe(fd);
  ClosePipe(fd);
  ASSERT_DEATH(WritePipe(fd[1], to_write.c_str(), to_write.length()), ".*");
  ASSERT_DEATH(ReadPipe(fd[0], buffer_output, to_write.length()), ".*");
  free(buffer_output);
}

TEST(T_Util, Nonblock2Block) {
  int fd[2];
  MakePipe(fd);

  Nonblock2Block(fd[1]);
  int flags = fcntl(fd[1], F_GETFL);
  EXPECT_EQ(0, flags & O_NONBLOCK);
  ASSERT_DEATH(Nonblock2Block(-1), ".*");
  ClosePipe(fd);
}

TEST(T_Util, Block2Nonblock) {
  int fd[2];
  MakePipe(fd);

  Block2Nonblock(fd[1]);
  int flags = fcntl(fd[1], F_GETFL);
  EXPECT_EQ(O_NONBLOCK, flags & O_NONBLOCK);
  ASSERT_DEATH(Block2Nonblock(-1), ".*");
  ClosePipe(fd);
}

TEST(T_Util, SendMes2Socket) {
  void *buffer = calloc(20, sizeof(char));
  struct sockaddr_in client_addr;
  unsigned int client_length = sizeof(client_addr);
  int server_fd = MakeSocket(socket_address, 0777);
  listen(server_fd, 1);
  int client_fd = ConnectSocket(socket_address);
  SendMsg2Socket(client_fd, to_write);
  int new_connection = accept(server_fd, (struct sockaddr *) &client_addr,
      &client_length);
  read(new_connection, buffer, to_write.length());

  EXPECT_STREQ(to_write.c_str(), static_cast<const char*>(buffer));
  close(new_connection);
  close(client_fd);
  close(server_fd);
  free(buffer);
}

TEST(T_Util, Mutex) {
  ASSERT_DEATH(LockMutex(static_cast<pthread_mutex_t*>(NULL)), ".*");
  ASSERT_DEATH(UnlockMutex(static_cast<pthread_mutex_t*>(NULL)), ".*");
}

TEST(T_Util, SwitchCredentials) {
  // if I am root
  if(getuid() == 0) {
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

TEST(T_Util, FileExists) {
  string filename = "/tmp/fileexists.txt";
  FILE* myfile = fopen(filename.c_str(), "w");
  fclose(myfile);

  EXPECT_FALSE(FileExists("/my/fake/path/to/file.txt"));
  EXPECT_TRUE(FileExists(filename));
}

TEST(T_Util, GetFileSize) {
  string filename = "/tmp/filesize.txt";
  CreateFile(filename, to_write);

  EXPECT_EQ(-1, GetFileSize("/my/fake/path/to/file.txt"));
  EXPECT_LT(0, GetFileSize(filename));
}

TEST(T_Util, DirectoryExists) {
  EXPECT_TRUE(DirectoryExists("/usr"));
  EXPECT_FALSE(DirectoryExists("/fakepath/myfakepath"));
}

TEST(T_Util, SymlinkExists) {
  string filename = "/tmp/mysymlinkfile.txt";
  string symlinkname = "/tmp/mysymlink";
  CreateFile(filename, "");
  symlink(filename.c_str(), symlinkname.c_str());

  EXPECT_TRUE(SymlinkExists(symlinkname));
  EXPECT_FALSE(SymlinkExists("/fakepath/myfakepath"));
}

TEST(T_Util, MkdirDeep) {
  string mydirectory = "/tmp/mydirectory";
  string myfile = "/tmp/myfile";
  CreateFile(myfile, to_write);

  EXPECT_FALSE(MkdirDeep("", 0777));
  EXPECT_TRUE(MkdirDeep(mydirectory, 0777));
  EXPECT_TRUE(MkdirDeep("/tmp/another/another/another/mydirectory", 0777));
  EXPECT_TRUE(MkdirDeep(mydirectory, 0777));  // already exists, but writable
  EXPECT_FALSE(MkdirDeep(myfile, 0777));
}

TEST(T_Util, MakeCacheDirectories) {
  const string path = "/tmp/cache";
  MakeCacheDirectories(path, 0777);

  EXPECT_TRUE(DirectoryExists(path + "/txn"));
  EXPECT_TRUE(DirectoryExists(path + "/quarantaine"));
  for (int i = 0; i <= 0xff; i++) {
    char hex[3];
    snprintf(hex, sizeof(hex), "%02x", i);
    string current_dir = path + "/" + string(hex);
    ASSERT_TRUE(DirectoryExists(current_dir));
  }
}

TEST(T_Util, TryLockFile) {
  string filename = "/tmp/trylockfile.txt";
  int fd;

  EXPECT_EQ(-1, TryLockFile("/fakepath/fakefile.txt"));
  EXPECT_LE(0, fd = TryLockFile(filename));
  EXPECT_EQ(-2, TryLockFile(filename));  // second time it fails
  close(fd);
}

TEST(T_Util, LockFile) {
  string filename = "/tmp/lockfile.txt";
  int retval;
  int fd;

  EXPECT_EQ(-1, LockFile("/fakepath/fakefile.txt"));
  EXPECT_LE(0, fd = LockFile(filename));

  tbb::tbb_thread thread(LockFileTest, filename, retval);
  sleep(2);
  close(fd);  // releases the lock
  thread.join();
  close(retval);

  EXPECT_LE(0, retval);
  EXPECT_NE(fd, retval);
}

TEST(T_Util, UnlockFile) {
  string filename = "/tmp/unlockfile.txt";
  int fd1 = TryLockFile(filename);
  int fd2;

  EXPECT_EQ(-1, LockFile("/fakepath/fakefile.txt"));
  EXPECT_LE(0, fd1);
  EXPECT_EQ(-2, TryLockFile(filename));
  UnlockFile(fd1);
  EXPECT_LE(0, fd2 = TryLockFile(filename));  //can be locked again
  close(fd1);
  close(fd2);
}

TEST(T_Util, CreateTempFile) {
  FILE* file = NULL;
  string final_path;

  EXPECT_EQ(static_cast<FILE*>(NULL), CreateTempFile("/fakepath/fakefile.txt",
      0600, "w+", &final_path));
  EXPECT_FALSE(FileExists(final_path));
  EXPECT_NE(static_cast<FILE*>(NULL), file = CreateTempFile(
      "/tmp/mytempfile.txt", 0600, "w+", &final_path));
  EXPECT_TRUE(FileExists(final_path));
  fclose(file);
}

TEST(T_Util, CreateTempPath) {
  string file;

  EXPECT_EQ("", file = CreateTempPath("/fakepath/myfakefile.txt", 0600));
  EXPECT_FALSE(FileExists(file));
  EXPECT_NE("", file = CreateTempPath("/tmp/createmppath.txt", 0600));
  EXPECT_TRUE(FileExists(file));
}

TEST(T_Util, CreateTempDir) {
  string directory;

  EXPECT_EQ("", directory = CreateTempDir("/fakepath/myfakedirectory", 0600));
  EXPECT_FALSE(DirectoryExists(directory));
  EXPECT_NE("", directory = CreateTempDir("/tmp/creatempdirectory", 0600));
  EXPECT_TRUE(DirectoryExists(directory));
}

TEST(T_Util, FindFiles) {
  vector<string> result;
  string dir = "/tmp/findfilesdir";
  string files[] = { dir + "/file1.txt", dir + "/file2.txt",
      dir + "/file3.conf" };
  const unsigned size = 3;
  MkdirDeep(dir, 0755);
  for(unsigned i = 0; i < size; ++i)
    CreateFile(files[i], files[i]);

  result = FindFiles("/fakepath/fakedir", "");
  EXPECT_TRUE(result.empty());

  result = FindFiles(dir, "");  // find them all
  EXPECT_EQ(size + 2, result.size());  // FindFiles includes . and ..
  for(unsigned i = 0; i < size; ++i)
    EXPECT_EQ(files[i], result[i + 2]);  // they are sorted

  result = FindFiles(dir, ".fake");
  EXPECT_EQ(0u, result.size());

  result = FindFiles(dir, ".conf");
  EXPECT_EQ(1u, result.size());
  EXPECT_EQ(files[2], result[0]);

  result = FindFiles(dir, ".txt");
  EXPECT_EQ(2u, result.size());
  EXPECT_EQ(files[0], result[0]);
  EXPECT_EQ(files[1], result[1]);
}

TEST(T_Util, GetUmask) {
  unsigned test_umask = 0755;
  mode_t original_mask = umask(test_umask);

  EXPECT_EQ(test_umask, GetUmask());
  umask(original_mask);  // set once again the original mask
}

TEST(T_Util, StringifyBool) {
  EXPECT_EQ("yes", StringifyBool(34));
  EXPECT_EQ("yes", StringifyBool(19000));
  EXPECT_EQ("yes", StringifyBool(-10));
  EXPECT_EQ("no", StringifyBool(0));
}

TEST(T_Util, StringifyInt) {
  EXPECT_EQ("34", StringifyInt(34));
  EXPECT_EQ("19000", StringifyInt(19000));
  EXPECT_EQ("-10", StringifyInt(-10));
  EXPECT_EQ("0", StringifyInt(0));
}

TEST(T_Util, StringifyByteAsHex) {
  EXPECT_EQ("aa", StringifyByteAsHex(0xaa));
  EXPECT_EQ("10", StringifyByteAsHex(0x10));
  EXPECT_EQ("00", StringifyByteAsHex(0x00));
  EXPECT_EQ("ff", StringifyByteAsHex(0xff));
}

TEST(T_Util, StringifyDouble) {
  EXPECT_EQ("3.140", StringifyDouble(3.14));
  EXPECT_EQ("3.142", StringifyDouble(3.1415));
  EXPECT_EQ("0.000", StringifyDouble(0.0));
  EXPECT_EQ("-904567.896", StringifyDouble(-904567.8956023455));
}

TEST(T_Util, StringifyTime) {
  time_t now;
  char buf[80];
  time(&now);
  struct tm ts = *localtime(&now);
  strftime(buf, sizeof(buf), "%d %h %Y %H:%M:%S", &ts);
  string time_local = buf;
  ts = *gmtime(&now);
  strftime(buf, sizeof(buf), "%d %h %Y %H:%M:%S", &ts);
  string time_gm = buf;

  EXPECT_EQ(time_local, StringifyTime(now, true));
  EXPECT_EQ(time_gm, StringifyTime(now, false));
}

TEST(T_Util, RfcTimestamp) {

}
