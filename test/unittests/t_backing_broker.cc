/**
 * This file is part of the CernVM File System.
 */

#include <errno.h>
#include <fcntl.h>
#include <gtest/gtest.h>
#include <pthread.h>
#include <sys/socket.h>
#include <unistd.h>
#if defined(__linux__) && defined(__has_include)
#if __has_include(<linux/fuse.h>)
#include <linux/fuse.h>
#endif
#endif

#include "monitor.h"

#ifdef FUSE_DEV_IOC_BACKING_OPEN

namespace {

void *ServeThread(void *data) {
  backing_broker::Serve(*static_cast<int *>(data));
  return NULL;
}

}  // anonymous namespace

/**
 * Drives the passthrough broker over a socketpair, without root and without a
 * FUSE mount.  No /dev/fuse is handed over, so a backing file that passes the
 * broker's checks comes back with ENODEV.
 */
class T_BackingBroker : public ::testing::Test {
 protected:
  virtual void SetUp() {
    ASSERT_EQ(0, socketpair(AF_UNIX, SOCK_SEQPACKET, 0, sv_));
    ASSERT_EQ(0, pthread_create(&thread_, NULL, ServeThread, &sv_[1]));
  }

  virtual void TearDown() {
    close(sv_[0]);
    pthread_join(thread_, NULL);
    close(sv_[1]);
  }

  int Call(int op, int arg, int fd) {
    return backing_broker::Call(sv_[0], op, arg, fd);
  }

  int sv_[2];
  pthread_t thread_;
};


TEST_F(T_BackingBroker, FuseDeviceOnly) {
  EXPECT_EQ(-EBADF, Call(backing_broker::kSetFuseDevice, 0, -1));
  const int null = open("/dev/null", O_RDWR);
  ASSERT_GE(null, 0);
  EXPECT_EQ(-EINVAL, Call(backing_broker::kSetFuseDevice, 0, null));
  close(null);
}


TEST_F(T_BackingBroker, OnlyReadOnlyRegularFiles) {
  EXPECT_EQ(-EBADF, Call(backing_broker::kBackingOpen, 0, -1));

  const int dir = open("/", O_RDONLY | O_DIRECTORY);
  ASSERT_GE(dir, 0);
  EXPECT_EQ(-EINVAL, Call(backing_broker::kBackingOpen, 0, dir));
  close(dir);

  char path[] = "/tmp/cvmfs_backing_broker_XXXXXX";
  const int file = mkstemp(path);
  ASSERT_GE(file, 0);
  EXPECT_EQ(-EACCES, Call(backing_broker::kBackingOpen, 0, file));
  close(file);

  const int readonly = open(path, O_RDONLY);
  ASSERT_GE(readonly, 0);
  EXPECT_EQ(-ENODEV, Call(backing_broker::kBackingOpen, 0, readonly));
  close(readonly);
  unlink(path);
}


TEST_F(T_BackingBroker, CloseWithoutFuseDevice) {
  EXPECT_EQ(-ENODEV, Call(backing_broker::kBackingClose, 1, -1));
}


TEST_F(T_BackingBroker, MalformedRequest) {
  const char garbage[3] = {1, 2, 3};
  ASSERT_EQ(3, send(sv_[0], garbage, sizeof(garbage), 0));
  int32_t reply = 0;
  ASSERT_EQ(static_cast<ssize_t>(sizeof(reply)),
            recv(sv_[0], &reply, sizeof(reply), 0));
  EXPECT_EQ(-EINVAL, reply);
  EXPECT_EQ(-EINVAL, Call(42, 0, -1));
}


TEST_F(T_BackingBroker, PeerGone) {
  close(sv_[0]);
  pthread_join(thread_, NULL);
  int fresh[2];
  ASSERT_EQ(0, socketpair(AF_UNIX, SOCK_SEQPACKET, 0, fresh));
  close(fresh[1]);
  EXPECT_EQ(-EPIPE, backing_broker::Call(fresh[0], 42, 0, -1));
  sv_[0] = fresh[0];
  ASSERT_EQ(0, pthread_create(&thread_, NULL, ServeThread, &sv_[1]));
}

#endif  // FUSE_DEV_IOC_BACKING_OPEN
