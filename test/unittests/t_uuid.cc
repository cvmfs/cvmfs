/**
 * This file is part of the CernVM File System.
 */

#include <unistd.h>

#include "gtest/gtest.h"
#include "util/file_guard.h"
#include "util/pointer.h"
#include "util/posix.h"
#include "util/string.h"
#include "util/uuid.h"

using namespace std;  // NOLINT

namespace cvmfs {

TEST(T_Uuid, Unique) {
  UniquePtr<Uuid> uuid(Uuid::Create(""));
  UniquePtr<Uuid> uuid2(Uuid::Create(""));
  ASSERT_TRUE(uuid.IsValid());
  ASSERT_TRUE(uuid2.IsValid());
  EXPECT_NE(uuid->uuid(), uuid2->uuid());
}


TEST(T_Uuid, Create) {
  string path;
  FILE *f = CreateTempFile("./cvmfs_ut_uuid", 0600, "w", &path);
  ASSERT_TRUE(f != NULL);
  fclose(f);
  unlink(path.c_str());

  UniquePtr<Uuid> uuid(Uuid::Create(path));
  ASSERT_TRUE(uuid.IsValid());
  ASSERT_EQ(16U, uuid->size());
  char data[16];
  memset(data, 0, sizeof(data));
  EXPECT_NE(0, memcmp(data, uuid->data(), uuid->size()));

  UnlinkGuard unlink_guard(path);
  f = fopen(path.c_str(), "r");
  ASSERT_TRUE(f != NULL);
  string line;
  bool retval = GetLineFile(f, &line);
  EXPECT_TRUE(retval);
  fclose(f);
  EXPECT_EQ(line, uuid->uuid());
}


TEST(T_Uuid, CreateOneTime) {
  EXPECT_NE(Uuid::CreateOneTime(), Uuid::CreateOneTime());
}


TEST(T_Uuid, FromCache) {
  string path;
  FILE *f = CreateTempFile("./cvmfs_ut_uuid", 0600, "w", &path);
  ASSERT_TRUE(f != NULL);
  fprintf(f, "unique");
  fclose(f);
  UnlinkGuard unlink_guard(path);

  UniquePtr<Uuid> uuid(Uuid::Create(path));
  ASSERT_FALSE(uuid.IsValid());

  EXPECT_EQ(0, truncate(path.c_str(), 0));
  UniquePtr<Uuid> uuid_empty(Uuid::Create(path));
  ASSERT_FALSE(uuid_empty.IsValid());

  UniquePtr<Uuid> uuid_valid(Uuid::Create(""));
  EXPECT_TRUE(uuid_valid.IsValid());
  f = fopen(path.c_str(), "w");
  EXPECT_TRUE(f != NULL);
  fprintf(f, "%s", uuid_valid->uuid().c_str());
  fclose(f);
  UniquePtr<Uuid> uuid_cached(Uuid::Create(path));
  EXPECT_TRUE(uuid_cached.IsValid());
  EXPECT_EQ(uuid_cached->uuid(), uuid_valid->uuid());
  EXPECT_EQ(
      0, memcmp(uuid_cached->data(), uuid_valid->data(), uuid_valid->size()));
}

TEST(T_Uuid, FailWrite) {
  UniquePtr<Uuid> uuid(Uuid::Create("/no/such/path"));
  EXPECT_FALSE(uuid.IsValid());
}

TEST(T_Uuid, FailRead) {
  string path;
  FILE *f = CreateTempFile("./cvmfs_ut_uuid", 0600, "w", &path);
  ASSERT_TRUE(f != NULL);
  fclose(f);
  UnlinkGuard unlink_guard(path);
  UniquePtr<Uuid> uuid(Uuid::Create(path));
  EXPECT_FALSE(uuid.IsValid());
}

}  // namespace cvmfs
