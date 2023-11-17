/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include "bridge/marshal.h"
#include "bridge/migrate.h"
#include "fuse_directory_handle.h"
#include "fuse_inode_gen.h"
#include "fuse_state.h"
#include "state.h"
#include "util/smalloc.h"

#include <cstdint>
#include <cstring>

TEST(T_State, DirectoryListing) {
  cvmfs::DirectoryHandles h;
  size_t nbytes = StateSerializer::SerializeDirectoryHandles(h, NULL);
  void *buffer = smalloc(nbytes);
  StateSerializer::SerializeDirectoryHandles(h, buffer);
  cvmfs::DirectoryHandles check;
  StateSerializer::DeserializeDirectoryHandles(buffer, &check);
  EXPECT_EQ(0u, check.size());

  cvmfs::DirectoryListing l1;
  l1.buffer = reinterpret_cast<char *>(smalloc(1));
  l1.buffer[0] = 'x';
  l1.size = l1.capacity = 1;

  cvmfs::DirectoryListing l2;
  l2.buffer = reinterpret_cast<char *>(smmap(1));
  l2.buffer[0] = 'y';
  l2.size = 1;
  l2.capacity = 0;

  cvmfs::DirectoryListing l3;
  l3.buffer = NULL;
  l3.size = l3.capacity = 0;

  h[137] = l1;
  h[42] = l2;
  h[1] = l3;

  nbytes = StateSerializer::SerializeDirectoryHandles(h, NULL);
  buffer = smalloc(nbytes);
  StateSerializer::SerializeDirectoryHandles(h, buffer);
  StateSerializer::DeserializeDirectoryHandles(buffer, &check);
  EXPECT_EQ(3u, check.size());

  EXPECT_EQ('x', check[137].buffer[0]);
  EXPECT_EQ(1u, check[137].size);
  EXPECT_EQ(1u, check[137].capacity);
  free(check[137].buffer);

  EXPECT_EQ('y', check[42].buffer[0]);
  EXPECT_EQ(1u, check[42].size);
  EXPECT_EQ(0u, check[42].capacity);
  smunmap(check[42].buffer);

  EXPECT_EQ(NULL, check[0].buffer);
  EXPECT_EQ(0u, check[0].size);
  EXPECT_EQ(0u, check[0].capacity);

  cvmfs::DirectoryHandles *old = new cvmfs::DirectoryHandles(h);
  check.clear();
  void *v2s = cvm_bridge_migrate_directory_handles_v1v2s(old);
  StateSerializer::DeserializeDirectoryHandles(v2s, &check);
  cvm_bridge_free_directory_handles_v1(old);
  EXPECT_EQ(3u, check.size());
  EXPECT_EQ('x', check[137].buffer[0]);
  free(check[137].buffer);
  EXPECT_EQ('y', check[42].buffer[0]);
  smunmap(check[42].buffer);
  EXPECT_EQ(NULL, check[0].buffer);
}

TEST(T_State, OpenFilesCounter) {
  unsigned char buffer[4];
  EXPECT_EQ(4u, StateSerializer::SerializeOpenFilesCounter(42, NULL));
  EXPECT_EQ(4u, StateSerializer::SerializeOpenFilesCounter(42, buffer));
  uint32_t check;
  EXPECT_EQ(4u, StateSerializer::DeserializeOpenFilesCounter(buffer, &check));
  EXPECT_EQ(42u, check);

  uint32_t *value_v1 = new uint32_t(137);
  void *v2s = cvm_bridge_migrate_nfiles_ctr_v1v2s(value_v1);
  EXPECT_EQ(4u, StateSerializer::DeserializeOpenFilesCounter(v2s, &check));
  cvm_bridge_free_nfiles_ctr_v1(value_v1);
  EXPECT_EQ(137u, check);
}

TEST(T_State, InodeGeneration) {
  cvmfs::InodeGenerationInfo value;
  value.initial_revision = 137;
  value.incarnation = 138;
  value.overflow_counter = 139;
  value.inode_generation = 140;

  size_t nbytes = StateSerializer::SerializeInodeGeneration(value, NULL);
  void *buffer = smalloc(nbytes);
  EXPECT_EQ(nbytes, StateSerializer::SerializeInodeGeneration(value, buffer));

  cvmfs::InodeGenerationInfo check;
  EXPECT_EQ(nbytes,
            StateSerializer::DeserializeInodeGeneration(buffer, &check));
  free(buffer);

  EXPECT_EQ(value.version, check.version);
  EXPECT_EQ(value.initial_revision, check.initial_revision);
  EXPECT_EQ(value.incarnation, check.incarnation);
  EXPECT_EQ(value.overflow_counter, check.overflow_counter);
  EXPECT_EQ(value.inode_generation, check.inode_generation);

  cvmfs::InodeGenerationInfo *value_v1 = new cvmfs::InodeGenerationInfo();
  memcpy(value_v1, &value, sizeof(value));
  void *v2s = cvm_bridge_migrate_inode_generation_v1v2s(value_v1);

  EXPECT_EQ(nbytes, StateSerializer::DeserializeInodeGeneration(v2s, &check));
  cvm_bridge_free_inode_generation_v1(value_v1);

  EXPECT_EQ(value.version, check.version);
  EXPECT_EQ(value.initial_revision, check.initial_revision);
  EXPECT_EQ(value.incarnation, check.incarnation);
  EXPECT_EQ(value.overflow_counter, check.overflow_counter);
  EXPECT_EQ(value.inode_generation, check.inode_generation);
}

TEST(T_State, FuseState) {
  cvmfs::FuseState value;
  value.cache_symlinks = true;
  value.has_dentry_expire = true;

  size_t nbytes = StateSerializer::SerializeFuseState(value, NULL);
  void *buffer = smalloc(nbytes);
  EXPECT_EQ(nbytes, StateSerializer::SerializeFuseState(value, buffer));

  cvmfs::FuseState check;
  EXPECT_EQ(nbytes, StateSerializer::DeserializeFuseState(buffer, &check));
  free(buffer);

  EXPECT_EQ(value.version, check.version);
  EXPECT_EQ(value.cache_symlinks, check.cache_symlinks);
  EXPECT_EQ(value.has_dentry_expire, check.has_dentry_expire);

  cvmfs::FuseState *value_v1 = new cvmfs::FuseState();
  memcpy(value_v1, &value, sizeof(value));
  void *v2s = cvm_bridge_migrate_fuse_state_v1v2s(value_v1);

  EXPECT_EQ(nbytes, StateSerializer::DeserializeFuseState(v2s, &check));
  cvm_bridge_free_fuse_state_v1(value_v1);

  EXPECT_EQ(value.version, check.version);
  EXPECT_EQ(value.cache_symlinks, check.cache_symlinks);
  EXPECT_EQ(value.has_dentry_expire, check.has_dentry_expire);
}
