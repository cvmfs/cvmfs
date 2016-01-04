/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

// Single Authz test is static
// TODO(jblomer): find a better solution than pulling in the .cc file
#include "../../cvmfs/voms_authz/voms_authz.cc"  // NOLINT

#define TEST_DN "/DC=ch/DC=cern/OU=Organic Units/OU=Users" \
                "/CN=bbockelm/CN=659869/CN=Brian Paul Bockelman"

TEST(T_VOMS, VomsAuthz) {
  // Initialize the VOMS data structures
  struct vomsdata voms_info;
  memset(&voms_info, '\0', sizeof(struct vomsdata));
  struct voms voms_entry;
  memset(&voms_entry, '\0', sizeof(struct voms));
  struct voms *voms_entries[2];
  voms_entries[0] = &voms_entry;
  voms_entries[1] = NULL;
  voms_info.data = voms_entries;
  struct data voms_data[3];
  memset(voms_data, '\0', 3*sizeof(struct data));
  struct data *voms_datap[4];
  voms_datap[0] = voms_data;
  voms_datap[1] = voms_data + 1;
  voms_datap[2] = voms_data + 2;
  voms_datap[3] = NULL;
  voms_entry.std = voms_datap;

  // Fill in attributes actually used in the authz matching.
  std::vector<char> user_dn; user_dn.reserve(100);
  strncpy(&user_dn[0], TEST_DN, 99);
  voms_entry.user = &user_dn[0];
  char voname[] = "cms";
  voms_entry.voname = voname;
  std::vector<char> group1; group1.reserve(50);
  strncpy(&group1[0], "/cms", 49);
  std::vector<char> group2; group2.reserve(50);
  strncpy(&group2[0], "/cms/uscms", 49);
  std::vector<char> group3; group3.reserve(50);
  strncpy(&group3[0], "/cms/escms", 49);
  std::vector<char> role1; role1.reserve(50);
  strncpy(&role1[0], "pilot", 49);

  voms_data[0].group = &group1[0];
  voms_data[1].group = &group2[0];
  voms_data[2].group = &group3[0];
  voms_data[2].role = &role1[0];

  // Ok, now let's verify the authz checks.
  EXPECT_EQ(CheckSingleAuthz(&voms_info, "cms:/cms"), true);
  EXPECT_EQ(CheckSingleAuthz(&voms_info, "cms:/cms/uscms"), true);
  EXPECT_EQ(CheckSingleAuthz(&voms_info, "atlas:/atlas"), false);
  EXPECT_EQ(CheckSingleAuthz(&voms_info, TEST_DN), true);
  EXPECT_EQ(CheckSingleAuthz(&voms_info, "cms:/cms/dcms"), false);
  EXPECT_EQ(CheckSingleAuthz(&voms_info, "cms:/cms/Role=pilot"), true);
  EXPECT_EQ(CheckSingleAuthz(&voms_info, "cms:/cms/escms/Role=pilot"), true);
  EXPECT_EQ(CheckSingleAuthz(&voms_info, "cms:/cms/Role=prod"), false);
  EXPECT_EQ(CheckSingleAuthz(&voms_info, "cms:/cms/uscms/Role=pilot"), false);
  EXPECT_EQ(CheckSingleAuthz(&voms_info, "cms:/cms/dcms/Role=pilot"), false);

  voms_data[0].group = NULL;
  EXPECT_EQ(CheckSingleAuthz(&voms_info, "cms:/cms"), true);
  EXPECT_EQ(CheckSingleAuthz(&voms_info, "cms:/cms/Role=pilot"), true);
  voms_entry.user = NULL;
  EXPECT_EQ(CheckSingleAuthz(&voms_info, TEST_DN), false);

  // Switch to multiple authz functions.
  EXPECT_EQ(CheckMultipleAuthz(&voms_info, ""), false);
  EXPECT_EQ(CheckMultipleAuthz(&voms_info, "\n"), false);
  EXPECT_EQ(CheckMultipleAuthz(&voms_info, "cms:/cms"), true);
  EXPECT_EQ(CheckMultipleAuthz(&voms_info, "cms:/cms\n"), true);
  EXPECT_EQ(CheckMultipleAuthz(&voms_info, "atlas:/atlas"), false);
  EXPECT_EQ(CheckMultipleAuthz(&voms_info, "cms:/cms\natlas"), true);
  EXPECT_EQ(CheckMultipleAuthz(&voms_info, "atlas:/atlas\ncms:/cms"), true);
  EXPECT_EQ(CheckMultipleAuthz(&voms_info, "atlas:/atlas\n\ncms:/cms"), true);
  EXPECT_EQ(CheckMultipleAuthz(
    &voms_info, "atlas:/atlas\n\ndteam:/dteam\n"), false);
  EXPECT_EQ(CheckMultipleAuthz(&voms_info, TEST_DN), false);
  EXPECT_EQ(CheckMultipleAuthz(&voms_info, TEST_DN "\n"), false);
  EXPECT_EQ(CheckMultipleAuthz(
    &voms_info, TEST_DN "\ncms:/cms/Role=prod"), false);
  EXPECT_EQ(CheckMultipleAuthz(
    &voms_info, TEST_DN "\ncms:/cms/Role=pilot"), true);
  voms_entry.user = &user_dn[0];
  EXPECT_EQ(CheckMultipleAuthz(
    &voms_info, TEST_DN "\ncms:/cms/Role=prod"), true);
}
