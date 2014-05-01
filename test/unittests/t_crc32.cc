#include <gtest/gtest.h>

#include <pthread.h>
#include <string>

#include "../../cvmfs/checksum.h"

using namespace std;  // NOLINT

TEST(T_Crc32, Basics) {
  uint32_t crc32;
  checksum::InitCrc32C(&crc32);
  checksum::UpdateCrc32C(&crc32, NULL, 0);
  checksum::FinalCrc32C(&crc32);
  EXPECT_EQ(crc32, uint32_t(0));

  string test1 = "The quick brown fox jumps over the lazy dog";
  checksum::InitCrc32C(&crc32);
  checksum::UpdateCrc32C(&crc32,
                         (const unsigned char *)test1.data(), test1.length());
  checksum::FinalCrc32C(&crc32);
  EXPECT_EQ(crc32, uint32_t(0x22620404));

  string test2 = "123456789";
  checksum::InitCrc32C(&crc32);
  checksum::UpdateCrc32C(&crc32,
                         (const unsigned char *)test2.data(), test2.length());
  checksum::FinalCrc32C(&crc32);
  EXPECT_EQ(crc32, uint32_t(0xe3069283));
  
  string test2_1 = "12345";
  string test2_2 = "6789";
  checksum::InitCrc32C(&crc32);
  checksum::UpdateCrc32C(&crc32,
                         (const unsigned char *)test2_1.data(), 
                         test2_1.length());
  checksum::UpdateCrc32C(&crc32,
                         (const unsigned char *)test2_2.data(), 
                         test2_2.length());
  checksum::FinalCrc32C(&crc32);
  EXPECT_EQ(crc32, uint32_t(0xe3069283));
}
