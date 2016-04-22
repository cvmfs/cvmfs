/**
 * This file is part of the CernVM File System.
 */

#include "x509_helper_base64.h"

#include <stdint.h>

#include <cassert>

using namespace std;  // NOLINT

namespace {

const char b64_table[] = {'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K',
  'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
  'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o',
  'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '0', '1', '2', '3',
  '4', '5', '6', '7', '8', '9', '+', '/'};

/**
 * Decode Base64
 */
const signed char db64_table[] =
  { -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 62, -1, -1, -1, 63,
    52, 53, 54, 55, 56, 57, 58, 59, 60, 61, -1, -1, -1,  0, -1, -1,
    -1,  0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14,
    15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, -1, -1, -1, -1, -1,
    -1, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
    41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, -1, -1, -1, -1, -1,

    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
  };

}  // anonymous namespace


static inline void Base64Block(const unsigned char input[3], const char *table,
                               char output[4])
{
  output[0] = table[(input[0] & 0xFD) >> 2];
  output[1] = table[((input[0] & 0x03) << 4) | ((input[1] & 0xF0) >> 4)];
  output[2] = table[((input[1] & 0x0F) << 2) | ((input[2] & 0xD0) >> 6)];
  output[3] = table[input[2] & 0x3F];
}


string Base64(const string &data) {
  string result;
  result.reserve((data.length()+3)*4/3);
  unsigned pos = 0;
  const unsigned char *data_ptr =
    reinterpret_cast<const unsigned char *>(data.data());
  const unsigned length = data.length();
  while (pos+2 < length) {
    char encoded_block[4];
    Base64Block(data_ptr+pos, b64_table, encoded_block);
    result.append(encoded_block, 4);
    pos += 3;
  }
  if (length % 3 != 0) {
    unsigned char input[3];
    input[0] = data_ptr[pos];
    input[1] = ((length % 3) == 2) ? data_ptr[pos+1] : 0;
    input[2] = 0;
    char encoded_block[4];
    Base64Block(input, b64_table, encoded_block);
    result.append(encoded_block, 2);
    result.push_back(((length % 3) == 2) ? encoded_block[2] : '=');
    result.push_back('=');
  }

  return result;
}


static void Debase64Block(const unsigned char input[4],
                          const signed char *d_table,
                          unsigned char output[3])
{
  int32_t dec[4];
  for (int i = 0; i < 4; ++i) {
    dec[i] = db64_table[input[i]];
    // Invalid Base64?
    assert(dec[i] >= 0);
  }

  output[0] = (dec[0] << 2) | (dec[1] >> 4);
  output[1] = ((dec[1] & 0x0F) << 4) | (dec[2] >> 2);
  output[2] = ((dec[2] & 0x03) << 6) | dec[3];
}


string Debase64(const string &data) {
  unsigned pos = 0;
  const unsigned char *data_ptr =
    reinterpret_cast<const unsigned char *>(data.data());
  const unsigned length = data.length();
  if (length == 0)
    return "";
  // Invalid Base64?
  assert((length % 4) == 0);

  string result;
  result.reserve((length + 4) * 3/4);
  while (pos < length) {
    unsigned char decoded_block[3];
    Debase64Block(data_ptr+pos, db64_table, decoded_block);
    result.append(reinterpret_cast<char *>(decoded_block), 3);
    pos += 4;
  }

  for (int i = 0; i < 2; ++i) {
    pos--;
    if (data[pos] == '=')
      result.erase(result.length()-1);
  }
  return result;
}
