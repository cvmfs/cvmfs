/**
 * This file is part of the CernVM File System.
 */

#include "x509_helper_base64.h"

using namespace std;

namespace {
const char b64_table[] = {'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K',
  'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
  'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o',
  'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '0', '1', '2', '3',
  '4', '5', '6', '7', '8', '9', '+', '/'};
}


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