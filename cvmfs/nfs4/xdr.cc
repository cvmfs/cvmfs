/**
 * This file is part of the CernVM File System.
 */

#include "nfs4/xdr.h"

#include <cstring>

namespace nfs4 {

namespace {

inline size_t Pad4(size_t len) { return (4 - (len & 3)) & 3; }

}  // anonymous namespace

bool XdrReader::Take(size_t len) {
  if (!ok_ || len > size_ - pos_) {
    ok_ = false;
    return false;
  }
  return true;
}

bool XdrReader::GetU32(uint32_t *v) {
  if (!Take(4))
    return false;
  const unsigned char *p = data_ + pos_;
  *v = (static_cast<uint32_t>(p[0]) << 24) | (static_cast<uint32_t>(p[1]) << 16)
       | (static_cast<uint32_t>(p[2]) << 8) | static_cast<uint32_t>(p[3]);
  pos_ += 4;
  return true;
}

bool XdrReader::GetI32(int32_t *v) {
  uint32_t u;
  if (!GetU32(&u))
    return false;
  *v = static_cast<int32_t>(u);
  return true;
}

bool XdrReader::GetU64(uint64_t *v) {
  uint32_t hi, lo;
  if (!GetU32(&hi) || !GetU32(&lo))
    return false;
  *v = (static_cast<uint64_t>(hi) << 32) | lo;
  return true;
}

bool XdrReader::GetI64(int64_t *v) {
  uint64_t u;
  if (!GetU64(&u))
    return false;
  *v = static_cast<int64_t>(u);
  return true;
}

bool XdrReader::GetBool(bool *v) {
  uint32_t u;
  if (!GetU32(&u))
    return false;
  if (u > 1) {
    ok_ = false;
    return false;
  }
  *v = (u == 1);
  return true;
}

bool XdrReader::GetOpaque(std::string *v, uint32_t max_len) {
  uint32_t len;
  if (!GetU32(&len))
    return false;
  if (max_len != 0 && len > max_len) {
    ok_ = false;
    return false;
  }
  const size_t padded = static_cast<size_t>(len) + Pad4(len);
  if (!Take(padded))
    return false;
  v->assign(reinterpret_cast<const char *>(data_ + pos_), len);
  pos_ += padded;
  return true;
}

bool XdrReader::GetFixedOpaque(unsigned char *v, size_t len) {
  const size_t padded = len + Pad4(len);
  if (!Take(padded))
    return false;
  memcpy(v, data_ + pos_, len);
  pos_ += padded;
  return true;
}

bool XdrReader::Skip(size_t len) {
  if (!Take(len))
    return false;
  pos_ += len;
  return true;
}


void XdrWriter::PutU32(uint32_t v) {
  const char b[4] = {static_cast<char>(v >> 24), static_cast<char>(v >> 16),
                     static_cast<char>(v >> 8), static_cast<char>(v)};
  buf_.append(b, 4);
}

void XdrWriter::PutU64(uint64_t v) {
  PutU32(static_cast<uint32_t>(v >> 32));
  PutU32(static_cast<uint32_t>(v));
}

void XdrWriter::PutOpaque(const void *data, size_t len) {
  PutU32(static_cast<uint32_t>(len));
  PutFixedOpaque(data, len);
}

void XdrWriter::PutFixedOpaque(const void *data, size_t len) {
  buf_.append(static_cast<const char *>(data), len);
  buf_.append(Pad4(len), '\0');
}

void XdrWriter::PutRaw(const void *data, size_t len) {
  buf_.append(static_cast<const char *>(data), len);
}

size_t XdrWriter::ReserveU32() {
  const size_t offset = buf_.size();
  buf_.append(4, '\0');
  return offset;
}

void XdrWriter::PatchU32(size_t offset, uint32_t v) {
  buf_[offset] = static_cast<char>(v >> 24);
  buf_[offset + 1] = static_cast<char>(v >> 16);
  buf_[offset + 2] = static_cast<char>(v >> 8);
  buf_[offset + 3] = static_cast<char>(v);
}

}  // namespace nfs4
