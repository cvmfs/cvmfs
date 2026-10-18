/**
 * This file is part of the CernVM File System.
 *
 * Minimal XDR (RFC 4506) encoder/decoder for the built-in NFS server.
 */

#ifndef CVMFS_NFS4_XDR_H_
#define CVMFS_NFS4_XDR_H_

#include <stdint.h>

#include <cstddef>
#include <string>
#include <vector>

namespace nfs4 {

/**
 * Reads XDR items from a borrowed buffer.  Errors are sticky: after the first
 * short read every getter returns false and ok() stays false, so callers can
 * decode a whole structure and check once.
 */
class XdrReader {
 public:
  XdrReader(const unsigned char *data, size_t size)
      : data_(data), size_(size), pos_(0), ok_(true) { }

  bool GetU32(uint32_t *v);
  bool GetI32(int32_t *v);
  bool GetU64(uint64_t *v);
  bool GetI64(int64_t *v);
  bool GetBool(bool *v);
  /** Variable-length opaque<max_len>; max_len 0 means unbounded. */
  bool GetOpaque(std::string *v, uint32_t max_len = 0);
  bool GetFixedOpaque(unsigned char *v, size_t len);
  bool GetString(std::string *v, uint32_t max_len = 0) {
    return GetOpaque(v, max_len);
  }
  bool Skip(size_t len);

  bool ok() const { return ok_; }
  size_t pos() const { return pos_; }
  size_t remaining() const { return size_ - pos_; }
  const unsigned char *cursor() const { return data_ + pos_; }

 private:
  bool Take(size_t len);

  const unsigned char *data_;
  size_t size_;
  size_t pos_;
  bool ok_;
};


class XdrWriter {
 public:
  XdrWriter() { }

  void PutU32(uint32_t v);
  void PutI32(int32_t v) { PutU32(static_cast<uint32_t>(v)); }
  void PutU64(uint64_t v);
  void PutI64(int64_t v) { PutU64(static_cast<uint64_t>(v)); }
  void PutBool(bool v) { PutU32(v ? 1 : 0); }
  void PutOpaque(const void *data, size_t len);
  void PutOpaque(const std::string &v) { PutOpaque(v.data(), v.size()); }
  void PutFixedOpaque(const void *data, size_t len);
  void PutString(const std::string &v) { PutOpaque(v); }
  /** Raw bytes, already XDR encoded. */
  void PutRaw(const void *data, size_t len);

  /** Reserves a u32 slot that can be filled in later via PatchU32. */
  size_t ReserveU32();
  void PatchU32(size_t offset, uint32_t v);

  size_t size() const { return buf_.size(); }
  void Truncate(size_t size) { buf_.resize(size); }
  const std::string &buffer() const { return buf_; }
  std::string *mutable_buffer() { return &buf_; }

 private:
  std::string buf_;
};

}  // namespace nfs4

#endif  // CVMFS_NFS4_XDR_H_
