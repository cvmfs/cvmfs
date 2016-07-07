/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_XATTR_H_
#define CVMFS_XATTR_H_

#include <inttypes.h>

#include <map>
#include <string>
#include <vector>

/**
 * Represents extended attributes that are maintained by the system or the user
 * and just blindly stored and returned by cvmfs.  Note that cvmfs' magic
 * extended attributes (e.g. user.pid) will hide the ones maintained by the user
 * but they are still present in the file catalogs.
 *
 * First application of the extended attributes is security.capability in order
 * to support POSIX file capabilities.  Cvmfs' support for custom extended
 * attributes is limited to 256 attributes, with names <= 256 characters and
 * values <= 256 bytes.  Thus there is no need for big endian/little endian
 * conversion.  The name must not be the empty string and must not contain the
 * zero character.  There are no restrictions on the content.
 */
class XattrList {
 public:
  static const uint8_t kVersion;

  XattrList() : version_(kVersion) { }
  static XattrList *CreateFromFile(const std::string &path);

  std::vector<std::string> ListKeys() const;
  std::string ListKeysPosix(const std::string &merge_with) const;
  bool Has(const std::string &key) const;
  bool Get(const std::string &key, std::string *value) const;
  bool Set(const std::string &key, const std::string &value);
  bool Remove(const std::string &key);
  bool IsEmpty() const { return xattrs_.empty(); }

  void Serialize(unsigned char **outbuf, unsigned *size) const;
  static XattrList *Deserialize(const unsigned char *inbuf,
                                const unsigned size);

  uint8_t version() { return version_; }

 private:
  struct XattrHeader {
    XattrHeader() : version(kVersion), num_xattrs(0) { }
    explicit XattrHeader(const uint8_t num_xattrs) :
      version(kVersion),
      num_xattrs(num_xattrs)
    { }
    uint8_t version;
    uint8_t num_xattrs;
  };
  struct XattrEntry {
    XattrEntry(const std::string &key, const std::string &value);
    XattrEntry() : len_key(0), len_value(0) { }
    uint16_t GetSize() const;
    std::string GetKey() const;
    std::string GetValue() const;
    uint8_t len_key;
    uint8_t len_value;
    // Concatenate the key the value.  When written out or read in, data is cut
    // off at len_key+len_value
    char data[512];
  };

  uint8_t version_;
  std::map<std::string, std::string> xattrs_;
};

#endif  // CVMFS_XATTR_H_
