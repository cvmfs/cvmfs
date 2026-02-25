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
 * attributes is limited to 256 attributes, with names <= 255 characters and
 * values <= 64k bytes. Note that values > 255 characters require cvmfs
 * version >= 2.14.  Earlier versions will ignore all xattrs for a file
 * if a big one is set. Attribute names must not be the empty string and
 * must not contain the zero character. There are no restrictions on the content.
 */
class XattrList {
 public:
  static const uint8_t kVersionSmall;  ///< Version 1, key and value < 255 chars
  static const uint8_t kVersionBig;    ///< Version 2, value up to 64k chars

  static bool IsSupportedVersion(uint8_t v) {
    return v == kVersionSmall || v == kVersionBig;
  }

  static XattrList *CreateFromFile(const std::string &path);

  std::vector<std::string> ListKeys() const;
  std::string ListKeysPosix(const std::string &merge_with) const;
  bool Has(const std::string &key) const;
  bool Get(const std::string &key, std::string *value) const;
  bool Set(const std::string &key, const std::string &value);
  bool Remove(const std::string &key);
  bool IsEmpty() const { return xattrs_.empty(); }
  void Clear() { xattrs_.clear(); }

  void Serialize(unsigned char **outbuf, unsigned *size,
                 const std::vector<std::string> *blacklist = NULL) const;
  static XattrList *Deserialize(const unsigned char *inbuf,
                                const unsigned size);

 private:
  struct XattrHeader {
    XattrHeader() : version(kVersionSmall), num_xattrs(0) { }
    explicit XattrHeader(const uint8_t num_xattrs)
        : version(kVersionSmall), num_xattrs(num_xattrs) { }
    uint8_t version;
    uint8_t num_xattrs;
  };

  class XattrEntrySerializer {
   public:
    explicit XattrEntrySerializer(uint8_t version);
    uint32_t GetHeaderSize() const { return version_ == kVersionBig ? 3 : 2; }
    uint32_t Serialize(const std::string &key, const std::string &value,
                       unsigned char *to);
    uint32_t Deserialize(const unsigned char *from, uint32_t bufsize,
                         std::string *key, std::string *value);

   private:
     uint8_t version_;
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

  std::map<std::string, std::string> xattrs_;
};

#endif  // CVMFS_XATTR_H_
