/**
 * This file is part of the CernVM File System.
 */


#include "xattr.h"

#include <alloca.h>
// clang-format off
#include <sys/xattr.h>
// clang-format on

#include <cassert>
#include <cstring>

#include "util/platform.h"
#include "util/pointer.h"
#include "util/smalloc.h"
#include "util/string.h"

using namespace std;  // NOLINT

const uint8_t XattrList::kVersionSmall = 1;
const uint8_t XattrList::kVersionBig = 2;  // As of cvmfs 2.14

/**
 * Converts all the extended attributes of path into a XattrList.  Attributes
 * that violate the XattrList restrictions are ignored.  If path does not exist
 * or on I/O errors, NULL is returned.  The list of extended attributes is not
 * supposed to change during the runtime of this method.  The list of values
 * must not exceed 64kB.
 */
XattrList *XattrList::CreateFromFile(const std::string &path) {
  // Parse the \0 separated list of extended attribute keys
  char *list;
  ssize_t sz_list = platform_llistxattr(path.c_str(), NULL, 0);
  if ((sz_list < 0) || (sz_list > 64 * 1024)) {
    return NULL;
  } else if (sz_list == 0) {
    // No extended attributes
    return new XattrList();
  }
  list = reinterpret_cast<char *>(alloca(sz_list));
  sz_list = platform_llistxattr(path.c_str(), list, sz_list);
  if (sz_list < 0) {
    return NULL;
  } else if (sz_list == 0) {
    // Can only happen if the list was removed since the previous call to
    // llistxattr
    return new XattrList();
  }
  vector<string> keys = SplitString(string(list, sz_list), '\0');

  // Retrieve extended attribute values
  XattrList *result = new XattrList();
  char value_smallbuf[255];
  for (unsigned i = 0; i < keys.size(); ++i) {
    if (keys[i].empty())
      continue;

    char *buffer = value_smallbuf;
    size_t sz_buffer = 255;
    ssize_t sz_value = platform_lgetxattr(path.c_str(), keys[i].c_str(), buffer,
                                          sz_buffer);
    // check if we need to allocate bigger buffer
    if ((sz_value < 0) && (errno == ERANGE)) {
      // query lgetxattr with size 0 to get proper buffer size
      sz_value = platform_lgetxattr(path.c_str(), keys[i].c_str(), buffer, 0);
      if (buffer != value_smallbuf)
        free(buffer);
      sz_buffer = sz_value;
      buffer = reinterpret_cast<char *>(smalloc(sz_buffer));
      sz_value = platform_lgetxattr(path.c_str(), keys[i].c_str(), buffer,
                                    sz_buffer);
    }
    if (sz_value >= 0)
      result->Set(keys[i], string(buffer, sz_value));
    if (buffer != value_smallbuf)
        free(buffer);
  }
  return result;
}


XattrList *XattrList::Deserialize(const unsigned char *inbuf,
                                  const unsigned size) {
  if (inbuf == NULL)
    return new XattrList();

  UniquePtr<XattrList> result(new XattrList());
  if (size < sizeof(XattrHeader))
    return NULL;
  XattrHeader header;
  memcpy(&header, inbuf, sizeof(header));
  if (!IsSupportedVersion(header.version))
    return NULL;

  XattrEntrySerializer entry_serializer(header.version);
  unsigned char *bufpos = const_cast<unsigned char *>(inbuf);
  unsigned remain = size;
  bufpos += sizeof(XattrHeader);
  remain -= sizeof(XattrHeader);

  for (unsigned i = 0; i < header.num_xattrs; ++i) {
    std::string key;
    std::string value;
    const uint32_t nbytes =
      entry_serializer.Deserialize(bufpos, remain, &key, &value);
    if (nbytes == 0)
      return NULL;
    const bool retval = result->Set(key, value);
    if (!retval)
      return NULL;

    remain -= nbytes;
    bufpos += nbytes;
  }
  return result.Release();
}


bool XattrList::Has(const string &key) const {
  return xattrs_.find(key) != xattrs_.end();
}


bool XattrList::Get(const string &key, string *value) const {
  assert(value);
  const map<string, string>::const_iterator iter = xattrs_.find(key);
  if (iter != xattrs_.end()) {
    *value = iter->second;
    return true;
  }
  return false;
}


vector<string> XattrList::ListKeys() const {
  vector<string> result;
  for (map<string, string>::const_iterator i = xattrs_.begin(),
                                           iEnd = xattrs_.end();
       i != iEnd;
       ++i) {
    result.push_back(i->first);
  }
  return result;
}


/**
 * The format of extended attribute lists in the (l)listxattr call is an array
 * of all the keys concatenated and separated by null characters.  If merge_with
 * is not empty, the final list will be have the keys from the XattrList and the
 * keys from merge_with without duplicates.  The merge_with list is supposed to
 * be in POSIX format.
 */
string XattrList::ListKeysPosix(const string &merge_with) const {
  string result;
  if (!merge_with.empty()) {
    vector<string> merge_list = SplitString(merge_with, '\0');
    for (unsigned i = 0; i < merge_list.size(); ++i) {
      if (merge_list[i].empty())
        continue;
      if (xattrs_.find(merge_list[i]) == xattrs_.end()) {
        result += merge_list[i];
        result.push_back('\0');
      }
    }
  }
  for (map<string, string>::const_iterator i = xattrs_.begin(),
                                           iEnd = xattrs_.end();
       i != iEnd;
       ++i) {
    result += i->first;
    result.push_back('\0');
  }
  return result;
}


bool XattrList::Set(const string &key, const string &value) {
  if (key.empty())
    return false;
  if (key.length() > 255)
    return false;
  if (key.find('\0') != string::npos)
    return false;
  if (value.length() >= 64 * 1024)
    return false;

  const map<string, string>::iterator iter = xattrs_.find(key);
  if (iter != xattrs_.end()) {
    iter->second = value;
  } else {
    if (xattrs_.size() >= 256)
      return false;
    xattrs_[key] = value;
  }
  return true;
}


bool XattrList::Remove(const string &key) {
  const map<string, string>::iterator iter = xattrs_.find(key);
  if (iter != xattrs_.end()) {
    xattrs_.erase(iter);
    return true;
  }
  return false;
}


/**
 * If the list of attributes is empty, Serialize returns NULL.  Deserialize
 * can deal with NULL pointers.
 */
void XattrList::Serialize(unsigned char **outbuf,
                          unsigned *size,
                          const std::vector<std::string> *blacklist) const {
  if (xattrs_.empty()) {
    *size = 0;
    *outbuf = NULL;
    return;
  }

  XattrHeader header;
  *size = sizeof(header);

  for (map<string, string>::const_iterator it_att = xattrs_.begin(),
       it_att_end = xattrs_.end(); it_att != it_att_end; ++it_att)
  {
    *size += it_att->first.length();
    *size += it_att->second.length();
    if (it_att->second.length() > 255)
      header.version = kVersionBig;
  }

  XattrEntrySerializer entry_serializer(header.version);
  *size += xattrs_.size() * entry_serializer.GetHeaderSize();
  *outbuf = reinterpret_cast<unsigned char *>(smalloc(*size));
  unsigned char *bufpos = *outbuf;

  // We copy the header at the end when we know the actual number of entries
  bufpos += sizeof(header);

  header.num_xattrs = 0;
  for (map<string, string>::const_iterator it_att = xattrs_.begin(),
                                           it_att_end = xattrs_.end();
       it_att != it_att_end;
       ++it_att) {
    // Only serialize non-blacklist items
    if (blacklist != NULL) {
      bool skip = false;
      for (unsigned i_bl = 0; i_bl < blacklist->size(); ++i_bl) {
        if (HasPrefix(it_att->first, (*blacklist)[i_bl],
                      true /* ignore_case */)) {
          skip = true;
          break;
        }
      }
      if (skip)
        continue;
    }

    bufpos += entry_serializer.Serialize(it_att->first, it_att->second, bufpos);
    header.num_xattrs++;
  }

  // We might have skipped all attributes
  if (header.num_xattrs == 0) {
    free(*outbuf);
    *size = 0;
    *outbuf = NULL;
  } else {
    memcpy(*outbuf, &header, sizeof(header));
  }
}


//------------------------------------------------------------------------------

XattrList::XattrEntrySerializer::XattrEntrySerializer(uint8_t version)
  : version_(version)
{
  assert(version_ == kVersionSmall || version_ == kVersionBig);
}

uint32_t XattrList::XattrEntrySerializer::Serialize(
  const std::string &key, const std::string &value, unsigned char *to)
{
  assert(key.size() < 256);
  assert(value.size() < ((version_ == kVersionSmall) ? 256 : 64 * 1024));

  const uint8_t len_key = key.size();
  memcpy(to, &len_key, 1);
  to += 1;

  if (version_ == kVersionSmall) {
    const uint8_t len_value = value.size();
    memcpy(to, &len_value, 1);
    to += 1;
  } else {
    assert(version_ == kVersionBig);
    const uint16_t len_value = platform_htole16(value.size());
    memcpy(to, &len_value, 2);
    to += 2;
  }

  memcpy(to, key.data(), key.size());
  to += key.size();
  memcpy(to, value.data(), value.size());
  to += value.size();

  return GetHeaderSize() + key.size() + value.size();
}

uint32_t XattrList::XattrEntrySerializer::Deserialize(
  const unsigned char *from, uint32_t bufsize,
  std::string *key, std::string *value)
{
  if (bufsize < GetHeaderSize())
    return 0;
  bufsize -= GetHeaderSize();

  uint8_t len_key;
  memcpy(&len_key, from, 1);
  key->resize(len_key);
  from += 1;

  if (version_ == kVersionSmall) {
    uint8_t len_value;
    memcpy(&len_value, from, 1);
    value->resize(len_value);
    from += 1;
  } else {
    assert(version_ == kVersionBig);
    uint16_t len_value;
    memcpy(&len_value, from, 2);
    value->resize(platform_le16toh(len_value));
    from += 2;
  }


  if (bufsize < key->size() + value->size())
    return 0;

  memcpy(const_cast<char *>(key->data()), from, key->size());
  from += key->size();
  memcpy(const_cast<char *>(value->data()), from, value->size());

  return GetHeaderSize() + key->size() + value->size();
}

string XattrList::XattrEntry::GetKey() const {
  if (len_key == 0)
    return "";
  return string(data, len_key);
}


uint16_t XattrList::XattrEntry::GetSize() const {
  return sizeof(len_key) + sizeof(len_value) + uint16_t(len_key)
         + uint16_t(len_value);
}


string XattrList::XattrEntry::GetValue() const {
  if (len_value == 0)
    return "";
  return string(&data[len_key], len_value);
}


XattrList::XattrEntry::XattrEntry(const string &key, const string &value)
    : len_key(key.size()), len_value(value.size()) {
  memcpy(data, key.data(), len_key);
  memcpy(data + len_key, value.data(), len_value);
}
