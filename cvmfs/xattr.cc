/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "xattr.h"

#include <alloca.h>
#include <sys/xattr.h>

#include <cassert>
#include <cstring>

#include "platform.h"
#include "smalloc.h"
#include "util.h"

using namespace std;  // NOLINT

const uint8_t XattrList::kVersion = 1;

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
  if ((sz_list < 0) || (sz_list > 64*1024)) {
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
  char value[256];
  for (unsigned i = 0; i < keys.size(); ++i) {
    if (keys[i].empty())
      continue;
    ssize_t sz_value =
      platform_lgetxattr(path.c_str(), keys[i].c_str(), value, 256);
    if (sz_value < 0)
      continue;
    result->Set(keys[i], string(value, sz_value));
  }
  return result;
}


XattrList *XattrList::Deserialize(
  const unsigned char *inbuf,
  const unsigned size)
{
  if (inbuf == NULL)
    return new XattrList();

  UniquePtr<XattrList> result(new XattrList());
  if (size < sizeof(XattrHeader))
    return NULL;
  XattrHeader header;
  memcpy(&header, inbuf, sizeof(header));
  if (header.version != kVersion)
    return NULL;
  unsigned pos = sizeof(header);
  for (unsigned i = 0; i < header.num_xattrs; ++i) {
    XattrEntry entry;
    unsigned size_preamble = sizeof(entry.len_key) + sizeof(entry.len_value);
    if (size - pos < size_preamble)
      return NULL;
    memcpy(&entry, inbuf + pos, size_preamble);
    if (size - pos < entry.GetSize())
      return NULL;
    if (entry.GetSize() == size_preamble)
      return NULL;
    pos += size_preamble;
    memcpy(entry.data, inbuf + pos, entry.GetSize() - size_preamble);
    pos += entry.GetSize() - size_preamble;
    bool retval = result->Set(entry.GetKey(), entry.GetValue());
    if (!retval)
      return NULL;
  }
  return result.Release();
}


bool XattrList::Has(const string &key) const {
  return xattrs_.find(key) != xattrs_.end();
}


bool XattrList::Get(const string &key, string *value) const {
  assert(value);
  map<string, string>::const_iterator iter = xattrs_.find(key);
  if (iter != xattrs_.end()) {
    *value = iter->second;
    return true;
  }
  return false;
}


vector<string> XattrList::ListKeys() const {
  vector<string> result;
  for (map<string, string>::const_iterator i = xattrs_.begin(),
       iEnd = xattrs_.end(); i != iEnd; ++i)
  {
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
       iEnd = xattrs_.end(); i != iEnd; ++i)
  {
    result += i->first;
    result.push_back('\0');
  }
  return result;
}


bool XattrList::Set(const string &key, const string &value) {
  if (key.empty())
    return false;
  if (key.length() > 256)
    return false;
  if (key.find('\0') != string::npos)
    return false;
  if (value.length() > 256)
    return false;

  map<string, string>::iterator iter = xattrs_.find(key);
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
  map<string, string>::iterator iter = xattrs_.find(key);
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
void XattrList::Serialize(unsigned char **outbuf, unsigned *size) const {
  if (xattrs_.empty()) {
    *size = 0;
    *outbuf = NULL;
    return;
  }

  XattrHeader header(xattrs_.size());
  uint32_t packed_size = sizeof(header);

  // Determine size of the buffer
  XattrEntry *entries = reinterpret_cast<XattrEntry *>(
    smalloc(header.num_xattrs * sizeof(XattrEntry)));
  unsigned ientries = 0;
  for (map<string, string>::const_iterator i = xattrs_.begin(),
       iEnd = xattrs_.end(); i != iEnd; ++i, ientries++)
  {
    /*entries[ientries] =*/
    new (entries + ientries) XattrEntry(i->first, i->second);
    packed_size += entries[ientries].GetSize();
  }

  // Copy data into buffer
  *size = packed_size;
  *outbuf = reinterpret_cast<unsigned char *>(smalloc(packed_size));
  memcpy(*outbuf, &header, sizeof(header));
  unsigned pos = sizeof(header);
  for (unsigned i = 0; i < header.num_xattrs; ++i) {
    memcpy(*outbuf + pos, &entries[i], entries[i].GetSize());
    pos += entries[i].GetSize();
  }

  free(entries);
}


//------------------------------------------------------------------------------


string XattrList::XattrEntry::GetKey() const {
  if (len_key == 0)
    return "";
  return string(data, len_key);
}


uint16_t XattrList::XattrEntry::GetSize() const {
  return sizeof(len_key) + sizeof(len_value) +
         uint16_t(len_key) + uint16_t(len_value);
}


string XattrList::XattrEntry::GetValue() const {
  if (len_value == 0)
    return "";
  return string(&data[len_key], len_value);
}


XattrList::XattrEntry::XattrEntry(const string &key, const string &value)
  : len_key(key.size())
  , len_value(value.size())
{
  memcpy(data, key.data(), len_key);
  memcpy(data+len_key, value.data(), len_value);
}
