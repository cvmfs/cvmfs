/**
 * This file is part of the CernVM File System.
 */

#include "acl.h"

#include <cassert>
#include <cstring>
#include <vector>
#include <algorithm>

#include <string.h>

#include "util/posix.h"

using namespace std;  // NOLINT

#ifdef COMPARE_TO_LIBACL
#include "acl/libacl.h"
#else // COMPARE_TO_LIBACL

// ACL permission bits
#define ACL_READ (0x04)
#define ACL_WRITE (0x02)
#define ACL_EXECUTE (0x01)

// ACL tag types
#define ACL_UNDEFINED_TAG (0x00)
#define ACL_USER_OBJ (0x01)
#define ACL_USER (0x02)
#define ACL_GROUP_OBJ (0x04)
#define ACL_GROUP (0x08)
#define ACL_MASK (0x10)
#define ACL_OTHER (0x20)

// ACL qualifier constants
#define ACL_UNDEFINED_ID ((id_t) - 1)

#endif // COMPARE_TO_LIBACL

#define ACL_EA_VERSION 0x0002

// ACL data structures
struct acl_ea_entry {
  u_int16_t e_tag;
  u_int16_t e_perm;
  u_int32_t e_id;

  // implements sorting compatible with libacl
  bool operator<(const acl_ea_entry& other) const {
    if (e_tag != other.e_tag) {
      return e_tag < other.e_tag;
    }
    return e_id < other.e_id;
  }
};

struct acl_ea_header {
  u_int32_t a_version;
  acl_ea_entry a_entries[0];
};

static int acl_from_text_to_string_entries(const string &acl_string, vector<string> &string_entries)
{
  std::size_t entry_pos = 0;
  while (entry_pos != string::npos) {
    size_t entry_length;
    size_t next_pos;
    size_t const sep_pos = acl_string.find_first_of(",\n", entry_pos);
    if (sep_pos == string::npos) {
      if (acl_string.length() > entry_pos) {
        entry_length = acl_string.length() - entry_pos;
        next_pos = string::npos;
      } else {
        // we've just looked past a trailing delimiter
        break;
      }
    } else {
      assert(sep_pos >= entry_pos);
      entry_length = sep_pos - entry_pos;
      next_pos = sep_pos + 1;
    }
    if (entry_length == 0) {
      // libacl tolerates excessive whitespace but not excessive delimiters.
      // It's simpler for us to treat whitespace as delimiters.
      entry_pos = next_pos;
      continue;
    }
    string entry(acl_string, entry_pos, entry_length);
    entry_pos = next_pos;

    // search for '#'-starting comment, discard if found
    size_t const comment_pos = entry.find('#');
    if (comment_pos != string::npos) {
      entry = string(entry, 0, comment_pos);
    }

    // TODO(autkin): trim whitespace on both ends

    // discard empty lines
    if (entry.length() == 0) {
      continue;
    }

    string_entries.push_back(entry);
  }
  return 0;
}

static int acl_parms_from_text(const string &str, u_int16_t *perms)
{
  // Currently unsupported syntax features found in setfacl:
  // - X (capital x)
  // - numeric syntax
  // See "man 1 setfacl", "The perms field is..."

  *perms = 0;
  for (const char &c : str) {
    switch(c) {
      case 'r': *perms |= ACL_READ; break;
      case 'w': *perms |= ACL_WRITE; break;
      case 'x': *perms |= ACL_EXECUTE; break;
      case '-': break;
      default: return EINVAL;
    }
  }
  return 0;
}

static int acl_entry_from_text(const string &str, acl_ea_entry &entry)
{
  // break down to 3 fields by ':'
  // type:qualifier:permissions according to terminology
  // e_tag:e_id:e_perm are acl_ea_entry field names
  size_t sep_pos = str.find(':');
  if (sep_pos == string::npos) {
    return EINVAL;
  }
  string const type(str, 0, sep_pos);
  size_t next_field_pos = sep_pos + 1;
  sep_pos = str.find(':', next_field_pos);
  if (sep_pos == string::npos) {
    return EINVAL;
  }
  string const qualifier(str, next_field_pos, sep_pos - next_field_pos);
  next_field_pos = sep_pos + 1;
  string const permissions(str, next_field_pos);

  if (!type.compare("user") || !type.compare("u")) {
    entry.e_tag = qualifier.empty() ? ACL_USER_OBJ : ACL_USER;
  } else if (!type.compare("group") || !type.compare("g")) {
    entry.e_tag = qualifier.empty() ? ACL_GROUP_OBJ : ACL_GROUP;
  } else if (!type.compare("other") || !type.compare("o")) {
    entry.e_tag = ACL_OTHER;
  } else if (!type.compare("mask") || !type.compare("m")) {
    entry.e_tag = ACL_MASK;
  } else {
    return EINVAL;
  }
  entry.e_tag = htole16(entry.e_tag);

  if (qualifier.empty()) {
    entry.e_id = ACL_UNDEFINED_ID;
  } else {
    char* at_null_terminator_if_number;
    long number = strtol(qualifier.c_str(), &at_null_terminator_if_number, 10);
    if (*at_null_terminator_if_number != '\0') {
      bool ok;
      if (entry.e_tag == htole16(ACL_USER)) {
        [[maybe_unused]] gid_t main_gid;
        uid_t uid;
        ok = GetUidOf(qualifier, &uid, &main_gid);
        number = uid;
      } else if (entry.e_tag == htole16(ACL_GROUP)) {
        gid_t gid;
        ok = GetGidOf(qualifier, &gid);
        number = gid;
      } else {
        assert(false);
      }
      if (!ok) {
        return EINVAL;
      }
    }
    entry.e_id = htole32(number);
  }

  // parse perms
  u_int16_t host_byteorder_perms;
  int ret;
  ret = acl_parms_from_text(permissions, &host_byteorder_perms);
  if (ret) {
    return ret;
  }
  entry.e_perm = htole16(host_byteorder_perms);

  return 0;
}

static bool acl_valid_builtin(const vector<acl_ea_entry> &entries)
{
  // From man acl_valid:
  // The three required entries ACL_USER_OBJ, ACL_GROUP_OBJ, and ACL_OTHER must
  // exist exactly once in the ACL.
  //
  // If the ACL contains any ACL_USER or ACL_GROUP entries, then an ACL_MASK
  // entry is also required.
  //
  // The ACL may contain at most one ACL_MASK entry.
  //
  // The user identifiers must be unique among all entries of type ACL_USER.
  // The group identifiers must be unique among all entries of type ACL_GROUP.

  bool types_met[ACL_OTHER + 1] = {false, };

  for (auto entry_it = entries.begin(); entry_it != entries.end(); ++entry_it) {
    const acl_ea_entry &e = *entry_it;
    assert(e.e_tag <= ACL_OTHER);
    bool &type_met = types_met[e.e_tag];
    switch (e.e_tag) {
      // at most one of these types
      case ACL_USER_OBJ:
      case ACL_GROUP_OBJ:
      case ACL_OTHER:
      case ACL_MASK:
        if (type_met) {
          return false;
        } else {
          type_met = true;
        }
        break;
      case ACL_USER:
      case ACL_GROUP:
        type_met = true;
        break;
      default:
        assert(false);
    }
  }
  if (!(types_met[ACL_USER_OBJ] && types_met[ACL_GROUP_OBJ] && types_met[ACL_OTHER])) {
    return false;
  }
  if ((types_met[ACL_USER] || types_met[ACL_GROUP]) && !types_met[ACL_MASK]) {
    return false;
  }
  // TODO(autkin): ACL_USER, ACL_GROUP uniqueness checks. Not a pressing issue.
  return true;
}

int acl_from_text_to_xattr_value(const string& textual_acl, char *&o_binary_acl, size_t &o_size, bool &o_equiv_mode)
{
  int ret;

  o_equiv_mode = true;

  // get individual textual entries from one big text
  vector<string> string_entries;
  ret = acl_from_text_to_string_entries(textual_acl, string_entries);
  if (ret) {
    return ret;
  }

  // get individual entries in structural form
  vector<acl_ea_entry> entries;
  for (auto string_it = string_entries.begin(); string_it != string_entries.end(); ++string_it) {
    acl_ea_entry entry;
    ret = acl_entry_from_text(*string_it, entry);
    if (ret) {
      return ret;
    }
    if (entry.e_tag == ACL_GROUP || entry.e_tag == ACL_USER) {
      o_equiv_mode = false;
    }
    entries.push_back(entry);
  }

  // sort entries as libacl does, to be able to use it in testing as a reference
  sort(entries.begin(), entries.end());

  // reject what acl_valid() rejects, to be able to use it in testing
  if (!acl_valid_builtin(entries)) {
    return EINVAL;
  }

  // if nothing but usual u,g,o bits, don't produce a binary. Mimicking libacl.
  if (o_equiv_mode) {
    o_binary_acl = NULL;
    o_size = 0;
    return 0;
  }

  // get one big buffer with all the entries in the "on-disk" xattr format
  size_t const acl_entry_count = entries.size();
  size_t const buf_size = sizeof(acl_ea_header) + (acl_entry_count * sizeof(acl_ea_entry));
  char *buf = static_cast<char*>(malloc(buf_size));
  if (!buf) {
    return ENOMEM;
  }
  acl_ea_header* header = reinterpret_cast<acl_ea_header*>(buf);
  header->a_version = htole32(ACL_EA_VERSION);
  acl_ea_entry* ext_entry = reinterpret_cast<acl_ea_entry*>(header + 1);
  for (auto entry_it = entries.begin(); entry_it != entries.end(); ++entry_it) {
    *ext_entry = *entry_it;
    ext_entry += 1;
  }

  o_binary_acl = buf;
  o_size = buf_size;
  return 0;
}

#ifdef COMPARE_TO_LIBACL
int acl_from_text_to_xattr_value_libacl(const string textual_acl, char *&o_binary_acl, size_t &o_size, bool &o_equiv_mode)
{
  acl_t acl = acl_from_text(textual_acl.c_str());  // Convert ACL string to acl_t object
  if (!acl) {
    return EINVAL;
  }
  if (acl_valid(acl) != 0) {
    acl_free(acl);
    return EINVAL;
  }

  // check if the ACL string contains more than the synthetic ACLs
  int equiv = acl_equiv_mode(acl, NULL);
  assert(equiv != -1);

  o_equiv_mode = equiv == 0;
  if (!o_equiv_mode) {
    o_binary_acl = (char *)acl_to_xattr(acl, &o_size);
  } else {
    o_binary_acl = NULL;
    o_size = 0;
  }
  acl_free(acl);
  return 0;
}

int acl_from_text_to_xattr_value_both_impl(const string textual_acl, char *&o_binary_acl, size_t &o_size, bool &o_equiv_mode)
{
  struct impl_result {
    int ret;
    size_t binary_size;
    char *binary_acl;
    bool equiv_mode;
  } b, l;
  b.ret = acl_from_text_to_xattr_value(textual_acl, b.binary_acl, b.binary_size, b.equiv_mode);
  l.ret = acl_from_text_to_xattr_value_libacl(textual_acl, l.binary_acl, l.binary_size, l.equiv_mode);
  assert(b.ret == l.ret);
  if (!l.ret) {
    assert(b.binary_size == l.binary_size);
    assert(0 == memcmp(b.binary_acl, l.binary_acl, b.binary_size));
    assert(b.equiv_mode == l.equiv_mode);
    free(l.binary_acl);
  }
  o_binary_acl = b.binary_acl;
  o_size = b.binary_size;
  o_equiv_mode = b.equiv_mode;
  return b.ret;
}
#endif // COMPARE_TO_LIBACL
