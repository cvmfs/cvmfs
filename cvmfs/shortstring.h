/**
 * This file is part of the CernVM File System.
 *
 * Implements a string class that stores short strings on the stack and
 * malloc's a std::string on the heap on overflow.  Used for file names and
 * path names that are usually small.
 */

#ifndef CVMFS_SHORTSTRING_H_
#define CVMFS_SHORTSTRING_H_

#include <algorithm>
#include <cstring>
#include <string>

#include "util/atomic.h"

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

const unsigned char kDefaultMaxName = 25;
const unsigned char kDefaultMaxLink = 25;
const unsigned char kDefaultMaxPath = 200;

template<unsigned char StackSize, char Type>
class ShortString {
 public:
  ShortString() : long_string_(NULL), length_(0) {
#ifdef DEBUGMSG
    atomic_inc64(&num_instances_);
#endif
  }
  ShortString(const ShortString &other) : long_string_(NULL) {
#ifdef DEBUGMSG
    atomic_inc64(&num_instances_);
#endif
    Assign(other);
  }
  ShortString(const char *chars, const unsigned length) : long_string_(NULL) {
#ifdef DEBUGMSG
    atomic_inc64(&num_instances_);
#endif
    Assign(chars, length);
  }
  explicit ShortString(const std::string &std_string) : long_string_(NULL) {
#ifdef DEBUGMSG
    atomic_inc64(&num_instances_);
#endif
    Assign(std_string.data(), std_string.length());
  }

  ShortString &operator=(const ShortString &other) {
    if (this != &other)
      Assign(other);
    return *this;
  }

  ~ShortString() { delete long_string_; }

  void Assign(const char *chars, const unsigned length) {
    delete long_string_;
    long_string_ = NULL;
    this->length_ = length;
    if (length > StackSize) {
#ifdef DEBUGMSG
      atomic_inc64(&num_overflows_);
#endif
      long_string_ = new std::string(chars, length);
    } else {
      if (length)
        memcpy(stack_, chars, length);
    }
  }

  void Assign(const ShortString &other) {
    Assign(other.GetChars(), other.GetLength());
  }

  void Append(const char *chars, const unsigned length) {
    if (long_string_) {
      long_string_->append(chars, length);
      return;
    }

    const unsigned new_length = this->length_ + length;
    if (new_length > StackSize) {
#ifdef DEBUGMSG
      atomic_inc64(&num_overflows_);
#endif
      long_string_ = new std::string();
      long_string_->reserve(new_length);
      long_string_->assign(stack_, length_);
      long_string_->append(chars, length);
      return;
    }
    if (length > 0)
      memcpy(&stack_[this->length_], chars, length);
    this->length_ = new_length;
  }

  /**
   * Truncates the current string to be of size smaller or equal to current size
   *
   * Note: Can lead to a heap allocated string that is shorter than
   *       the reserved stack space.
   */
  void Truncate(unsigned new_length) {
    assert(new_length <= this->GetLength());
    if (long_string_) {
      long_string_->erase(new_length);
      return;
    }
    this->length_ = new_length;
  }

  void Clear() {
    delete long_string_;
    long_string_ = NULL;
    length_ = 0;
  }

  const char *GetChars() const {
    if (long_string_) {
      return long_string_->data();
    } else {
      return stack_;
    }
  }

  unsigned GetLength() const {
    if (long_string_)
      return long_string_->length();
    return length_;
  }

  bool IsEmpty() const { return GetLength() == 0; }

  std::string ToString() const {
    return std::string(this->GetChars(), this->GetLength());
  }

  const char *c_str() const {
    if (long_string_)
      return long_string_->c_str();

    char *c = const_cast<char *>(stack_) + length_;
    *c = '\0';
    return stack_;
  }

  bool operator==(const ShortString &other) const {
    const unsigned this_length = this->GetLength();
    const unsigned other_length = other.GetLength();
    if (this_length != other_length)
      return false;
    if (this_length == 0)
      return true;

    return memcmp(this->GetChars(), other.GetChars(), this_length) == 0;
  }

  bool operator!=(const ShortString &other) const { return !(*this == other); }

  bool operator<(const ShortString &other) const {
    const unsigned this_length = this->GetLength();
    const unsigned other_length = other.GetLength();

    if (this_length < other_length)
      return true;
    if (this_length > other_length)
      return false;

    const char *this_chars = this->GetChars();
    const char *other_chars = other.GetChars();
    for (unsigned i = 0; i < this_length; ++i) {
      if (this_chars[i] < other_chars[i])
        return true;
      if (this_chars[i] > other_chars[i])
        return false;
    }
    return false;
  }

  bool StartsWith(const ShortString &other) const {
    const unsigned this_length = this->GetLength();
    const unsigned other_length = other.GetLength();
    if (this_length < other_length)
      return false;

    return memcmp(this->GetChars(), other.GetChars(), other_length) == 0;
  }

  ShortString Suffix(const unsigned start_at) const {
    const unsigned length = this->GetLength();
    if (start_at >= length)
      return ShortString("", 0);

    return ShortString(this->GetChars() + start_at, length - start_at);
  }

  static uint64_t num_instances() { return atomic_read64(&num_instances_); }
  static uint64_t num_overflows() { return atomic_read64(&num_overflows_); }

 private:
  std::string *long_string_;
  char stack_[StackSize + 1];  // +1 to add a final '\0' if necessary
  unsigned char length_;
  static atomic_int64 num_overflows_;
  static atomic_int64 num_instances_;
};  // class ShortString

typedef ShortString<kDefaultMaxPath, 0> PathString;
typedef ShortString<kDefaultMaxName, 1> NameString;
typedef ShortString<kDefaultMaxLink, 2> LinkString;

template<unsigned char StackSize, char Type>
atomic_int64 ShortString<StackSize, Type>::num_overflows_ = 0;
template<unsigned char StackSize, char Type>
atomic_int64 ShortString<StackSize, Type>::num_instances_ = 0;

// See posix.cc for the std::string counterparts
PathString GetParentPath(const PathString &path);
NameString GetFileName(const PathString &path);

bool IsSubPath(const PathString& parent, const PathString& path);


#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif

#endif  // CVMFS_SHORTSTRING_H_
