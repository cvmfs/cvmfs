#ifndef SHORTSTRING_H_
#define SHORTSTRING_H_

#include <cstring>
#include <string>

template<unsigned char StackSize>
class ShortString {
 public:
  ShortString() : length_(0), long_string_(NULL) { }
  ShortString(const ShortString &other) {
    long_string_ = NULL;
    Assign(other);
  }
  ShortString(const char *chars, const unsigned length) {
    long_string_ = NULL;
    Assign(chars, length);
  }
  ShortString & operator= (const ShortString & other) {
    if (this != &other)
      Assign(other);
    return *this;
  }
  ~ShortString() { delete long_string_; }

  void Assign(const char *chars, const unsigned length) {
    delete long_string_;
    long_string_ = NULL;
    if (length > StackSize) {
      long_string_ = new std::string(chars, length);
    } else {
      if (length)
        memcpy(stack_, chars, length);
      this->length_ = length;
    }
  }

  void Assign(const ShortString &other) {
    if (other.long_string_) {
      Assign(&((*other.long_string_)[0]), other.long_string_->length());
    } else {
      Assign(other.stack_, other.length_);
    }
  }

  void Append(const ShortString &other) {
    Append(other.stack_, other.length_);
  }

  void Append(const char *chars, const unsigned length) {
    if (long_string_) {
      long_string_->append(chars, length);
      return;
    }

    const unsigned new_length = this->length_ + length;
    if (new_length > StackSize) {
      long_string_ = new std::string();
      long_string_->reserve(new_length);
      long_string_->assign(stack_, length_);
      long_string_->append(chars, length);
      return;
    }
    memcpy(&stack_[this->length_], chars, length);
    this->length_ = new_length;
  }

  const char *GetChars() const {
    if (long_string_)
      return long_string_->c_str();
    else
      return stack_;
  }

  unsigned GetLength() const {
    if (long_string_)
      return long_string_->length();
    return length_;
  }

  unsigned IsEmpty() const {
    if (long_string_)
      return long_string_->empty();
    return length_ == 0;
  }

  const char *c_str() const {
    if (long_string_)
      return long_string_->c_str();

    char *c = (char *)stack_ + length_;
    *c = '\0';
    return stack_;
  }

 private:
  unsigned char length_;
  std::string *long_string_;
  char stack_[StackSize+1];
};

#endif  // SHORTSTRING_H_
