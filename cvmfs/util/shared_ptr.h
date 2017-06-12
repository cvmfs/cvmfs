/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UTIL_SHARED_POINTER_H_
#define CVMFS_UTIL_SHARED_POINTER_H_

#include "atomic.h"

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif  // CVMFS_NAMESPACE_GUARD

template <typename T>
class WeakPtr;

template <typename T>
class SharedPtr {
 public:
  typedef T element_type;

  SharedPtr() {  // never throws
    value_ = NULL;
    count_ = NULL;
  }

  template <class Y>
  explicit SharedPtr(Y* p) {
    if (p == value_) {
      abort();
    }
    value_ = static_cast<T*>(p);
    count_ = new atomic_int64;
    atomic_write64(count_, 1);
  }

  ~SharedPtr() {  // never throws
    if (count_) {
      atomic_dec64(count_);
      if (*count_ < 1) {
        delete value_;
        delete count_;
      }
    }
  }

  SharedPtr(SharedPtr const& r)
      : value_(r.value_), count_(r.count_) {  // never throws
    if (count_) {
      atomic_inc64(count_);
    }
  }

  template <class Y>
  SharedPtr(SharedPtr<Y> const& r)
      : value_(r.value_), count_(r.count_) {  // never throws
    if (count_) {
      atomic_inc64(count_);
    }
  }

  template <class Y>
  explicit SharedPtr(WeakPtr<Y> const& r) : value_(r.get()), count_(r.count_) {
    if (r.UseCount() == 0) {
      abort();
    }
    atomic_inc64(count_);
  }

  SharedPtr& operator=(SharedPtr const& r) {  // never throws
    value_ = r.value_;
    count_ = r.count_;
    if (count_) {
      atomic_inc64(count_);
    }
    return *this;
  }

  template <class Y>
  SharedPtr& operator=(SharedPtr<Y> const& r) {  // never throws
    value_ = r.value_;
    count_ = r.count_;
    if (count_) {
      atomic_inc64(count_);
    }
    return *this;
  }

  void Reset() {  // never throws
    if (*count_ == 1) {
      delete value_;
      delete count_;
    }
    value_ = NULL;
    count_ = NULL;
  }

  template <class Y>
  void Reset(Y* p) {
    if (*count_ == 1) {
      delete value_;
      delete count_;
    }
    value_ = static_cast<element_type*>(p);
    count_ = new atomic_int64;
    atomic_write64(count_, 1);
  }

  T& operator*() const {  // never throws
    return *value_;
  }

  T* operator->() const {  // never throws
    return value_;
  }

  element_type& operator[](std::ptrdiff_t i) const {  // never throws
    return value_[i];
  }

  element_type* Get() const {  // never throws
    return value_;
  }

  bool Unique() const {  // never throws
    return count_ && (*count_ == 1);
  }

  long UseCount() const {  // never throws
    return count_ ? *count_ : -1;
  }

  operator void*() const {  // never throws
    return static_cast<void*>(value_);
  }

  void Swap(SharedPtr& b) {  // never throws
    element_type* v = value_;
    value_ = b.value_;
    b.value_ = v;
    atomic_int64* c = count_;
    count_ = b.count_;
    b.count_ = c;
  }

 private:
  element_type* value_;
  atomic_int64* count_;
};

template <class T, class U>
bool operator==(SharedPtr<T> const& a,
                SharedPtr<U> const& b) {  // never throws
  return a.value_ == b.value_;
}

template <class T, class U>
bool operator!=(SharedPtr<T> const& a,
                SharedPtr<U> const& b) {  // never throws
  return a.value_ != b.value_;
}

template <class T, class U>
bool operator<(SharedPtr<T> const& a, SharedPtr<U> const& b) {  // never throws
  return a.value_ < b.value_;
}

template <class T>
bool operator==(SharedPtr<T> const& p, void* q) {  // never throws
  return p.value_ == q;
}

template <class T>
bool operator==(void* q, SharedPtr<T> const& p) {  // never throws
  return p.value_ == q;
}

template <class T>
bool operator!=(SharedPtr<T> const& p, void* q) {  // never throws
  return p.value_ != q;
}

template <class T>
bool operator!=(void* q, SharedPtr<T> const& p) {  // never throws
  return p.value_ != q;
}

template <class T>
void Swap(SharedPtr<T>& a, SharedPtr<T>& b) {  // never throws
  a.Swap(b);
}

template <class T>
typename SharedPtr<T>::element_type* GetPointer(
    SharedPtr<T> const& p) {  // never throws
  return p.value_;
}

template <class T, class U>
SharedPtr<T> StaticPointerCast(SharedPtr<U> const& r) {  // never throws
}

template <class T, class U>
SharedPtr<T> ConstPointerCast(SharedPtr<U> const& r) {  // never throws
}

template <class T, class U>
SharedPtr<T> DynamicPointerCast(SharedPtr<U> const& r) {  // never throws
}

template <class T, class U>
SharedPtr<T> ReinterpretPointerCast(SharedPtr<U> const& r) {  // never throws
}

template <class E, class T, class Y>
std::basic_ostream<E, T>& operator<<(std::basic_ostream<E, T>& os,
                                     SharedPtr<Y> const& p) {
  os << *(p.value_);
  return os;
}

#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif

#endif  // CVMFS_UTIL_SHARED_POINTER_H_
