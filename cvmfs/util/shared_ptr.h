/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UTIL_SHARED_PTR_H_
#define CVMFS_UTIL_SHARED_PTR_H_

#include <atomic>
#include <cstdint>
#include <cstdlib>

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif  // CVMFS_NAMESPACE_GUARD

template<typename T>
class SharedPtr {
 public:
  typedef T element_type;

  SharedPtr() {  // never throws
    value_ = NULL;
    count_ = NULL;
  }

  template<class Y>
  explicit SharedPtr(Y *p) {
    value_ = static_cast<element_type *>(p);
    count_ = new std::atomic<int64_t>;
    count_.store(1);
  }

  ~SharedPtr() {  // never throws
    if (count_) {
      count_.fetch_sub(1);
      if (count_.load() == 0) {
        delete value_;
        delete count_;
      }
    }
  }

  SharedPtr(SharedPtr const &r)
      : value_(r.value_), count_(r.count_) {  // never throws
    if (count_) {
      count_.fetch_add(1);
    }
  }

  template<class Y>
  explicit SharedPtr(SharedPtr<Y> const &r)
      : value_(r.value_), count_(r.count_) {  // never throws
    if (count_) {
      count_.fetch_add(1);
    }
  }

  SharedPtr &operator=(SharedPtr const &r) {  // never throws
    if (this == &r)
      return *this;

    Reset();
    value_ = r.value_;
    count_ = r.count_;
    if (count_) {
      count_.fetch_add(1);
    }
    return *this;
  }

  template<class Y>
  SharedPtr &operator=(SharedPtr<Y> const &r) {  // never throws
    Reset();
    value_ = r.Get();
    count_ = r.GetCountPtr();
    if (count_) {
      count_.fetch_add(1);
    }
    return *this;
  }

  void Reset() {  // never throws
    if (count_) {
      count_.fetch_sub(1);
      if (count_.load() == 0) {
        delete value_;
        delete count_;
      }
      value_ = NULL;
      count_ = NULL;
    }
  }

  template<class Y>
  void Reset(Y *p) {
    Reset();
    value_ = static_cast<element_type *>(p);
    count_ = new std::atomic<int64_t>;
    count_.store(1);
  }

  T &operator*() const {  // never throws
    return *value_;
  }

  T *operator->() const {  // never throws
    return value_;
  }

  element_type *Get() const {  // never throws
    return value_;
  }

  std::atomic<int64_t> *GetCountPtr() const { return count_; }

  bool Unique() const {  // never throws
    return count_ && (count_.load() == 1);
  }

  int64_t UseCount() const {  // never throws
    return count_ ? count_.load() : -1;
  }

 private:
  element_type *value_;
  std::atomic<int64_t> *count_;
};

template<class T, class U>
bool operator==(SharedPtr<T> const &a,
                SharedPtr<U> const &b) {  // never throws
  return a.value_ == b.value_;
}

template<class T, class U>
bool operator!=(SharedPtr<T> const &a,
                SharedPtr<U> const &b) {  // never throws
  return a.value_ != b.value_;
}

template<class T, class U>
bool operator<(SharedPtr<T> const &a, SharedPtr<U> const &b) {  // never throws
  return a.value_ < b.value_;
}

template<class T>
typename SharedPtr<T>::element_type *GetPointer(
    SharedPtr<T> const &p) {  // never throws
  return p.value_;
}

template<class T, class U>
SharedPtr<T> StaticPointerCast(SharedPtr<U> const &r) {  // never throws
  return SharedPtr<T>(static_cast<T *>(r.value_));
}

template<class T, class U>
SharedPtr<T> ConstPointerCast(SharedPtr<U> const &r) {  // never throws
  return SharedPtr<T>(const_cast<T *>(r.value_));
}

template<class T, class U>
SharedPtr<T> DynamicPointerCast(SharedPtr<U> const &r) {  // never throws
  return SharedPtr<T>(dynamic_cast<T *>(r.value_));
}

template<class T, class U>
SharedPtr<T> ReinterpretPointerCast(SharedPtr<U> const &r) {  // never throws
  return SharedPtr<T>(reinterpret_cast<T *>(r.value_));
}

#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif

#endif  // CVMFS_UTIL_SHARED_PTR_H_
