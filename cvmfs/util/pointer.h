/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UTIL_POINTER_H_
#define CVMFS_UTIL_POINTER_H_

#include <cstdlib>

#include "util/single_copy.h"

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

/**
 * Type Trait:
 * "Static" assertion that a template parameter is a pointer
 */
template<typename T>
struct IsPointer {
  static const bool value = false;
};
template<typename T>
struct IsPointer<T *> {
  static const bool value = true;
};


template<class T, class DerivedT>
class UniquePtrBase : SingleCopy {
 public:
  inline UniquePtrBase() : ref_(NULL) { }
  inline explicit UniquePtrBase(T *ref) : ref_(ref) { }
  inline ~UniquePtrBase() { Free(); }

  inline T *operator->() const { return ref_; }
  // NOLINTNEXTLINE(misc-unconventional-assign-operator)
  inline DerivedT &operator=(T *ref) {
    if (ref_ != ref) {
      Free();
      ref_ = ref;
    }
    return *(static_cast<DerivedT *>(this));
  }
  inline T *weak_ref() const { return ref_; }
  inline bool IsValid() const { return (ref_ != NULL); }
  inline T *Release() {
    T *r = ref_;
    ref_ = NULL;
    return r;
  }
  inline void Destroy() {
    Free();
    ref_ = NULL;
  }

 protected:
  void Free() { static_cast<DerivedT *>(this)->Free(); }
  T *ref_;
};


template<class T>
class std::unique_ptr : public UniquePtrBase<T, std::unique_ptr<T> > {
  friend class UniquePtrBase<T, std::unique_ptr<T> >;

 private:
  typedef UniquePtrBase<T, std::unique_ptr<T> > BaseT;

 public:
  using BaseT::operator=;
  inline UniquePtr() : BaseT(NULL) { }
  inline explicit UniquePtr(T *ref) : BaseT(ref) { }
  inline T &operator*() const { return *BaseT::ref_; }

 protected:
  void Free() { delete BaseT::ref_; }
};


template<>
class std::unique_ptr<void> : public UniquePtrBase<void, std::unique_ptr<void> > {
 private:
  typedef UniquePtrBase<void, std::unique_ptr<void> > BaseT;

 public:
  friend class UniquePtrBase<void, std::unique_ptr<void> >;
  using BaseT::operator=;
  inline UniquePtr() : BaseT(NULL) { }
  inline explicit UniquePtr(void *ref) : BaseT(ref) { }

 protected:
  void Free() {
    if (IsValid()) {
      free(BaseT::ref_);
    }
  }
};

template<>
class std::unique_ptr<unsigned char>
    : public UniquePtrBase<unsigned char, std::unique_ptr<unsigned char> > {
 private:
  typedef UniquePtrBase<unsigned char, std::unique_ptr<unsigned char> > BaseT;

 public:
  friend class UniquePtrBase<unsigned char, std::unique_ptr<unsigned char> >;
  using BaseT::operator=;
  inline UniquePtr() : BaseT(NULL) { }
  inline explicit UniquePtr(unsigned char *ref) : BaseT(ref) { }

 protected:
  void Free() {
    if (IsValid()) {
      free(BaseT::ref_);
    }
  }
};


#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif

#endif  // CVMFS_UTIL_POINTER_H_
