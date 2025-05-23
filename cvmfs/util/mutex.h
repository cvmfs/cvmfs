/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UTIL_MUTEX_H_
#define CVMFS_UTIL_MUTEX_H_

#include <pthread.h>

#include "util/single_copy.h"

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

/**
 * Used to allow for static polymorphism in the RAII template to statically
 * decide which 'lock' functions to use, if we have more than one possibility.
 * (I.e. Read/Write locks)
 * Note: Static Polymorphism - Strategy Pattern
 *
 * TODO(jblomer): eventually replace this by C++11 typed enum
 */
struct RAII_Polymorphism {
  enum T {
    None,
    ReadLock,
    WriteLock
  };
};


/**
 * Basic template wrapper class for any kind of RAII-like behavior.
 * The user is supposed to provide a template specialization of Enter() and
 * Leave(). On creation of the RAII object it will call Enter() respectively
 * Leave() on destruction. The gold standard example is a LockGard (see below).
 *
 * Note: Resource Acquisition Is Initialization (Bjarne Stroustrup)
 */
template<typename T, RAII_Polymorphism::T P = RAII_Polymorphism::None>
class RAII : SingleCopy {
 public:
  inline explicit RAII(T &object) : ref_(object) { Enter(); }
  inline explicit RAII(T *object) : ref_(*object) { Enter(); }
  inline ~RAII() { Leave(); }

 protected:
  inline void Enter() { ref_.Lock(); }
  inline void Leave() { ref_.Unlock(); }

 private:
  T &ref_;
};


/**
 * This is a simple scoped lock implementation. Every object that provides the
 * methods Lock() and Unlock() should work with it. Classes that will be used
 * with this template should therefore simply inherit from Lockable.
 *
 * Creating a LockGuard object on the stack will lock the provided object. When
 * the LockGuard runs out of scope it will automatically release the lock. This
 * ensures a clean unlock in a lot of situations!
 *
 * TODO(jblomer): C++11 replace this by a type alias to RAII
 */
template<typename LockableT>
class LockGuard : public RAII<LockableT> {
 public:
  inline explicit LockGuard(LockableT *object) : RAII<LockableT>(object) { }
};


template<>
inline void RAII<pthread_mutex_t>::Enter() {
  pthread_mutex_lock(&ref_);
}
template<>
inline void RAII<pthread_mutex_t>::Leave() {
  pthread_mutex_unlock(&ref_);
}
typedef RAII<pthread_mutex_t> MutexLockGuard;


template<>
inline void RAII<pthread_rwlock_t, RAII_Polymorphism::ReadLock>::Enter() {
  pthread_rwlock_rdlock(&ref_);
}
template<>
inline void RAII<pthread_rwlock_t, RAII_Polymorphism::ReadLock>::Leave() {
  pthread_rwlock_unlock(&ref_);
}
template<>
inline void RAII<pthread_rwlock_t, RAII_Polymorphism::WriteLock>::Enter() {
  pthread_rwlock_wrlock(&ref_);
}
template<>
inline void RAII<pthread_rwlock_t, RAII_Polymorphism::WriteLock>::Leave() {
  pthread_rwlock_unlock(&ref_);
}
typedef RAII<pthread_rwlock_t, RAII_Polymorphism::ReadLock> ReadLockGuard;
typedef RAII<pthread_rwlock_t, RAII_Polymorphism::WriteLock> WriteLockGuard;

#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif

#endif  // CVMFS_UTIL_MUTEX_H_
