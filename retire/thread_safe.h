/**
 *  This wrapper object implements a simple approach for a monitored object.
 *  I.e. All method calls to the wrapped object are synchronized by a mutex.
 *
 *  This code is highly inspired by the paper:
 *     Wrapping C++ Member Function Calls
 *     by Bjarne Stroustrup
 *
 *  Usage:
 *    Foo a;
 *    Monitor<Foo> foo(a);
 *    foo->f();
 *
 *  Or:
 *    Monitor<Foo> foo(new Foo);
 *    foo->f();
 *
 *  When passing a pointer to the Monitor, it will be freed automatically
 *  by a simple reference counting approach.
 *
 *  In any case a Monitor will initialize a mutex, lock it before calling
 *  f() and unlock it afterwards. This is done automatically.
 *
 *  written by René Meusel in Potsdam 2011
 */

#ifndef THREAD_SAFE_H
#define THREAD_SAFE_H 1

#include <pthread.h>

namespace cvmfs {

template<class T> class Monitor;

template<class T>
class SuffixProxy {
 private:
  T* p_;
  pthread_mutex_t *mutex_;
  mutable bool own_;
  
  // usual creation
  SuffixProxy(T* pp, pthread_mutex_t *mutexp) : 
      p_(pp), 
      mutex_(mutexp),
      own_(true) {}
  
  // keep track of ownership
  SuffixProxy(const SuffixProxy& a) :
      p_(a.p_),
      mutex_(a.mutex_),
      own_(true) {
    a.own_ = false;
  }
  
  // prevent assignment of these objects
  SuffixProxy& operator=(const SuffixProxy&);
  
 public:
  template<class U> friend class Monitor;

  ~SuffixProxy() {
    if (own_) {
      pthread_mutex_unlock(mutex_);
    }
  }
  
  T* operator->() {
    return p_;
  }
};

template<class T>
class Monitor {
 private:
  T *p_;
  int *owned_;
  mutable bool ownership_;
  pthread_mutex_t *mutex_;
  
 private:
  inline void Init() { 
    owned_ = new int(1);
    mutex_ = new pthread_mutex_t;
    pthread_mutex_init(mutex_, NULL);
  }
   
  inline void IncrementOwned() const {
    ++*owned_;
  }
  
  inline void DecrementOwned() const {
    if (--*owned_ == 0) {
      delete owned_;
      pthread_mutex_destroy(mutex_);
      
      if(ownership_) {
        delete p_;
      }
    }
  }

 public:
  Monitor(T& x) :
      p_(&x),
      ownership_(false) {
    Init();
  }
   
  Monitor(T* pp) : 
      p_(pp),
      ownership_(true) {
    Init();
  }
  
  Monitor(const Monitor& a) :
      p_(a.p_),
      owned_(a.owned_),
      ownership_(a.ownership_),
      mutex_(a.mutex_) {
    IncrementOwned();
  }
  
  Monitor& operator=(const Monitor& a) {
    a.IncrementOwned();
    DecrementOwned();
    p_ = a.p_;
    owned_ = a.owned_;
    ownership_ = a.ownership_;
    mutex_ = a.mutex_;
    return *this;
  }
  
  ~Monitor() {
    DecrementOwned();
  }
  
  SuffixProxy<T> operator->() {
    pthread_mutex_lock(mutex_);
    return SuffixProxy<T>(p_, mutex_);
  }
  
};

}

#endif /* THREAD_SAFE_H */
