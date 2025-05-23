/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UTIL_ASYNC_H_
#define CVMFS_UTIL_ASYNC_H_

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

/**
 * Encapsulates a callback function that handles asynchronous responses.
 *
 * This is an abstract base class for two different callback function objects.
 * There are two specializations:
 *  --> 1. for static members or global C-like functions
 *  --> 2. for member functions of arbitrary objects
 */
template<typename ParamT>
class CallbackBase {
 public:
  virtual ~CallbackBase() { }
  virtual void operator()(const ParamT &value) const = 0;
};

template<>
class CallbackBase<void> {
 public:
  virtual ~CallbackBase() { }
  virtual void operator()() const = 0;
};

/**
 * This callback function object can be used to call static members or global
 * functions with the following signature:
 * void <name>(ParamT <parameter>);
 *
 * TODO: One might use varidic templates once C++11 will be supported, in order
 *       to allow for more than one parameter to be passed to the callback.
 *
 * @param ParamT    the type of the parameter to be passed to the callback
 */
template<typename ParamT>
class Callback : public CallbackBase<ParamT> {
 public:
  typedef void (*CallbackFunction)(const ParamT &value);

  explicit Callback(CallbackFunction function) : function_(function) { }
  void operator()(const ParamT &value) const { function_(value); }

 private:
  CallbackFunction function_;
};

template<>
class Callback<void> : public CallbackBase<void> {
 public:
  typedef void (*CallbackFunction)();

  explicit Callback(CallbackFunction function) : function_(function) { }
  void operator()() const { function_(); }

 private:
  CallbackFunction function_;
};


/**
 * A BoundCallback can be used to call a member of an arbitrary object as a
 * callback.
 * The member must have the following interface:
 * void <DelegateT>::<member name>(ParamT <parameter>);
 *
 * Note: it is the responsibility of the user to ensure that the bound object
 *       for `delegate` remains alive in the whole time this callback might be
 *       invoked.
 *
 * @param ParamT      the type of the parameter to be passed to the callback
 * @param DelegateT   the <class name> of the object the member <member name>
 *                    should be invoked in
 */
template<typename ParamT, class DelegateT>
class BoundCallback : public CallbackBase<ParamT> {
 public:
  typedef void (DelegateT::*CallbackMethod)(const ParamT &value);

  BoundCallback(CallbackMethod method, DelegateT *delegate)
      : delegate_(delegate), method_(method) { }

  void operator()(const ParamT &value) const { (delegate_->*method_)(value); }

 private:
  DelegateT *delegate_;
  CallbackMethod method_;
};

template<class DelegateT>
class BoundCallback<void, DelegateT> : public CallbackBase<void> {
 public:
  typedef void (DelegateT::*CallbackMethod)();

  BoundCallback(CallbackMethod method, DelegateT *delegate)
      : delegate_(delegate), method_(method) { }

  void operator()() const { (delegate_->*method_)(); }

 private:
  DelegateT *delegate_;
  CallbackMethod method_;
};


/**
 * A BoundClosure works exactly the same as a BoundCallback (see above) but,
 * allows for an opaque enclosure of an arbitrary chunk of user data on
 * creation. When the closure is invoked the provided user data chunk is
 * passed as a second argument to the specified callback method.
 *
 * Note: delegate must be still around when the closure is invoked!
 *
 * @param ParamT        the type of the parameter to be passed to the callback
 * @param DelegateT     the <class name> of the object the member <member name>
 *                      should be invoked in
 * @param ClosureDataT  the type of the user data chunk to be passed on invoke
 */
template<typename ParamT, class DelegateT, typename ClosureDataT>
class BoundClosure : public CallbackBase<ParamT> {
 public:
  typedef void (DelegateT::*CallbackMethod)(const ParamT &value,
                                            const ClosureDataT closure_data);

 public:
  BoundClosure(CallbackMethod method, DelegateT *delegate, ClosureDataT data)
      : delegate_(delegate), method_(method), closure_data_(data) { }

  void operator()(const ParamT &value) const {
    (delegate_->*method_)(value, closure_data_);
  }

 private:
  DelegateT *delegate_;
  CallbackMethod method_;
  const ClosureDataT closure_data_;
};

template<class DelegateT, typename ClosureDataT>
class BoundClosure<void, DelegateT, ClosureDataT> : public CallbackBase<void> {
 public:
  typedef void (DelegateT::*CallbackMethod)(const ClosureDataT closure_data);

 public:
  BoundClosure(CallbackMethod method, DelegateT *delegate, ClosureDataT data)
      : delegate_(delegate), method_(method), closure_data_(data) { }

  void operator()() const { (delegate_->*method_)(closure_data_); }

 private:
  DelegateT *delegate_;
  CallbackMethod method_;
  const ClosureDataT closure_data_;
};


/**
 * This template contains convenience functions to be inherited by classes pro-
 * viding callback functionality. You can use this as a base class providing the
 * callback parameter your class is going to process.
 * Users of your class can profit from the static MakeCallback resp. MakeClosure
 * methods to generated Callback objects. Later they can easily be passed back
 * to your class.
 *
 * Note: the convenience methods return a pointer to a callback object. The user
 *       is responsible to clean up those pointers in some way. If it is a one-
 *       time callback, it makes sense to delete it inside the inheriting class
 *       once it was invoked.
 *
 * TODO: C++11 - Replace this functionality by lambdas
 *
 * @param ParamT  the parameter type of the callbacks to be called
 */
template<class ParamT>
class Callbackable {
 public:
  typedef CallbackBase<ParamT> CallbackTN;

 public:
  /**
   * Produces a BoundClosure object that captures a closure value passed along
   * to the invoked method.
   *
   * @param method        Function pointer to the method to be called
   * @param delegate      The delegate object on which <method> will be called
   * @param closure_data  The closure data to be passed along to <method>
   */
  template<class DelegateT, typename ClosureDataT>
  static CallbackTN *MakeClosure(
      typename BoundClosure<ParamT, DelegateT, ClosureDataT>::CallbackMethod
          method,
      DelegateT *delegate,
      const ClosureDataT &closure_data) {
    return new BoundClosure<ParamT, DelegateT, ClosureDataT>(
        method, delegate, closure_data);
  }

  /**
   * Produces a BoundCallback object that invokes a method on a delegate object.
   *
   * @param method    Function pointer to the method to be called
   * @param delegate  The delegate object on which <method> will be called
   */
  template<class DelegateT>
  static CallbackTN *MakeCallback(
      typename BoundCallback<ParamT, DelegateT>::CallbackMethod method,
      DelegateT *delegate) {
    return new BoundCallback<ParamT, DelegateT>(method, delegate);
  }

  /**
   * Produces a Callback object that invokes a static function or a globally de-
   * fined C-like function.
   *
   * @param function  Function pointer to the function to be invoked
   */
  static CallbackTN *MakeCallback(
      typename Callback<ParamT>::CallbackFunction function) {
    return new Callback<ParamT>(function);
  }
};


/**
 * Wrapper function to bind an arbitrary this* to a method call in a C-style
 * spawned thread function.
 * The method called by the ThreadProxy template is meant to look like this:
 *   void foo();
 */
template<class DelegateT>
void ThreadProxy(DelegateT *delegate, void (DelegateT::*method)()) {
  (*delegate.*method)();
}


#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif

#endif  // CVMFS_UTIL_ASYNC_H_
