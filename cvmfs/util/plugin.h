/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UTIL_PLUGIN_H_
#define CVMFS_UTIL_PLUGIN_H_

#include <pthread.h>

#include <cassert>
#include <vector>

#include "atomic.h"

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

/**
 * Used internally by the PolymorphicConstruction template
 * Provides an abstract interface for Factory objects that allow the poly-
 * morphic creation of arbitrary objects at runtime.
 *
 * @param AbstractProductT  the abstract base class of all classes that could be
 *                          polymorphically constructed by this factory
 * @param ParameterT        the type of the parameter that is used to figure out
 *                          which class should be instanciated at runtime
 * @param InfoT             wrapper type for introspection data of registered
 *                          plugins
 */
template <class AbstractProductT, typename ParameterT, typename InfoT>
class AbstractFactory {
 public:
  AbstractFactory() {}
  virtual ~AbstractFactory() {}

  virtual bool WillHandle(const ParameterT &param) const = 0;
  virtual AbstractProductT* Construct(const ParameterT &param) const = 0;
  virtual InfoT Introspect() const = 0;
};


/**
 * Implementation of the AbstractFactory template to wrap the creation of a
 * specific class instance. Namely ConcreteProductT. (Note: still abstract)
 * See the description of PolymorphicCreation for more details
 *
 * @param ConcreteProductT  the class that will be instanciated by this factory
 *                          class (must be derived from AbstractProductT)
 * @param AbstractProductT  the base class of all used ConcreteProductT classes
 * @param ParameterT        the type of the parameter that is used to poly-
 *                          morphically create a specific ConcreteProductT
 * @param InfoT             wrapper type for introspection data of registered
 *                          plugins
 */
template <class ConcreteProductT,
          class AbstractProductT,
          typename ParameterT,
          typename InfoT>
class AbstractFactoryImpl2 : public AbstractFactory<AbstractProductT,
                                                    ParameterT,
                                                    InfoT>
{
 public:
  inline bool WillHandle(const ParameterT &param) const {
    return ConcreteProductT::WillHandle(param);
  }
  inline AbstractProductT* Construct(const ParameterT &param) const {
    AbstractProductT* product = new ConcreteProductT(param);
    return product;
  }
};


/**
 * Template to add an implementation of Introspect() based on the type of InfoT.
 * Generally Introspect() will call ConcreteProductT::GetInfo() and return it's
 * result. However if InfoT = void, this method still needs to be stubbed.
 * (See also the template specialization for InfoT = void below)
 */
template <class ConcreteProductT,
          class AbstractProductT,
          typename ParameterT,
          typename InfoT>
class AbstractFactoryImpl :
  public AbstractFactoryImpl2<ConcreteProductT,
                              AbstractProductT,
                              ParameterT,
                              InfoT>
{
  inline InfoT Introspect() const {
    return ConcreteProductT::GetInfo();
  }
};

/**
 * Template specialization for InfoT = void that only stubs the abstract method
 * Introspect().
 */
template <class ConcreteProductT,
          class AbstractProductT,
          typename ParameterT>
class AbstractFactoryImpl<ConcreteProductT,
                          AbstractProductT,
                          ParameterT,
                          void> :
  public AbstractFactoryImpl2<ConcreteProductT,
                              AbstractProductT,
                              ParameterT,
                              void>
{
  inline void Introspect() const {}
};

/**
 * Template to simplify the polymorphic creation of a number of concrete classes
 * that share the common base class AbstractProductT. Use this to create flexible
 * class hierarchies.
 *
 * The template assumes a number of things from the user classes:
 *  1. AbstractProductT must implement `static void RegisterPlugins()` which
 *     will register all available derived classes by calling
 *     `RegisterPlugin<DerivedClass>()` for each implemented sub-class.
 *  2. Each derived class of AbstractProductT must implement
 *     `static bool WillHandle(const ParameterT &param)` that figures out if the
 *     concrete class can cope with the given parameter
 *  3. Each derived class must have at least the following constructor:
 *     `DerivedClass(const ParameterT &param)` which is used to instantiate the
 *     concrete class in case it returned true in WillHandle()
 *  4. (OPTIONAL) Both AbstractProductT and ConcreteProductTs can override the
 *     virtual method `bool Initialize()` which will be called directly after
 *     creation of a ConcreteProductT. If it returns false, the constructed in-
 *     stance is deleted and the list of plugins is traversed further.
 *  5. (OPTIONAL) The ConcreteProductTs can implement a `static InfoT GetInfo()`
 *     that can be used for run-time introspection of registered plugins using
 *     PolymorphicConstruction<AbstractProductT, ParameterT, InfoT>::Introspect()
 *
 * A possible class hierarchy could look like this:
 *
 *    PolymorphicConstruction<AbstractNumberCruncher, Parameter>
 *     |
 *     +--> AbstractNumberCruncher
 *           |
 *           +--> ConcreteMulticoreNumberCruncher
 *           |
 *           +--> ConcreteGpuNumberCruncher
 *           |
 *           +--> ConcreteClusterNumberCruncher
 *
 * In this example AbstractNumberCruncher::RegisterPlugins() will register all
 * three concrete number cruncher classes. Using the whole thing would look like
 * so:
 *
 *   Parameter param = Parameter(typicalGpuProblem);
 *   AbstractNumberCruncher *polymorphicCruncher =
 *                                   AbstractNumberCruncher::Construct(param);
 *   polymorphicCruncher->Crunch();
 *
 * `polymorphicCruncher` now points to an instance of ConcreteGpuNumberCruncher
 * and can be used as any other polymorphic class with the interface defined in
 * AbstractNumberCruncher.
 *
 * Note: PolymorphicCreation goes through the list of registered plugins in the
 *       order they have been registered and instantiates the first class that
 *       claims responsibility for the given parameter.
 *
 * @param AbstractProductT  the common base class of all classes that should be
 *                          polymorphically created. In most cases this will be
 *                          the class that directly inherits from Polymorphic-
 *                          Construction.
 * @param ParameterT        the type of the parameter that is used to poly-
 *                          morphically instantiate one of the subclasses of
 *                          AbstractProductT
 * @param InfoT             (optional) wrapper type for introspection data of
 *                          registered plugins. InfoT AbstractProductT::GetInfo()
 *                          needs to be implemented for each plugin
 */
template <class AbstractProductT, typename ParameterT, typename InfoT>
class PolymorphicConstructionImpl {
 protected:
  typedef AbstractFactory<AbstractProductT, ParameterT, InfoT> Factory;
  typedef std::vector<Factory*> RegisteredPlugins;

 public:
  virtual ~PolymorphicConstructionImpl() { }

  static AbstractProductT* Construct(const ParameterT &param) {
    LazilyRegisterPlugins();

    // select and initialize the correct plugin at runtime
    // (polymorphic construction)
    typename RegisteredPlugins::const_iterator i = registered_plugins_.begin();
    typename RegisteredPlugins::const_iterator iend = registered_plugins_.end();
    for (; i != iend; ++i) {
      if ((*i)->WillHandle(param)) {
        // create and initialize the class that claimed responsibility
        AbstractProductT *product = (*i)->Construct(param);
        if (!product->Initialize()) {
          delete product;
          continue;
        }
        return product;
      }
    }

    // no plugin found to handle the given parameter...
    return NULL;
  }

 protected:
  static void LazilyRegisterPlugins() {
    // Thread Safety Note:
    //   Double Checked Locking with atomics!
    //   Simply double checking registered_plugins_.empty() is _not_ thread safe
    //   since a second thread might find a registered_plugins_ list that is
    //   currently under construction and therefore _not_ empty but also _not_
    //   fully initialized!
    // See StackOverflow: http://stackoverflow.com/questions/8097439/lazy-initialized-caching-how-do-i-make-it-thread-safe
    if (atomic_read32(&needs_init_)) {
      pthread_mutex_lock(&init_mutex_);
      if (atomic_read32(&needs_init_)) {
        AbstractProductT::RegisterPlugins();
        atomic_dec32(&needs_init_);
      }
      pthread_mutex_unlock(&init_mutex_);
    }

    assert(!registered_plugins_.empty());
  }

  /**
   * Friend class for testability (see test/common/testutil.h)
   */
  friend class PolymorphicConstructionUnittestAdapter;

  /**
   * Registers a plugin that is polymorphically constructable afterwards.
   * Warning: Multiple registrations of the same ConcreteProductT might lead to
   *          undefined behaviour!
   *
   * @param ConcreteProductT  the concrete implementation of AbstractProductT
   *                          that should be registered as constructable.
   *
   * Note: You shall not need to use this method anywhere in your code
   *       except in AbstractProductT::RegisterPlugins().
   */
  template <class ConcreteProductT>
  static void RegisterPlugin() {
    registered_plugins_.push_back(
      new AbstractFactoryImpl<ConcreteProductT,
                              AbstractProductT,
                              ParameterT,
                              InfoT>());
  }

  virtual bool Initialize() { return true; }

 private:
  /**
   * This method clears the list of registered plugins.
   * Note: A user of PolymorphicConstruction is _not_ supposed to use this! The
   *       method is meant to be used solely for testing purposes! In particular
   *       a unit test registering a mocked plugin is supposed to clear up after
   *       _each_ unit test! see: gtest: SetUp() / TearDown() and
   *                              PolymorphicConstructionUnittestAdapter
   *
   *       DO NOT USE THIS OUTSIDE UNIT TESTS!!
   *       -> Global state is nasty!
   */
  static void UnregisterAllPlugins() {
    registered_plugins_.clear();
    needs_init_ = 1;
  }

 protected:
  static RegisteredPlugins registered_plugins_;

 private:
  static atomic_int32      needs_init_;
  static pthread_mutex_t   init_mutex_;
};


/**
 * Interface template for PolymorphicConstruction.
 * This adds the static method Introspect() to each PolymorphicConstruction type
 * implementation if (and only if) InfoT is not void.
 * Backward compatibility: if InfoT is not defined (i.e. is void), Introspect()
 *                         is not defined at all! (see template specialization)
 */
template <class AbstractProductT, typename ParameterT, typename InfoT = void>
class PolymorphicConstruction :
       public PolymorphicConstructionImpl<AbstractProductT, ParameterT, InfoT> {
 private:
  typedef PolymorphicConstructionImpl<AbstractProductT, ParameterT, InfoT> T;
  typedef typename T::RegisteredPlugins RegisteredPlugins;

 public:
  typedef std::vector<InfoT> IntrospectionData;

  static IntrospectionData Introspect() {
    IntrospectionData introspection_data;
    introspection_data.reserve(T::registered_plugins_.size());
    const RegisteredPlugins &plugins = T::registered_plugins_;

    T::LazilyRegisterPlugins();
    typename RegisteredPlugins::const_iterator i    = plugins.begin();
    typename RegisteredPlugins::const_iterator iend = plugins.end();
    for (; i != iend; ++i) {
      introspection_data.push_back((*i)->Introspect());
    }

    return introspection_data;
  }
};

/**
 * Template specialization for backward compatibility that _does not_ implement
 * a static Introspect() method when the InfoT parameter is not given or is void
 */
template <class AbstractProductT, typename ParameterT>
class PolymorphicConstruction<AbstractProductT, ParameterT, void> :
      public PolymorphicConstructionImpl<AbstractProductT, ParameterT, void> {};



template <class AbstractProductT, typename ParameterT, typename InfoT>
atomic_int32
PolymorphicConstructionImpl<AbstractProductT, ParameterT, InfoT>::
  needs_init_ = 1;

template <class AbstractProductT, typename ParameterT, typename InfoT>
pthread_mutex_t
PolymorphicConstructionImpl<AbstractProductT, ParameterT, InfoT>::init_mutex_ =
                                                      PTHREAD_MUTEX_INITIALIZER;

// init the static member registered_plugins_ inside the
// PolymorphicConstructionImpl template... whoa, what ugly code :o)
template <class AbstractProductT, typename ParameterT, typename InfoT>
typename PolymorphicConstructionImpl<AbstractProductT, ParameterT, InfoT>::
  RegisteredPlugins
PolymorphicConstructionImpl<AbstractProductT, ParameterT, InfoT>::
  registered_plugins_;


#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif

#endif  // CVMFS_UTIL_PLUGIN_H_
