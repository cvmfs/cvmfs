/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UTIL_H_
#define CVMFS_UTIL_H_

#include <sys/time.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <time.h>
#include <fcntl.h>
#include <pthread.h>

#include <cstdio>

#include <string>
#include <map>
#include <vector>
#include <set>

#include "murmur.h"
#include "platform.h"
#include "hash.h"
#include "shortstring.h"

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

const size_t kMaxPathLength = 256;

const int kDefaultFileMode = S_IWUSR | S_IRUSR | S_IRGRP | S_IROTH;
const int kDefaultDirMode = S_IXUSR | S_IWUSR | S_IRUSR |
                            S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH;

/**
 * Type Trait:
 * "Static" assertion that a template parameter is a pointer
 */
template<typename T>
struct IsPointer { static const bool value = false; };
template<typename T>
struct IsPointer<T*> { static const bool value = true; };

/**
 * Generic base class to mark an inheriting class as 'non-copyable'
 */
class SingleCopy {
protected:
  // Prevent SingleCopy from being instantiated on its own
  SingleCopy() {}

private:
  // Provoke a linker error by not implementing copy constructor and
  // assignment operator.
  SingleCopy(const SingleCopy &other);
  SingleCopy& operator=(const SingleCopy &rhs);
};

std::string MakeCanonicalPath(const std::string &path);
std::string GetParentPath(const std::string &path);
PathString GetParentPath(const PathString &path);
std::string GetFileName(const std::string &path);
NameString GetFileName(const PathString &path);

void CreateFile(const std::string &path, const int mode);
int MakeSocket(const std::string &path, const int mode);
int ConnectSocket(const std::string &path);
void MakePipe(int pipe_fd[2]);
void WritePipe(int fd, const void *buf, size_t nbyte);
void ReadPipe(int fd, void *buf, size_t nbyte);
void ReadHalfPipe(int fd, void *buf, size_t nbyte);
void ClosePipe(int pipe_fd[2]);
struct Pipe : public SingleCopy {
  Pipe() {
    int pipe_fd[2];
    MakePipe(pipe_fd);
    read_end = pipe_fd[0];
    write_end = pipe_fd[1];
  }

  Pipe(const int fd_read, const int fd_write) :
    read_end(fd_read), write_end(fd_write) {}

  void Close() {
    close (read_end);
    close (write_end);
  }

  template<typename T>
  bool Write(const T &data) {
    assert (!IsPointer<T>::value); // TODO: C++11 (replace by static_assert)
    const int num_bytes = write(write_end, &data, sizeof(T));
    return (num_bytes >= 0) && (static_cast<size_t>(num_bytes) == sizeof(T));
  }

  template<typename T>
  bool Read(T *data) {
    assert (!IsPointer<T>::value); // TODO: C++11 (replace by static_assert)
    int num_bytes = read(read_end, data, sizeof(T));
    return (num_bytes >= 0) && (static_cast<size_t>(num_bytes) == sizeof(T));
  }

  bool Write(const void *buf, size_t nbyte) {
    WritePipe(write_end, buf, nbyte);
    return true;
  }

  bool Read(void *buf, size_t nbyte) {
    ReadPipe(read_end, buf, nbyte);
    return true;
  }

  int read_end;
  int write_end;
};

void Nonblock2Block(int filedes);
void Block2Nonblock(int filedes);
void SendMsg2Socket(const int fd, const std::string &msg);
void LockMutex(pthread_mutex_t *mutex);
void UnlockMutex(pthread_mutex_t *mutex);

bool SwitchCredentials(const uid_t uid, const gid_t gid,
                       const bool temporarily);

bool FileExists(const std::string &path);
int64_t GetFileSize(const std::string &path);
bool DirectoryExists(const std::string &path);
bool MkdirDeep(const std::string &path, const mode_t mode);
bool MakeCacheDirectories(const std::string &path, const mode_t mode);
FILE *CreateTempFile(const std::string &path_prefix, const int mode,
                     const char *open_flags, std::string *final_path);
std::string CreateTempPath(const std::string &path_prefix, const int mode);
int TryLockFile(const std::string &path);
int LockFile(const std::string &path);
void UnlockFile(const int filedes);
bool RemoveTree(const std::string &path);
std::vector<std::string> FindFiles(const std::string &dir,
                                   const std::string &suffix);

std::string StringifyBool(const bool value);
std::string StringifyInt(const int64_t value);
std::string StringifyByteAsHex(const unsigned char value);
std::string StringifyDouble(const double value);
std::string StringifyTime(const time_t seconds, const bool utc);
std::string StringifyTimeval(const timeval value);
std::string StringifyIpv4(const uint32_t ip_address);
int64_t String2Int64(const std::string &value);
uint64_t String2Uint64(const std::string &value);
int HexDigit2Int(const char digit);
void String2Uint64Pair(const std::string &value, uint64_t *a, uint64_t *b);
bool HasPrefix(const std::string &str, const std::string &prefix,
               const bool ignore_case);
bool IsNumeric(const std::string &str);

std::vector<std::string> SplitString(const std::string &str,
                                     const char delim,
                                     const unsigned max_chunks = 0);
std::string JoinStrings(const std::vector<std::string> &strings,
                        const std::string &joint);

double DiffTimeSeconds(struct timeval start, struct timeval end);

std::string GetLineMem(const char *text, const int text_size);
bool GetLineFile(FILE *f, std::string *line);
bool GetLineFd(const int fd, std::string *line);
std::string Trim(const std::string &raw);
std::string ToUpper(const std::string &mixed_case);
std::string ReplaceAll(const std::string &haystack, const std::string &needle,
                       const std::string &replace_by);

void BlockSignal(int signum);
void WaitForSignal(int signum);
void Daemonize();
bool Shell(int *pipe_stdin, int *pipe_stdout, int *pipe_stderr);
bool ExecuteBinary(      int                       *fd_stdin,
                         int                       *fd_stdout,
                         int                       *fd_stderr,
                   const std::string               &binary_path,
                   const std::vector<std::string>  &argv,
                   const bool                       double_fork = true,
                         pid_t                     *child_pid = NULL);
bool ManagedExec(const std::vector<std::string>  &command_line,
                 const std::set<int>             &preserve_fildes,
                 const std::map<int, int>        &map_fildes,
                 const bool                       drop_credentials,
                 const bool                       double_fork = true,
                       pid_t                     *child_pid = NULL);

void SafeSleepMs(const unsigned ms);


template <typename hashed_type>
struct hash_murmur {
  size_t operator() (const hashed_type key) const {
#ifdef __x86_64__
    return MurmurHash64A(&key, sizeof(key), 0x9ce603115bba659bLLU);
#else
    return MurmurHash2(&key, sizeof(key), 0x07387a4f);
#endif
  }
};


template <class T>
class UniquePtr : SingleCopy {
 public:
  inline UniquePtr() : ref_(NULL) {}
  inline UniquePtr(T *ref) : ref_(ref) {}
  inline ~UniquePtr()                 { delete ref_; }

  inline operator bool() const        { return (ref_ != NULL); }
  inline operator T*() const          { return *ref_; }
  inline UniquePtr& operator=(T* ref) { ref_ = ref; return *this; }
  inline T* operator->() const        { return ref_; }

  inline T* weak_ref() const          { return ref_; }

 private:
  T *ref_;
};

/**
 * Very simple StopWatch implementation.
 * Currently the implementation does not allow a restart of a stopped
 * watch. You should always reset the clock before you reuse it.
 *
 * Stopwatch watch();
 * watch.Start();
 * // do nasty thing
 * watch.Stop();
 * printf("%f", watch.GetTime());
 */
class StopWatch : SingleCopy {
 public:
  StopWatch() : running_(false) {}

  void Start();
  void Stop();
  void Reset();

  double GetTime() const;

 private:
  bool running_;
  timeval start_, end_;
};


/**
 * Wraps the functionality of mmap() to create a read-only memory mapped file.
 *
 * Note: You need to call Map() to actually map the provided file path to memory
 */
class MemoryMappedFile : SingleCopy {
 public:
  MemoryMappedFile(const std::string &file_path);
  ~MemoryMappedFile();

  bool Map();
  void Unmap();

  inline unsigned char*      buffer()    const { return mapped_file_; }
  inline size_t              size()      const { return mapped_size_; }
  inline const std::string&  file_path() const { return file_path_; }

  inline bool IsMapped() const { return mapped_; }

 private:
  const std::string  file_path_;
  int                file_descriptor_;
  unsigned char     *mapped_file_;
  size_t             mapped_size_;
  bool               mapped_;
};

//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//

/**
 * Used internally by the PolymorphicConstruction template
 * Provides an abstract interface for Factory objects that allow the poly-
 * morphic creation of arbitrary objects at runtime.
 *
 * @param AbstractProductT  the abstract base class of all classes that could be
 *                          polymorphically constructed by this factory
 * @param ParameterT        the type of the parameter that is used to figure out
 *                          which class should be instanciated at runtime
 */
template <class AbstractProductT, typename ParameterT>
class AbstractFactory {
 public:
  AbstractFactory() {}
  virtual ~AbstractFactory() {}

  virtual bool WillHandle(const ParameterT &param) const = 0;
  virtual AbstractProductT* Construct(const ParameterT &param) const = 0;
};


/**
 * Concrete (but templated) implementation of the AbstractFactory template to
 * wrap the creation of a specific class instance. Namely ConcreteProductT.
 * See the description of PolymorphicCreation for more details
 *
 * @param ConcreteProductT  the class that will be instanciated by this factory
 *                          class (must be derived from AbstractProductT)
 * @param AbstractProductT  the base class of all used ConcreteProductT classes
 * @param ParameterT        the type of the parameter that is used to poly-
 *                          morphically create a specific ConcreteProductT
 */
template <class ConcreteProductT, class AbstractProductT, typename ParameterT>
class AbstractFactoryImpl : public AbstractFactory<AbstractProductT, ParameterT> {
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
 */
template <class AbstractProductT, typename ParameterT>
class PolymorphicConstruction {
 private:
  typedef AbstractFactory<AbstractProductT, ParameterT> Factory;
  typedef std::vector<Factory*> RegisteredPlugins;

 public:
  virtual ~PolymorphicConstruction() {};

  static AbstractProductT* Construct(const ParameterT &param) {
    LazilyRegisterPlugins();

    // select and initialize the correct plugin at runtime
    // (polymorphic construction)
    typename RegisteredPlugins::const_iterator i    = registered_plugins_.begin();
    typename RegisteredPlugins::const_iterator iend = registered_plugins_.end();
    for (; i != iend; ++i) {
      if ((*i)->WillHandle(param)) {
        // create and initialize the class that claimed responsibility
        AbstractProductT *product = (*i)->Construct(param);
        if (! product->Initialize()) {
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
    if(atomic_read32(&needs_init_)) {
      pthread_mutex_lock(&init_mutex_);
      if(atomic_read32(&needs_init_)) {
        AbstractProductT::RegisterPlugins();
        atomic_dec32(&needs_init_);
      }
      pthread_mutex_unlock(&init_mutex_);
    }

    assert (!registered_plugins_.empty());
  }

  template <class ConcreteProductT>
  static void RegisterPlugin() {
    registered_plugins_.push_back(
      new AbstractFactoryImpl<ConcreteProductT,
                              AbstractProductT,
                              ParameterT>()
    );
  }

  virtual bool Initialize() { return true; };

 private:
  static RegisteredPlugins registered_plugins_;
  static atomic_int32      needs_init_;
  static pthread_mutex_t   init_mutex_;
};

template <class AbstractProductT, typename ParameterT>
atomic_int32
PolymorphicConstruction<AbstractProductT, ParameterT>::needs_init_ = 1;

template <class AbstractProductT, typename ParameterT>
pthread_mutex_t
PolymorphicConstruction<AbstractProductT, ParameterT>::init_mutex_ =
                                                      PTHREAD_MUTEX_INITIALIZER;

// init the static member registered_plugins_ inside the PolymorphicConstruction
// template... whoa, what ugly code :o)
template <class AbstractProductT, typename ParameterT>
typename PolymorphicConstruction<AbstractProductT, ParameterT>::RegisteredPlugins
PolymorphicConstruction<AbstractProductT, ParameterT>::registered_plugins_;


template<typename T, class A = std::allocator<T> >
class Buffer {
 public:
  Buffer() : used_(0), size_(0), buffer_(NULL), initialized_(false) {}

  Buffer(const size_t size) : used_(0), size_(0), buffer_(NULL),
                              initialized_(false)
  {
    Allocate(size);
  }

  virtual ~Buffer() {
    Deallocate();
  }

  void Allocate(const size_t size) {
    assert (!IsInitialized());
    size_        = size;
    buffer_      = A().allocate(size_bytes());
    initialized_ = true;
  }

  bool IsInitialized() const { return initialized_; }

  typename A::pointer ptr() {
    assert (IsInitialized());
    return buffer_;
  }
  const typename A::pointer ptr() const {
    assert (IsInitialized());
    return buffer_;
  }

  void SetUsed(const size_t items) {
    assert (items <= size());
    used_ = items;
  }

  void SetUsedBytes(const size_t bytes) {
    assert (bytes <= size_bytes());
    assert (bytes % sizeof(T) == 0);
    used_ = bytes / sizeof(T);
  }

  size_t size()        const { return size_;             }
  size_t size_bytes()  const { return size_ * sizeof(T); }
  size_t used()        const { return used_;             }
  size_t used_bytes()  const { return used_ * sizeof(T); }

 private:
  Buffer(const Buffer &other) { assert (false); } // no copy!
  Buffer& operator=(const Buffer& other) { assert (false); }

  void Deallocate() {
    if (size_ == 0) {
      return;
    }
    A().deallocate(buffer_, size_bytes());
    buffer_      = NULL;
    size_        = 0;
    used_        = 0;
    initialized_ = false;
  }

 private:
  size_t               used_;
  size_t               size_;
  typename A::pointer  buffer_;
  bool                 initialized_;
};

#ifdef CVMFS_NAMESPACE_GUARD
}
#endif

#endif  // CVMFS_UTIL_H_
