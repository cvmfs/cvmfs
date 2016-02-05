/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UTIL_H_
#define CVMFS_UTIL_H_

#include <fcntl.h>
#include <gtest/gtest_prod.h>
#include <pthread.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include <algorithm>
#include <cstdio>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "hash.h"
#include "murmur.h"
#include "platform.h"
#include "prng.h"
#include "shortstring.h"

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

const unsigned kPageSize = 4096;
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

class FileDescriptor : SingleCopy {
 public:
  FileDescriptor() : fd_(-1) { }
  explicit FileDescriptor(int fd) : fd_(fd) { }
  virtual ~FileDescriptor() { if (IsValid()) close(fd_); }
  inline void Assign(int fd) { fd_ = fd; }
  inline operator int() const { return fd_; }
  inline bool operator ==(const int other_fd) const { return fd_ == other_fd; }
  inline bool operator !=(const int other_fd) const { return fd_ != other_fd; }
  inline bool IsValid() const  { return fd_ >= 0; }
  inline int Read(void *buffer, size_t size) {
    return IsValid() ? read(fd_, buffer, size) : -1;
  }
  inline int Write(const void *buffer, size_t size) {
    return IsValid() ? write(fd_, buffer, size) : -1;
  }

 protected:
  int fd_;
};

class Socket : public FileDescriptor {
 public:
  using FileDescriptor::operator int;
  Socket() : FileDescriptor() { }
  explicit Socket(int fd) : FileDescriptor(fd) { }
  inline int Listen(int backlog = 1) { return listen(fd_, backlog); }
  inline int Accept(struct sockaddr *client_addr, unsigned int *length) {
    return accept(fd_, client_addr, length);
  }
};

std::string MakeCanonicalPath(const std::string &path);
std::string GetParentPath(const std::string &path);
PathString GetParentPath(const PathString &path);
std::string GetFileName(const std::string &path);
NameString GetFileName(const PathString &path);
void SplitPath(const std::string &path,
               std::string *dirname,
               std::string *filename);
bool IsAbsolutePath(const std::string &path);
bool IsHttpUrl(const std::string &path);

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
    read_end.Assign(pipe_fd[0]);
    write_end.Assign(pipe_fd[1]);
  }

  Pipe(const int fd_read, const int fd_write) :
    read_end(fd_read), write_end(fd_write) {}

  template<typename T>
  bool Write(const T &data) {
    assert(!IsPointer<T>::value);  // TODO(rmeusel): C++11 static_assert
    const int num_bytes = write_end.Write(&data, sizeof(T));
    return (num_bytes >= 0) && (static_cast<size_t>(num_bytes) == sizeof(T));
  }

  template<typename T>
  bool Read(T *data) {
    assert(!IsPointer<T>::value);  // TODO(rmeusel): C++11 static_assert
    const int num_bytes = read_end.Read(data, sizeof(T));
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

  FileDescriptor read_end;
  FileDescriptor write_end;
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
bool SymlinkExists(const std::string &path);
bool SymlinkForced(const std::string &src, const std::string &dest);
bool MkdirDeep(const std::string &path, const mode_t mode,
               bool verify_writable = true);
bool MakeCacheDirectories(const std::string &path, const mode_t mode);
FILE *CreateTempFile(const std::string &path_prefix, const int mode,
                     const char *open_flags, std::string *final_path);
std::string CreateTempPath(const std::string &path_prefix, const int mode);
std::string CreateTempDir(const std::string &path_prefix);
std::string GetCurrentWorkingDirectory();
int TryLockFile(const std::string &path);
int LockFile(const std::string &path);
void UnlockFile(const int filedes);
bool RemoveTree(const std::string &path);
std::vector<std::string> FindFiles(const std::string &dir,
                                   const std::string &suffix);
bool GetUidOf(const std::string &username, uid_t *uid, gid_t *main_gid);
bool GetGidOf(const std::string &groupname, gid_t *gid);
mode_t GetUmask();
bool AddGroup2Persona(const gid_t gid);

std::string StringifyBool(const bool value);
std::string StringifyInt(const int64_t value);
std::string StringifyByteAsHex(const unsigned char value);
std::string StringifyDouble(const double value);
std::string StringifyTime(const time_t seconds, const bool utc);
std::string StringifyTimeval(const timeval value);
std::string RfcTimestamp();
time_t IsoTimestamp2UtcTime(const std::string &iso8601);
int64_t String2Int64(const std::string &value);
uint64_t String2Uint64(const std::string &value);
bool String2Uint64Parse(const std::string &value, uint64_t *result);

void String2Uint64Pair(const std::string &value, uint64_t *a, uint64_t *b);
bool HasPrefix(const std::string &str, const std::string &prefix,
               const bool ignore_case);
bool HasSuffix(const std::string &str, const std::string &suffix,
               const bool ignore_case);

std::vector<std::string> SplitString(const std::string &str,
                                     const char delim,
                                     const unsigned max_chunks = 0);
std::string JoinStrings(const std::vector<std::string> &strings,
                        const std::string &joint);
void ParseKeyvalMem(const unsigned char *buffer, const unsigned buffer_size,
                    std::map<char, std::string> *content);
bool ParseKeyvalPath(const std::string &filename,
                     std::map<char, std::string> *content);

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
bool ExecuteBinary(int *fd_stdin,
                   int *fd_stdout,
                   int *fd_stderr,
                   const std::string &binary_path,
                   const std::vector<std::string>  &argv,
                   const bool double_fork = true,
                   pid_t *child_pid = NULL);
bool ManagedExec(const std::vector<std::string> &command_line,
                 const std::set<int> &preserve_fildes,
                 const std::map<int, int> &map_fildes,
                 const bool drop_credentials,
                 const bool double_fork = true,
                 pid_t *child_pid = NULL);

void SafeSleepMs(const unsigned ms);
// Note that SafeWrite cannot return partial results but
// SafeRead can (as we may have hit the EOF).
ssize_t SafeRead(int fd, void *buf, size_t nbyte);
bool SafeWrite(int fd, const void *buf, size_t nbyte);

// Read the contents of a file descriptor to a string.
bool SafeReadToString(int fd, std::string *final_result);

/**
 * Knuth's random shuffle algorithm.
 */
template <typename T>
std::vector<T> Shuffle(const std::vector<T> &input, Prng *prng) {
  std::vector<T> shuffled(input);
  unsigned N = shuffled.size();
  // No shuffling for the last element
  for (unsigned i = 0; i < N; ++i) {
    const unsigned swap_idx = i + prng->Next(N - i);
    std::swap(shuffled[i], shuffled[swap_idx]);
  }
  return shuffled;
}


/**
 * Sorts the vector tractor and applies the same permutation to towed.  Both
 * vectors have to be of the same size.  Type T must be sortable (< operator).
 * Uses insertion sort (n^2), only efficient for small vectors.
 */
template <typename T, typename U>
void SortTeam(std::vector<T> *tractor, std::vector<U> *towed) {
  assert(tractor);
  assert(towed);
  assert(tractor->size() == towed->size());
  unsigned N = tractor->size();

  // Insertion sort on both, tractor and towed
  for (unsigned i = 1; i < N; ++i) {
    T val_tractor = (*tractor)[i];
    U val_towed = (*towed)[i];
    int pos;
    for (pos = i-1; (pos >= 0) && ((*tractor)[pos] > val_tractor); --pos) {
      (*tractor)[pos+1] = (*tractor)[pos];
      (*towed)[pos+1] = (*towed)[pos];
    }
    (*tractor)[pos+1] = val_tractor;
    (*towed)[pos+1] = val_towed;
  }
}


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
std::string Base64(const std::string &data);
bool Debase64(const std::string &data, std::string *decoded);

template <class T>
class UniquePtr : SingleCopy {
 public:
  inline UniquePtr() : ref_(NULL) {}
  inline explicit UniquePtr(T *ref) : ref_(ref) { }
  inline ~UniquePtr()                 { delete ref_; }

  inline operator bool() const        { return IsValid(); }
  inline T& operator*() const         { return *ref_; }
  inline T* operator->() const        { return ref_; }
  inline UniquePtr& operator=(T* ref) {
    if (ref_ != ref) {
      delete ref_;
      ref_ = ref;
    }
    return *this;
  }

  inline T* weak_ref() const          { return ref_; }
  inline bool IsValid() const         { return (ref_ != NULL); }
  inline T*   Release()               { T* r = ref_; ref_ = NULL; return r; }

 private:
  T *ref_;
};

/**
 * RAII object to call `unlink()` on a containing file when it gets out of scope
 */
class UnlinkGuard : SingleCopy {
 public:
  enum InitialState { kEnabled, kDisabled };

 public:
  inline UnlinkGuard() : enabled_(false) {}
  inline UnlinkGuard(const std::string &path,
                     const InitialState state = kEnabled)
            : path_(path)
            , enabled_(state == kEnabled) {}
  inline ~UnlinkGuard() { if (IsEnabled()) unlink(path_.c_str()); }

  inline void Set(const std::string &path) { path_ = path; Enable(); }

  inline bool IsEnabled() const { return enabled_;  }
  inline void Enable()          { enabled_ = true;  }
  inline void Disable()         { enabled_ = false; }

  const std::string& path() const { return path_; }

 private:
  std::string  path_;
  bool         enabled_;
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
  explicit MemoryMappedFile(const std::string &file_path);
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


//------------------------------------------------------------------------------


/**
 * Encapsulates a callback function that handles asynchronous responses.
 *
 * This is an abstract base class for two different callback function objects.
 * There are two specializations:
 *  --> 1. for static members or global C-like functions
 *  --> 2. for member functions of arbitrary objects
 */
template <typename ParamT>
class CallbackBase {
 public:
  virtual ~CallbackBase() {}
  virtual void operator()(const ParamT &value) const = 0;
};

template <>
class CallbackBase<void> {
 public:
  virtual ~CallbackBase() {}
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
template <typename ParamT>
class Callback : public CallbackBase<ParamT> {
 public:
  typedef void (*CallbackFunction)(const ParamT &value);

  explicit Callback(CallbackFunction function) : function_(function) {}
  void operator()(const ParamT &value) const { function_(value); }

 private:
  CallbackFunction function_;
};

template <>
class Callback<void> : public CallbackBase<void> {
 public:
  typedef void (*CallbackFunction)();

  explicit Callback(CallbackFunction function) : function_(function) {}
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
template <typename ParamT, class DelegateT>
class BoundCallback : public CallbackBase<ParamT> {
 public:
  typedef void (DelegateT::*CallbackMethod)(const ParamT &value);

  BoundCallback(CallbackMethod method, DelegateT *delegate) :
    delegate_(delegate),
    method_(method) {}

  void operator()(const ParamT &value) const { (delegate_->*method_)(value); }

 private:
  DelegateT*     delegate_;
  CallbackMethod method_;
};

template <class DelegateT>
class BoundCallback<void, DelegateT> : public CallbackBase<void> {
 public:
  typedef void (DelegateT::*CallbackMethod)();

  BoundCallback(CallbackMethod method, DelegateT *delegate) :
    delegate_(delegate), method_(method) {}

  void operator()() const { (delegate_->*method_)(); }

 private:
  DelegateT*     delegate_;
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
template <typename ParamT, class DelegateT, typename ClosureDataT>
class BoundClosure : public CallbackBase<ParamT> {
 public:
  typedef void (DelegateT::*CallbackMethod)(const ParamT        &value,
                                            const ClosureDataT   closure_data);

 public:
  BoundClosure(CallbackMethod  method,
               DelegateT      *delegate,
               ClosureDataT    data) :
    delegate_(delegate),
    method_(method),
    closure_data_(data) {}

  void operator()(const ParamT &value) const {
    (delegate_->*method_)(value, closure_data_);
  }

 private:
  DelegateT*          delegate_;
  CallbackMethod      method_;
  const ClosureDataT  closure_data_;
};

template <class DelegateT, typename ClosureDataT>
class BoundClosure<void, DelegateT, ClosureDataT> : public CallbackBase<void> {
 public:
  typedef void (DelegateT::*CallbackMethod)(const ClosureDataT closure_data);

 public:
  BoundClosure(CallbackMethod  method,
               DelegateT      *delegate,
               ClosureDataT    data) :
    delegate_(delegate), method_(method), closure_data_(data) {}

  void operator()() const { (delegate_->*method_)(closure_data_); }

 private:
  DelegateT*         delegate_;
  CallbackMethod     method_;
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
template <class ParamT>
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
  template <class DelegateT, typename ClosureDataT>
  static CallbackTN* MakeClosure(
    typename BoundClosure<ParamT, DelegateT, ClosureDataT>::
             CallbackMethod method,
    DelegateT *delegate,
    const ClosureDataT &closure_data)
  {
    return new BoundClosure<ParamT, DelegateT, ClosureDataT>(method,
                                                             delegate,
                                                             closure_data);
  }

  /**
   * Produces a BoundCallback object that invokes a method on a delegate object.
   *
   * @param method    Function pointer to the method to be called
   * @param delegate  The delegate object on which <method> will be called
   */
  template <class DelegateT>
  static CallbackTN* MakeCallback(
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
  static CallbackTN* MakeCallback(
        typename Callback<ParamT>::CallbackFunction function) {
    return new Callback<ParamT>(function);
  }
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
   * Friend class for testability (see test/unittests/testutil.h)
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


/**
 * Wrapper function to bind an arbitrary this* to a method call in a C-style
 * spawned thread function.
 * The method called by the ThreadProxy template is meant to look like this:
 *   void foo();
 */
template <class DelegateT>
void ThreadProxy(DelegateT        *delegate,
                 void (DelegateT::*method)()) {
  (*delegate.*method)();
}


template<typename T, class A = std::allocator<T> >
class Buffer {
 public:
  typedef typename A::pointer pointer_t;

  Buffer() : used_(0), size_(0), buffer_(NULL), initialized_(false) {}

  explicit Buffer(const size_t size)
    : used_(0)
    , size_(0)
    , buffer_(NULL)
    , initialized_(false)
  {
    Allocate(size);
  }

  virtual ~Buffer() {
    Deallocate();
  }

  void Allocate(const size_t size) {
    assert(!IsInitialized());
    size_        = size;
    buffer_      = allocator_.allocate(size_bytes());
    initialized_ = true;
  }

  bool IsInitialized() const { return initialized_; }

  typename A::pointer ptr() {
    assert(IsInitialized());
    return buffer_;
  }
  const typename A::pointer ptr() const {
    assert(IsInitialized());
    return buffer_;
  }

  typename A::pointer free_space_ptr() {
    assert(IsInitialized());
    return buffer_ + used();
  }

  const typename A::pointer free_space_ptr() const {
    assert(IsInitialized());
    return buffer_ + used();
  }

  void SetUsed(const size_t items) {
    assert(items <= size());
    used_ = items;
  }

  void SetUsedBytes(const size_t bytes) {
    assert(bytes <= size_bytes());
    assert(bytes % sizeof(T) == 0);
    used_ = bytes / sizeof(T);
  }

  size_t size()        const { return size_;              }
  size_t size_bytes()  const { return size_ * sizeof(T);  }
  size_t used()        const { return used_;              }
  size_t used_bytes()  const { return used_ * sizeof(T);  }
  size_t free()        const { return size_ - used_;      }
  size_t free_bytes()  const { return free() * sizeof(T); }

 private:
  Buffer(const Buffer &other) { assert(false); }  // no copy!
  Buffer& operator=(const Buffer& other) { assert (false); }

  void Deallocate() {
    if (size_ == 0) {
      return;
    }
    allocator_.deallocate(buffer_, size_bytes());
    buffer_      = NULL;
    size_        = 0;
    used_        = 0;
    initialized_ = false;
  }

 private:
  A                    allocator_;
  size_t               used_;
  size_t               size_;
  typename A::pointer  buffer_;
  bool                 initialized_;
};

#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif

#endif  // CVMFS_UTIL_H_
