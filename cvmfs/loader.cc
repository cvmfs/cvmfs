/**
 * This file is part of the CernVM File System.
 *
 * Implements stub callback functions for Fuse.  Their purpose is to
 * redirect calls to the cvmfs shared library and to block calls during the
 * update of the library.
 *
 * The main executable and the cvmfs shared library _must not_ share any
 * symbols.
 */

#define ENOATTR ENODATA  /**< instead of including attr/xattr.h */
#define _FILE_OFFSET_BITS 64

#include "cvmfs_config.h"
#include "loader.h"

#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <openssl/crypto.h>
#include <sched.h>
#include <signal.h>
#include <stddef.h>
#include <sys/resource.h>
#include <time.h>
#include <unistd.h>
// If valgrind headers are present on the build system, then we can detect
// valgrind at runtime.
#ifdef HAS_VALGRIND_HEADERS
#include <valgrind/valgrind.h>
#endif

#include <cassert>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include "atomic.h"
#include "duplex_fuse.h"
#include "duplex_ssl.h"
#include "fence.h"
#include "fuse_main.h"
#include "loader_talk.h"
#include "logging.h"
#include "options.h"
#include "platform.h"
#include "sanitizer.h"
#include "util/exception.h"
#include "util/posix.h"
#include "util/string.h"

using namespace std;  // NOLINT

namespace loader {

// Follow the fuse convention for option parsing
struct CvmfsOptions {
  char *config;
  int uid;
  int gid;
  int system_mount;
  int grab_mountpoint;
  int cvmfs_suid;
  int disable_watchdog;
  int simple_options_parsing;
  int foreground;
  int fuse_debug;

  // Ignored options
  int ign_netdev;
  int ign_user;
  int ign_nouser;
  int ign_users;
  int ign_auto;
  int ign_noauto;
  int ign_libfuse;
};

enum {
  KEY_HELP,
  KEY_VERSION,
  KEY_FOREGROUND,
  KEY_SINGLETHREAD,
  KEY_FUSE_DEBUG,
  KEY_CVMFS_DEBUG,
  KEY_OPTIONS_PARSE,
};
#define CVMFS_OPT(t, p, v) { t, offsetof(struct CvmfsOptions, p), v }
#define CVMFS_SWITCH(t, p) { t, offsetof(struct CvmfsOptions, p), 1 }
static struct fuse_opt cvmfs_array_opts[] = {
  CVMFS_OPT("config=%s",                    config, 0),
  CVMFS_OPT("uid=%d",                       uid, 0),
  CVMFS_OPT("gid=%d",                       gid, 0),
  CVMFS_SWITCH("system_mount",              system_mount),
  CVMFS_SWITCH("grab_mountpoint",           grab_mountpoint),
  CVMFS_SWITCH("cvmfs_suid",                cvmfs_suid),
  CVMFS_SWITCH("disable_watchdog",          disable_watchdog),
  CVMFS_SWITCH("simple_options_parsing",    simple_options_parsing),
  CVMFS_SWITCH("foreground",                foreground),
  CVMFS_SWITCH("fuse_debug",                fuse_debug),

  // Ignore these options
  CVMFS_SWITCH("_netdev",          ign_netdev),
  CVMFS_SWITCH("user",             ign_user),
  CVMFS_SWITCH("nouser",           ign_nouser),
  CVMFS_SWITCH("users",            ign_users),
  CVMFS_SWITCH("auto",             ign_auto),
  CVMFS_SWITCH("noauto",           ign_noauto),
  CVMFS_OPT("libfuse=%d",          ign_libfuse, 0),

  FUSE_OPT_KEY("-V",            KEY_VERSION),
  FUSE_OPT_KEY("--version",     KEY_VERSION),
  FUSE_OPT_KEY("-h",            KEY_HELP),
  FUSE_OPT_KEY("--help",        KEY_HELP),
  FUSE_OPT_KEY("-f",            KEY_FOREGROUND),
  FUSE_OPT_KEY("-d",            KEY_FUSE_DEBUG),
  FUSE_OPT_KEY("debug",         KEY_CVMFS_DEBUG),
  FUSE_OPT_KEY("-s",            KEY_SINGLETHREAD),
  FUSE_OPT_KEY("parse",         KEY_OPTIONS_PARSE),
  FUSE_OPT_KEY("-k",            KEY_OPTIONS_PARSE),
  {0, 0, 0},
};


string *repository_name_ = NULL;
string *mount_point_ = NULL;
string *config_files_ = NULL;
string *socket_path_ = NULL;
string *usyslog_path_ = NULL;
uid_t uid_ = 0;
gid_t gid_ = 0;
bool single_threaded_ = false;
bool foreground_ = false;
bool debug_mode_ = false;
bool system_mount_ = false;
bool grab_mountpoint_ = false;
bool parse_options_only_ = false;
bool suid_mode_ = false;
bool premounted_ = false;
bool disable_watchdog_ = false;
bool simple_options_parsing_ = false;
void *library_handle_;
Fence *fence_reload_;
CvmfsExports *cvmfs_exports_;
LoaderExports *loader_exports_;


static void Usage(const string &exename) {
  LogCvmfs(kLogCvmfs, kLogStdout,
    "The CernVM File System\n"
    "Version %s\n"
    "Copyright (c) 2009- CERN, all rights reserved\n\n"
    "Please visit http://cernvm.cern.ch for details.\n\n"
    "Usage: %s [-h] [-V] [-s] [-f] [-d] [-k] [-o mount options] "
      "<repository name> <mount point>\n\n"
    "CernVM-FS general options:\n"
    "  --help|-h            Print Help output (this)\n"
    "  --version|-V         Print CernVM-FS version\n"
    "  -s                   Run singlethreaded\n"
    "  -f                   Run in foreground\n"
    "  -d                   Enable debugging\n"
    "  -k                   Parse options\n"
    "CernVM-FS mount options:\n"
    "  -o config=FILES      colon-separated path list of config files\n"
    "  -o uid=UID           Drop credentials to another user\n"
    "  -o gid=GID           Drop credentials to another group\n"
    "  -o system_mount      Indicate that mount is system-wide\n"
    "  -o grab_mountpoint   give ownership of the mountpoint to the user "
                            "before mounting (required for autofs)\n"
    "  -o parse             Parse and print cvmfs parameters\n"
    "  -o cvmfs_suid        Enable suid mode\n\n"
    "  -o disable_watchdog  Do not spawn a post mortem crash handler\n"
    "  -o foreground        Run in foreground\n"
    "  -o libfuse=[2,3]     Enforce a certain libfuse version\n"
    "Fuse mount options:\n"
    "  -o allow_other       allow access to other users\n"
    "  -o allow_root        allow access to root\n"
    "  -o nonempty          allow mounts over non-empty directory\n",
    PACKAGE_VERSION, exename.c_str());
}

/**
 * For an premounted mountpoint, the argument is the file descriptor to
 * /dev/fuse provided in the form /dev/fd/%d
 */
bool CheckPremounted(const std::string &mountpoint) {
  int len;
  unsigned fd;
  bool retval = (sscanf(mountpoint.c_str(), "/dev/fd/%u%n", &fd, &len) == 1) &&
                (len >= 0) &&
                (static_cast<unsigned>(len) == mountpoint.length());
  if (retval) {
    LogCvmfs(kLogCvmfs, kLogStdout,
             "CernVM-FS: pre-mounted on file descriptor %d", fd);
    return true;
  }
  return false;
}


static void stub_init(void *userdata, struct fuse_conn_info *conn) {
  FenceGuard fence_guard(fence_reload_);
  cvmfs_exports_->cvmfs_operations.init(userdata, conn);
}


static void stub_destroy(void *userdata) {
  FenceGuard fence_guard(fence_reload_);
  cvmfs_exports_->cvmfs_operations.destroy(userdata);
}


static void stub_lookup(fuse_req_t req, fuse_ino_t parent,
                        const char *name)
{
  FenceGuard fence_guard(fence_reload_);
  cvmfs_exports_->cvmfs_operations.lookup(req, parent, name);
}


static void stub_getattr(fuse_req_t req, fuse_ino_t ino,
                         struct fuse_file_info *fi)
{
  FenceGuard fence_guard(fence_reload_);
  cvmfs_exports_->cvmfs_operations.getattr(req, ino, fi);
}


static void stub_readlink(fuse_req_t req, fuse_ino_t ino) {
  FenceGuard fence_guard(fence_reload_);
  cvmfs_exports_->cvmfs_operations.readlink(req, ino);
}


static void stub_opendir(fuse_req_t req, fuse_ino_t ino,
                         struct fuse_file_info *fi)
{
  FenceGuard fence_guard(fence_reload_);
  cvmfs_exports_->cvmfs_operations.opendir(req, ino, fi);
}


static void stub_releasedir(fuse_req_t req, fuse_ino_t ino,
                            struct fuse_file_info *fi)
{
  FenceGuard fence_guard(fence_reload_);
  cvmfs_exports_->cvmfs_operations.releasedir(req, ino, fi);
}


static void stub_readdir(fuse_req_t req, fuse_ino_t ino, size_t size,
                         off_t off, struct fuse_file_info *fi)
{
  FenceGuard fence_guard(fence_reload_);
  cvmfs_exports_->cvmfs_operations.readdir(req, ino, size, off, fi);
}


static void stub_open(fuse_req_t req, fuse_ino_t ino,
                      struct fuse_file_info *fi)
{
  FenceGuard fence_guard(fence_reload_);
  cvmfs_exports_->cvmfs_operations.open(req, ino, fi);
}


static void stub_read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                       struct fuse_file_info *fi)
{
  FenceGuard fence_guard(fence_reload_);
  cvmfs_exports_->cvmfs_operations.read(req, ino, size, off, fi);
}


static void stub_release(fuse_req_t req, fuse_ino_t ino,
                         struct fuse_file_info *fi)
{
  FenceGuard fence_guard(fence_reload_);
  cvmfs_exports_->cvmfs_operations.release(req, ino, fi);
}


static void stub_statfs(fuse_req_t req, fuse_ino_t ino) {
  FenceGuard fence_guard(fence_reload_);
  cvmfs_exports_->cvmfs_operations.statfs(req, ino);
}


#ifdef __APPLE__
static void stub_getxattr(fuse_req_t req, fuse_ino_t ino, const char *name,
                          size_t size, uint32_t position)
#else
static void stub_getxattr(fuse_req_t req, fuse_ino_t ino, const char *name,
                          size_t size)
#endif
{
  FenceGuard fence_guard(fence_reload_);
#ifdef __APPLE__
  cvmfs_exports_->cvmfs_operations.getxattr(req, ino, name, size, position);
#else
  cvmfs_exports_->cvmfs_operations.getxattr(req, ino, name, size);
#endif
}


static void stub_listxattr(fuse_req_t req, fuse_ino_t ino, size_t size) {
  FenceGuard fence_guard(fence_reload_);
  cvmfs_exports_->cvmfs_operations.listxattr(req, ino, size);
}


static void stub_forget(
  fuse_req_t req,
  fuse_ino_t ino,
#if CVMFS_USE_LIBFUSE == 2
  unsigned long nlookup  // NOLINT
#else
  uint64_t nlookup
#endif
) {
  FenceGuard fence_guard(fence_reload_);
  cvmfs_exports_->cvmfs_operations.forget(req, ino, nlookup);
}


#if (FUSE_VERSION >= 29)
static void stub_forget_multi(
  fuse_req_t req,
  size_t count,
  struct fuse_forget_data *forgets
) {
  FenceGuard fence_guard(fence_reload_);
  cvmfs_exports_->cvmfs_operations.forget_multi(req, count, forgets);
}
#endif


/**
 * The callback used when fuse is parsing all the options
 * We separate CVMFS options from FUSE options here.
 *
 * \return On success zero, else non-zero
 */
static int ParseFuseOptions(void *data __attribute__((unused)), const char *arg,
                            int key, struct fuse_args *outargs)
{
  unsigned arglen = 0;
  if (arg)
    arglen = strlen(arg);
  switch (key) {
    case FUSE_OPT_KEY_OPT:
      // Check if it a cvmfs option
      if ((arglen > 0) && (arg[0] != '-')) {
        const char **o;
        for (o = (const char**)cvmfs_array_opts; *o; o++) {
          unsigned olen = strlen(*o);
          if ((arglen > olen && arg[olen] == '=') &&
              (strncasecmp(arg, *o, olen) == 0))
            return 0;
        }
      }
      return 1;

    case FUSE_OPT_KEY_NONOPT:
      // first: repository name, second: mount point
      assert(arg != NULL);
      if (!repository_name_) {
        repository_name_ = new string(arg);
      } else {
        if (mount_point_)
          return 1;
        mount_point_ = new string(arg);
        premounted_ = CheckPremounted(*mount_point_);
      }
      return 0;

    case KEY_HELP:
      Usage(outargs->argv[0]);
      exit(0);
    case KEY_VERSION:
      LogCvmfs(kLogCvmfs, kLogStdout, "CernVM-FS version %s\n",
               PACKAGE_VERSION);
      exit(0);
    case KEY_FOREGROUND:
      foreground_ = true;
      return 0;
    case KEY_SINGLETHREAD:
      single_threaded_ = true;
      return 0;
    case KEY_FUSE_DEBUG:
      fuse_opt_add_arg(outargs, "-d");
    case KEY_CVMFS_DEBUG:
      debug_mode_ = true;
      return 0;
    case KEY_OPTIONS_PARSE:
      parse_options_only_ = true;
      return 0;
    default:
      PANIC(kLogStderr, "internal option parsing error");
  }
}


static fuse_args *ParseCmdLine(int argc, char *argv[]) {
  struct fuse_args *mount_options = new fuse_args();
  CvmfsOptions cvmfs_options;
  memset(&cvmfs_options, 0, sizeof(cvmfs_options));

  mount_options->argc = argc;
  mount_options->argv = argv;
  mount_options->allocated = 0;
  if ((fuse_opt_parse(mount_options, &cvmfs_options, cvmfs_array_opts,
                      ParseFuseOptions) != 0) ||
      !mount_point_ || !repository_name_)
  {
    delete mount_options;
    return NULL;
  }
  if (cvmfs_options.config) {
    config_files_ = new string(cvmfs_options.config);
    free(cvmfs_options.config);
  }
  uid_ = cvmfs_options.uid;
  gid_ = cvmfs_options.gid;
  system_mount_ = cvmfs_options.system_mount;
  grab_mountpoint_ = cvmfs_options.grab_mountpoint;
  suid_mode_ = cvmfs_options.cvmfs_suid;
  disable_watchdog_ = cvmfs_options.disable_watchdog;
  simple_options_parsing_ = cvmfs_options.simple_options_parsing;
  if (cvmfs_options.foreground) {
    foreground_ = true;
  }
  if (cvmfs_options.fuse_debug) {
    fuse_opt_add_arg(mount_options, "-d");
  }

  return mount_options;
}


static void SetFuseOperations(struct fuse_lowlevel_ops *loader_operations) {
  memset(loader_operations, 0, sizeof(*loader_operations));

  loader_operations->init        = stub_init;
  loader_operations->destroy     = stub_destroy;

  loader_operations->lookup      = stub_lookup;
  loader_operations->getattr     = stub_getattr;
  loader_operations->readlink    = stub_readlink;
  loader_operations->open        = stub_open;
  loader_operations->read        = stub_read;
  loader_operations->release     = stub_release;
  loader_operations->opendir     = stub_opendir;
  loader_operations->readdir     = stub_readdir;
  loader_operations->releasedir  = stub_releasedir;
  loader_operations->statfs      = stub_statfs;
  loader_operations->getxattr    = stub_getxattr;
  loader_operations->listxattr   = stub_listxattr;
  loader_operations->forget      = stub_forget;
}


static void *OpenLibrary(const string &path) {
  return dlopen(path.c_str(), RTLD_NOW | RTLD_LOCAL);
}


static void CloseLibrary() {
#ifdef HAS_VALGRIND_HEADERS
  // If the libcvmfs_fuse library is unloaded, valgrind can't resolve the
  // symbols anymore.  We skip under valgrind.
  if (!RUNNING_ON_VALGRIND) {
#endif
    dlclose(library_handle_);
    library_handle_ = NULL;
#ifdef HAS_VALGRIND_HEADERS
  }
#endif
}


static CvmfsExports *LoadLibrary(const bool debug_mode,
                                 LoaderExports *loader_exports)
{
  std::string local_lib_path = "./";
  if (getenv("CVMFS_LIBRARY_PATH") != NULL) {
    local_lib_path = getenv("CVMFS_LIBRARY_PATH");
    if (!local_lib_path.empty() && (*local_lib_path.rbegin() != '/'))
      local_lib_path.push_back('/');
  }

#if CVMFS_USE_LIBFUSE == 2
  string library_name = string("cvmfs_fuse") + ((debug_mode) ? "_debug" : "");
#else
  string library_name = string("cvmfs_fuse3") + ((debug_mode) ? "_debug" : "");
#endif
  library_name = platform_libname(library_name);
  string error_messages;

  static vector<string> library_paths;  // TODO(rmeusel): C++11 initializer
  if (library_paths.empty()) {
    library_paths.push_back(local_lib_path + library_name);
    library_paths.push_back("/usr/lib/"   + library_name);
    library_paths.push_back("/usr/lib64/" + library_name);
#ifdef __APPLE__
    // Since OS X El Capitan (10.11) came with SIP, we needed to relocate our
    // binaries from /usr/... to /usr/local/...
    library_paths.push_back("/usr/local/lib/" + library_name);
#endif
  }

  vector<string>::const_iterator i    = library_paths.begin();
  vector<string>::const_iterator iend = library_paths.end();
  for (; i != iend; ++i) {  // TODO(rmeusel): C++11 range based for
    library_handle_ = OpenLibrary(*i);
    if (library_handle_ != NULL) {
      break;
    }

    error_messages += string(dlerror()) + "\n";
  }

  if (!library_handle_) {
    LogCvmfs(kLogCvmfs, kLogStderr | kLogSyslogErr,
             "failed to load cvmfs library, tried: '%s'\n%s",
             JoinStrings(library_paths, "' '").c_str(), error_messages.c_str());
    return NULL;
  }

  CvmfsExports **exports_ptr = reinterpret_cast<CvmfsExports **>(
    dlsym(library_handle_, "g_cvmfs_exports"));
  if (!exports_ptr)
    return NULL;

  if (loader_exports) {
    LoadEvent *load_event = new LoadEvent();
    load_event->timestamp = time(NULL);
    load_event->so_version = (*exports_ptr)->so_version;
    loader_exports->history.push_back(load_event);
  }

  return *exports_ptr;
}


Failures Reload(const int fd_progress, const bool stop_and_go) {
  int retval;

  retval = cvmfs_exports_->fnMaintenanceMode(fd_progress);
  if (!retval)
    return kFailMaintenanceMode;

  SendMsg2Socket(fd_progress, "Blocking new file system calls\n");
  fence_reload_->Close();

  SendMsg2Socket(fd_progress, "Waiting for active file system calls\n");
  fence_reload_->Drain();

  retval = cvmfs_exports_->fnSaveState(fd_progress,
                                       &loader_exports_->saved_states);
  if (!retval)
    return kFailSaveState;

  SendMsg2Socket(fd_progress, "Unloading Fuse module\n");
  cvmfs_exports_->fnFini();
  CloseLibrary();

  if (stop_and_go) {
    CreateFile(*socket_path_ + ".paused", 0600);
    SendMsg2Socket(fd_progress, "Waiting for the delivery of SIGUSR1...\n");
    WaitForSignal(SIGUSR1);
    unlink((*socket_path_ + ".paused").c_str());
  }

  SendMsg2Socket(fd_progress, "Re-Loading Fuse module\n");
  cvmfs_exports_ = LoadLibrary(debug_mode_, loader_exports_);
  if (!cvmfs_exports_)
    return kFailLoadLibrary;
  retval = cvmfs_exports_->fnInit(loader_exports_);
  if (retval != kFailOk) {
    string msg_progress = cvmfs_exports_->fnGetErrorMsg() + " (" +
                          StringifyInt(retval) + ")\n";
    LogCvmfs(kLogCvmfs, kLogSyslogErr, "%s", msg_progress.c_str());
    SendMsg2Socket(fd_progress, msg_progress);
    return (Failures)retval;
  }

  retval = cvmfs_exports_->fnRestoreState(fd_progress,
                                          loader_exports_->saved_states);
  if (!retval)
    return kFailRestoreState;
  cvmfs_exports_->fnFreeSavedState(fd_progress, loader_exports_->saved_states);
  for (unsigned i = 0, l = loader_exports_->saved_states.size(); i < l; ++i) {
    delete loader_exports_->saved_states[i];
  }
  loader_exports_->saved_states.clear();

  SendMsg2Socket(fd_progress, "Activating Fuse module\n");
  cvmfs_exports_->fnSpawn();

  fence_reload_->Open();
  return kFailOk;
}

}  // namespace loader


using namespace loader;  // NOLINT(build/namespaces)

// Making OpenSSL (libcrypto) thread-safe
#ifndef OPENSSL_API_INTERFACE_V11

pthread_mutex_t *gLibcryptoLocks;

static void CallbackLibcryptoLock(int mode, int type,
                                  const char *file, int line) {
  (void)file;
  (void)line;

  int retval;

  if (mode & CRYPTO_LOCK) {
    retval = pthread_mutex_lock(&(gLibcryptoLocks[type]));
  } else {
    retval = pthread_mutex_unlock(&(gLibcryptoLocks[type]));
  }
  assert(retval == 0);
}

static unsigned long CallbackLibcryptoThreadId() {  // NOLINT(runtime/int)
  return platform_gettid();
}

#endif

static void SetupLibcryptoMt() {
#ifndef OPENSSL_API_INTERFACE_V11
  gLibcryptoLocks = static_cast<pthread_mutex_t *>(OPENSSL_malloc(
    CRYPTO_num_locks() * sizeof(pthread_mutex_t)));
  for (int i = 0; i < CRYPTO_num_locks(); ++i) {
    int retval = pthread_mutex_init(&(gLibcryptoLocks[i]), NULL);
    assert(retval == 0);
  }

  CRYPTO_set_id_callback(CallbackLibcryptoThreadId);
  CRYPTO_set_locking_callback(CallbackLibcryptoLock);
#endif
}

static void CleanupLibcryptoMt(void) {
#ifndef OPENSSL_API_INTERFACE_V11
  CRYPTO_set_locking_callback(NULL);
  for (int i = 0; i < CRYPTO_num_locks(); ++i)
    pthread_mutex_destroy(&(gLibcryptoLocks[i]));

  OPENSSL_free(gLibcryptoLocks);
#endif
}


int FuseMain(int argc, char *argv[]) {
  // Set a decent umask for new files (no write access to group/everyone).
  // We want to allow group write access for the talk-socket.
  umask(007);
  // SIGUSR1 is used for the stop_and_go mode during reload
  BlockSignal(SIGUSR1);

  int retval;

  // Jump into alternative process flavors (e.g. shared cache manager)
  // We are here due to a fork+execve (ManagedExec in util.cc) or due to
  // utility calls of cvmfs2
  if ((argc > 1) && (strstr(argv[1], "__") == argv[1])) {
    if (string(argv[1]) == string("__RELOAD__")) {
      if (argc < 3)
        return 1;
      bool stop_and_go = false;
      if ((argc > 3) && (string(argv[3]) == "stop_and_go"))
        stop_and_go = true;
      retval = loader_talk::MainReload(argv[2], stop_and_go);
      if ((retval != 0) && (stop_and_go)) {
        CreateFile(string(argv[2]) + ".paused.crashed", 0600);
      }
      return retval;
    }

    if (string(argv[1]) == string("__MK_ALIEN_CACHE__")) {
      if (argc < 5)
        return 1;
      string alien_cache_dir = argv[2];
      sanitizer::PositiveIntegerSanitizer sanitizer;
      if (!sanitizer.IsValid(argv[3]) || !sanitizer.IsValid(argv[4]))
        return 1;
      uid_t uid_owner = String2Uint64(argv[3]);
      gid_t gid_owner = String2Uint64(argv[4]);

      int retval = MkdirDeep(alien_cache_dir, 0770);
      if (!retval) {
        LogCvmfs(kLogCvmfs, kLogStderr, "Failed to create %s",
                 alien_cache_dir.c_str());
        return 1;
      }
      retval = chown(alien_cache_dir.c_str(), uid_owner, gid_owner);
      if (retval != 0) {
        LogCvmfs(kLogCvmfs, kLogStderr, "Failed to set owner of %s to %d:%d",
                 alien_cache_dir.c_str(), uid_owner, gid_owner);
        return 1;
      }
      retval = SwitchCredentials(uid_owner, gid_owner, false);
      if (!retval) {
        LogCvmfs(kLogCvmfs, kLogStderr, "Failed to impersonate %d:%d",
                 uid_owner, gid_owner);
        return 1;
      }
      // Allow access to user and group
      retval = MakeCacheDirectories(alien_cache_dir, 0770);
      if (!retval) {
        LogCvmfs(kLogCvmfs, kLogStderr, "Failed to create cache skeleton");
        return 1;
      }
      return 0;
    }

    debug_mode_ = getenv("__CVMFS_DEBUG_MODE__") != NULL;
    cvmfs_exports_ = LoadLibrary(debug_mode_, NULL);
    if (!cvmfs_exports_)
      return kFailLoadLibrary;
    return cvmfs_exports_->fnAltProcessFlavor(argc, argv);
  }

  SetupLibcryptoMt();

  // Option parsing
  struct fuse_args *mount_options;
  mount_options = ParseCmdLine(argc, argv);
  if (!mount_options) {
    Usage(argv[0]);
    return kFailOptions;
  }

  string parameter;
  OptionsManager *options_manager;
  if (simple_options_parsing_) {
    options_manager = new SimpleOptionsParser(
      new DefaultOptionsTemplateManager(*repository_name_));
  } else {
    options_manager = new BashOptionsManager(
      new DefaultOptionsTemplateManager(*repository_name_));
  }
  if (config_files_) {
    vector<string> tokens = SplitString(*config_files_, ':');
    for (unsigned i = 0, s = tokens.size(); i < s; ++i) {
      options_manager->ParsePath(tokens[i], false);
    }
  } else {
    options_manager->ParseDefault(*repository_name_);
  }

#ifdef __APPLE__
  string volname = "-ovolname=" + *repository_name_;
  fuse_opt_add_arg(mount_options, volname.c_str());
  // Allow for up to 5 minute "hangs" before OS X may kill cvmfs
  fuse_opt_add_arg(mount_options, "-odaemon_timeout=300");
  fuse_opt_add_arg(mount_options, "-onoapplexattr");
  // Should libfuse be single-threaded?  See CVM-871, CVM-855
  // single_threaded_ = true;
#endif
  if (options_manager->GetValue("CVMFS_MOUNT_RW", &parameter) &&
      options_manager->IsOn(parameter))
  {
    fuse_opt_add_arg(mount_options, "-orw");
  } else {
    fuse_opt_add_arg(mount_options, "-oro");
  }
  fuse_opt_add_arg(mount_options, "-onodev");
  if (options_manager->GetValue("CVMFS_SUID", &parameter) &&
      options_manager->IsOn(parameter))
  {
    suid_mode_ = true;
  }
  if (suid_mode_) {
    if (getuid() != 0) {
      PANIC(kLogStderr | kLogSyslogErr,
            "must be root to mount with suid option");
    }
    fuse_opt_add_arg(mount_options, "-osuid");
    LogCvmfs(kLogCvmfs, kLogStdout, "CernVM-FS: running with suid support");
  }
  loader_exports_ = new LoaderExports();
  loader_exports_->loader_version = PACKAGE_VERSION;
  loader_exports_->boot_time = time(NULL);
  loader_exports_->program_name = argv[0];
  loader_exports_->foreground = foreground_;
  loader_exports_->repository_name = *repository_name_;
  loader_exports_->mount_point = *mount_point_;
  loader_exports_->device_id = "0:0"; // initially unknown, set after mount
  loader_exports_->disable_watchdog = disable_watchdog_;
  loader_exports_->simple_options_parsing = simple_options_parsing_;
  if (config_files_)
    loader_exports_->config_files = *config_files_;
  else
    loader_exports_->config_files = "";

  if (parse_options_only_) {
    LogCvmfs(kLogCvmfs, kLogStdout, "# CernVM-FS parameters:\n%s",
             options_manager->Dump().c_str());
    return 0;
  }

  // Logging
  if (options_manager->GetValue("CVMFS_SYSLOG_LEVEL", &parameter))
    SetLogSyslogLevel(String2Uint64(parameter));
  else
    SetLogSyslogLevel(3);
  if (options_manager->GetValue("CVMFS_SYSLOG_FACILITY", &parameter))
    SetLogSyslogFacility(String2Int64(parameter));
  SetLogSyslogPrefix(*repository_name_);
  // Deferr setting usyslog until credentials are dropped

  // Permissions check
  if (options_manager->GetValue("CVMFS_CHECK_PERMISSIONS", &parameter)) {
    if (options_manager->IsOn(parameter)) {
      fuse_opt_add_arg(mount_options, "-odefault_permissions");
    }
  }

  if (!premounted_ && !DirectoryExists(*mount_point_)) {
    LogCvmfs(kLogCvmfs, kLogStderr | kLogSyslogErr,
             "Moint point %s does not exist", mount_point_->c_str());
    return kFailPermission;
  }

  // Number of file descriptors
  if (options_manager->GetValue("CVMFS_NFILES", &parameter)) {
    int retval = SetLimitNoFile(String2Uint64(parameter));
    if (retval == -2) {
      LogCvmfs(kLogCvmfs, kLogStdout, "CernVM-FS: running under valgrind");
    } else if (retval == -1) {
      if (system_mount_) {
        LogCvmfs(kLogCvmfs, kLogStderr | kLogSyslogErr,
                 "Failed to set maximum number of open files, "
                 "insufficient permissions");
        return kFailPermission;
      }
      unsigned soft_limit, hard_limit;
      GetLimitNoFile(&soft_limit, &hard_limit);
      LogCvmfs(kLogCvmfs, kLogStdout | kLogSyslogWarn,
               "Failed to set requested number of open files, "
               "using maximum number %u", hard_limit);
      if (hard_limit > soft_limit) {
        (void) SetLimitNoFile(hard_limit);
      }
    }
  }

  // Apply OOM score adjustment
  if (options_manager->GetValue("CVMFS_OOM_SCORE_ADJ", &parameter)) {
    string proc_path = "/proc/" + StringifyInt(getpid()) + "/oom_score_adj";
    int fd_oom = open(proc_path.c_str(), O_WRONLY);
    if (fd_oom < 0) {
      LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslogWarn,
               "failed to open %s", proc_path.c_str());
    } else {
      bool retval = SafeWrite(fd_oom, parameter.data(), parameter.length());
      if (!retval) {
        LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslogWarn,
                 "failed to set OOM score adjustment to %s", parameter.c_str());
      }
      close(fd_oom);
    }
  }

  // Protect the process from being killed by systemd
  if (options_manager->GetValue("CVMFS_SYSTEMD_NOKILL", &parameter) &&
      options_manager->IsOn(parameter))
  {
    argv[0][0] = '@';
  }

  // Grab mountpoint
  if (grab_mountpoint_) {
    if ((chown(mount_point_->c_str(), uid_, gid_) != 0) ||
        (chmod(mount_point_->c_str(), 0755) != 0))
    {
      LogCvmfs(kLogCvmfs, kLogStderr | kLogSyslogErr,
               "Failed to grab mountpoint %s (%d)",
               mount_point_->c_str(), errno);
      return kFailPermission;
    }
  }

  // Drop credentials
  if ((uid_ != 0) || (gid_ != 0)) {
    LogCvmfs(kLogCvmfs, kLogStdout, "CernVM-FS: running with credentials %d:%d",
             uid_, gid_);
    const bool retrievable = (suid_mode_ || !disable_watchdog_);
    if (!SwitchCredentials(uid_, gid_, retrievable)) {
      LogCvmfs(kLogCvmfs, kLogStderr | kLogSyslogErr,
               "Failed to drop credentials");
      return kFailPermission;
    }
  }

  // Only set usyslog now, otherwise file permissions are wrong
  usyslog_path_ = new string();
  if (options_manager->GetValue("CVMFS_USYSLOG", &parameter))
    *usyslog_path_ = parameter;
  SetLogMicroSyslog(*usyslog_path_);

  if (single_threaded_) {
    LogCvmfs(kLogCvmfs, kLogStdout,
             "CernVM-FS: running in single threaded mode");
  }
  if (debug_mode_) {
    LogCvmfs(kLogCvmfs, kLogStdout | kLogSyslogWarn,
             "CernVM-FS: running in debug mode");
  }

#ifndef FUSE_CAP_POSIX_ACL
  if (options_manager->GetValue("CVMFS_ENFORCE_ACLS", &parameter) &&
      options_manager->IsOn(parameter))
  {
    LogCvmfs(kLogCvmfs, kLogStderr | kLogSyslogErr,
             "CernVM-FS: ACL support requested but not available in this "
             "version of libfuse");
    return kFailPermission;
  }
#endif

  // Initialize the loader socket, connections are not accepted until Spawn()
  socket_path_ = new string("/var/run/cvmfs");
  if (options_manager->GetValue("CVMFS_RELOAD_SOCKETS", &parameter))
    *socket_path_ = MakeCanonicalPath(parameter);
  *socket_path_ += "/cvmfs." + *repository_name_;
  retval = loader_talk::Init(*socket_path_);
  if (!retval) {
    LogCvmfs(kLogCvmfs, kLogStderr | kLogSyslogErr,
             "Failed to initialize loader socket");
    return kFailLoaderTalk;
  }

  // Options are not needed anymore
  delete options_manager;
  options_manager = NULL;

  struct fuse_session *session;
#if CVMFS_USE_LIBFUSE == 2
  struct fuse_chan *channel;
  loader_exports_->fuse_channel_or_session = reinterpret_cast<void **>(
    &channel);
#else
  loader_exports_->fuse_channel_or_session = reinterpret_cast<void **>(
    &session);
#endif

  // Load and initialize cvmfs library
  LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak,
           "CernVM-FS: loading Fuse module... ");
  cvmfs_exports_ = LoadLibrary(debug_mode_, loader_exports_);
  if (!cvmfs_exports_) {
    return kFailLoadLibrary;
  }
  retval = cvmfs_exports_->fnInit(loader_exports_);
  if (retval != kFailOk) {
    if (retval == kFailDoubleMount) {
      LogCvmfs(kLogCvmfs, kLogStderr,
               "\nCernVM-FS: repository %s already mounted on %s",
               loader_exports_->repository_name.c_str(),
               loader_exports_->mount_point.c_str());
      return 0;
    }
    LogCvmfs(kLogCvmfs, kLogStderr | kLogSyslogErr, "%s (%d - %s)",
             cvmfs_exports_->fnGetErrorMsg().c_str(),
             retval, Code2Ascii((Failures)retval));
    cvmfs_exports_->fnFini();
    return retval;
  }
  LogCvmfs(kLogCvmfs, kLogStdout, "done");

  // Mount
  fence_reload_ = new Fence();

  if (suid_mode_) {
    const bool retrievable = true;
    if (!SwitchCredentials(0, getgid(), retrievable)) {
      LogCvmfs(kLogCvmfs, kLogStderr | kLogSyslogErr,
               "failed to re-gain root permissions for mounting");
      cvmfs_exports_->fnFini();
      return kFailPermission;
    }
  }


  struct fuse_lowlevel_ops loader_operations;
  SetFuseOperations(&loader_operations);
#if (FUSE_VERSION >= 29)
  if (cvmfs_exports_->cvmfs_operations.forget_multi)
    loader_operations.forget_multi = stub_forget_multi;
#endif

#if CVMFS_USE_LIBFUSE == 2
  channel = fuse_mount(mount_point_->c_str(), mount_options);
  if (!channel) {
    LogCvmfs(kLogCvmfs, kLogStderr | kLogSyslogErr,
             "failed to create Fuse channel");
    cvmfs_exports_->fnFini();
    return kFailMount;
  }

  session = fuse_lowlevel_new(mount_options, &loader_operations,
                              sizeof(loader_operations), NULL);
  if (!session) {
    LogCvmfs(kLogCvmfs, kLogStderr | kLogSyslogErr,
             "failed to create Fuse session");
    fuse_unmount(mount_point_->c_str(), channel);
    cvmfs_exports_->fnFini();
    return kFailMount;
  }
#else
  // libfuse3
  session = fuse_session_new(mount_options, &loader_operations,
                             sizeof(loader_operations), NULL);
  if (!session) {
    LogCvmfs(kLogCvmfs, kLogStderr | kLogSyslogErr,
             "failed to create Fuse session");
    cvmfs_exports_->fnFini();
    return kFailMount;
  }
  retval = fuse_session_mount(session, mount_point_->c_str());
  if (retval != 0) {
    LogCvmfs(kLogCvmfs, kLogStderr | kLogSyslogErr,
             "failed to mount file system");
    cvmfs_exports_->fnFini();
    return kFailMount;
  }
#endif

  // drop credentials
  if (suid_mode_) {
    const bool retrievable = !disable_watchdog_;
    if (!SwitchCredentials(uid_, gid_, retrievable)) {
      LogCvmfs(kLogCvmfs, kLogStderr | kLogSyslogErr,
               "failed to drop permissions after mounting");
      cvmfs_exports_->fnFini();
      return kFailPermission;
    }
  }

  // Determine device id
  int fd_mountinfo = open("/proc/self/mountinfo", O_RDONLY);
  if (fd_mountinfo > 0) {
    std::string line;
    while (GetLineFd(fd_mountinfo, &line)) {
      std::vector<std::string> tokens = SplitString(line, ' ');
      if (tokens.size() < 5) continue;
      if (tokens[4] != loader_exports_->mount_point) continue;
      unsigned i = 5;
      for (; i < tokens.size(); ++i) {
        if (tokens[i] == "-") break;
      }
      if (tokens.size() < i + 3) continue;
      if (tokens[i + 2] != "cvmfs2") continue;
      loader_exports_->device_id = tokens[2];
      break;
    }
    close(fd_mountinfo);
  }

  if (!premounted_) {
    LogCvmfs(kLogCvmfs, kLogStdout, "CernVM-FS: mounted cvmfs on %s",
             mount_point_->c_str());
  }
  LogCvmfs(kLogCvmfs, kLogSyslog,
           "CernVM-FS: linking %s to repository %s",
           mount_point_->c_str(), repository_name_->c_str());
  if (!foreground_)
    Daemonize();

  cvmfs_exports_->fnSpawn();
  loader_talk::Spawn();

  SetLogMicroSyslog("");
  retval = fuse_set_signal_handlers(session);
  assert(retval == 0);
#if CVMFS_USE_LIBFUSE == 2
  fuse_session_add_chan(session, channel);
#endif
  if (single_threaded_) {
    retval = fuse_session_loop(session);
  } else {
#if CVMFS_USE_LIBFUSE == 2
    retval = fuse_session_loop_mt(session);
#else
    retval = fuse_session_loop_mt(session, 1 /* use fd per thread */);
#endif
  }
  SetLogMicroSyslog(*usyslog_path_);

  loader_talk::Fini();
  cvmfs_exports_->fnFini();

  // Unmount
#if CVMFS_USE_LIBFUSE == 2
  fuse_remove_signal_handlers(session);
  fuse_session_remove_chan(channel);
  fuse_session_destroy(session);
  fuse_unmount(mount_point_->c_str(), channel);
  channel = NULL;
#else
  // libfuse3
  fuse_remove_signal_handlers(session);
  fuse_session_unmount(session);
  fuse_session_destroy(session);
#endif
  fuse_opt_free_args(mount_options);
  delete mount_options;
  session = NULL;
  mount_options = NULL;

  CloseLibrary();

  LogCvmfs(kLogCvmfs, kLogSyslog, "CernVM-FS: unmounted %s (%s)",
           mount_point_->c_str(), repository_name_->c_str());

  CleanupLibcryptoMt();

  delete fence_reload_;
  delete loader_exports_;
  delete config_files_;
  delete repository_name_;
  delete mount_point_;
  delete socket_path_;
  fence_reload_ = NULL;
  loader_exports_ = NULL;
  config_files_ = NULL;
  repository_name_ = NULL;
  mount_point_ = NULL;
  socket_path_ = NULL;

  if (retval != 0)
    return kFailFuseLoop;
  return kFailOk;
}


__attribute__((visibility("default")))
CvmfsStubExports *g_cvmfs_stub_exports = NULL;

static void __attribute__((constructor)) LibraryMain() {
  g_cvmfs_stub_exports = new CvmfsStubExports();
  g_cvmfs_stub_exports->fn_main = FuseMain;
}

static void __attribute__((destructor)) LibraryExit() {
  delete g_cvmfs_stub_exports;
  g_cvmfs_stub_exports = NULL;
}
