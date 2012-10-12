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
#define FUSE_USE_VERSION 26
#define _FILE_OFFSET_BITS 64

#include "cvmfs_config.h"
#include "cvmfs_loader.h"

#include <sys/resource.h>
#include <dlfcn.h>
#include <unistd.h>
#include <errno.h>
#include <sched.h>

#include <fuse/fuse_lowlevel.h>
#include <fuse/fuse_opt.h>

#include <cstdlib>
#include <cstring>

#include <vector>
#include <string>

#include "logging.h"
#include "options.h"
#include "util.h"
#include "atomic.h"

using namespace std;  // NOLINT

namespace loader {

// Follow the fuse convention for option parsing
struct CvmfsOptions {
  char *config;
  int uid;
  int gid;
  int grab_mountpoint;
};

enum {
  KEY_HELP,
  KEY_VERSION,
  KEY_FOREGROUND,
  KEY_SINGLETHREAD,
  KEY_DEBUG,
};
#define CVMFS_OPT(t, p, v) { t, offsetof(struct CvmfsOptions, p), v }
#define CVMFS_SWITCH(t, p) { t, offsetof(struct CvmfsOptions, p), 1 }
static struct fuse_opt cvmfs_array_opts[] = {
  CVMFS_OPT("config=%s",          config, 0),
  CVMFS_OPT("uid=%d",             uid, 0),
  CVMFS_OPT("gid=%d",             gid, 0),
  CVMFS_SWITCH("grab_mountpoint", grab_mountpoint),

  FUSE_OPT_KEY("-V",            KEY_VERSION),
  FUSE_OPT_KEY("--version",     KEY_VERSION),
  FUSE_OPT_KEY("-h",            KEY_HELP),
  FUSE_OPT_KEY("--help",        KEY_HELP),
  FUSE_OPT_KEY("-f",            KEY_FOREGROUND),
  FUSE_OPT_KEY("-d",            KEY_DEBUG),
  FUSE_OPT_KEY("debug",         KEY_DEBUG),
  FUSE_OPT_KEY("-s",            KEY_SINGLETHREAD),
  {0, 0, 0},
};


string *repository_name_ = NULL;
string *mount_point_ = NULL;
string *config_files_ = NULL;
uid_t uid_ = 0;
gid_t gid_ = 0;
bool single_threaded_ = false;
bool foreground_ = false;
bool debug_mode_ = false;
bool grab_mountpoint_ = false;
atomic_int32 blocking_;


static void Usage(const std::string &exename) {
  LogCvmfs(kLogCvmfs, kLogStdout,
    "The CernVM File System\n"
    "Version %s\n"
    "Copyright (c) 2009- CERN, all rights reserved\n\n"
    "Please visit http://cernvm.cern.ch for details.\n\n"
    "Usage: %s [-s] [-d] [-o mount options] <repository name> <mount point>\n"
    "CernVM-FS mount options:\n"
    "  -o config=FILES      colon-separated path list of config files\n"
    "  -o uid=UID           Drop credentials to another user\n"
    "  -o gid=GID           Drop credentials to another group\n"
    "  -o grab_mountpoint   give ownership of the mountpoint to the user "
                            "before mounting (required for autofs)\n\n"
    "Fuse mount options:\n"
    "  -o allow_other       allow access to other users\n"
    "  -o allow_root        allow access to root\n"
    "  -o nonempty          allow mounts over non-empty directory\n",
    PACKAGE_VERSION, exename.c_str()
  );
}


static inline void FileSystemFence() {
  while (atomic_read32(&blocking_)) {
    // Don't sleep, interferes with alarm()
    sched_yield();
  }
}


static void stub_init(void *userdata, struct fuse_conn_info *conn) {
  FileSystemFence();
}


static void stub_destroy(void *userdata) {
  FileSystemFence();
}


static void stub_lookup(fuse_req_t req, fuse_ino_t parent,
                        const char *name)
{
  FileSystemFence();
}


static void stub_getattr(fuse_req_t req, fuse_ino_t ino,
                         struct fuse_file_info *fi)
{
  FileSystemFence();
}


static void stub_readlink(fuse_req_t req, fuse_ino_t ino) {
  FileSystemFence();
}


static void stub_opendir(fuse_req_t req, fuse_ino_t ino,
                         struct fuse_file_info *fi)
{
  FileSystemFence();
}


static void stub_releasedir(fuse_req_t req, fuse_ino_t ino,
                            struct fuse_file_info *fi)
{
  FileSystemFence();
}


static void stub_readdir(fuse_req_t req, fuse_ino_t ino, size_t size,
                         off_t off, struct fuse_file_info *fi)
{
  FileSystemFence();
}


static void stub_open(fuse_req_t req, fuse_ino_t ino,
                      struct fuse_file_info *fi)
{
  FileSystemFence();
}


static void stub_read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                       struct fuse_file_info *fi)
{
  FileSystemFence();
}


static void stub_release(fuse_req_t req, fuse_ino_t ino,
                         struct fuse_file_info *fi)
{
  FileSystemFence();
}


static void stub_statfs(fuse_req_t req, fuse_ino_t ino) {
  FileSystemFence();
}


#ifdef __APPLE__
static void stub_getxattr(fuse_req_t req, fuse_ino_t ino, const char *name,
                          size_t size, uint32_t position)
#else
static void stub_getxattr(fuse_req_t req, fuse_ino_t ino, const char *name,
                          size_t size)
#endif
{
  FileSystemFence();
}


static void stub_listxattr(fuse_req_t req, fuse_ino_t ino, size_t size) {
  FileSystemFence();
}


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
      if (!repository_name_) {
        repository_name_ = new string(arg);
      } else {
        if (mount_point_)
          return 1;
        mount_point_ = new string(arg);
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
    case KEY_DEBUG:
      fuse_opt_add_arg(outargs, "-d");
      debug_mode_ = true;
      return 0;
    default:
      LogCvmfs(kLogCvmfs, kLogStderr, "internal option parsing error");
      abort();
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
  grab_mountpoint_ = cvmfs_options.grab_mountpoint;

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
}

}  // namespace loader


using namespace loader;

int main(int argc, char *argv[]) {
  // Set a decent umask for new files (no write access to group/everyone).
  // We want to allow group write access for the talk-socket.
  umask(007);

  int retval;

  // Jump into alternative process flavors
  if (argc > 1) {
    if (strcmp(argv[1], "__peersrv__") == 0) {
      //return peers::MainPeerServer(argc, argv);
      // TODO
      return 1;
    }
    if (strcmp(argv[1], "__cachemgr__") == 0) {
      //return quota::MainCacheManager(argc, argv);
      // TODO
      return 1;
    }
  }

  // Option parsing
  struct fuse_args *mount_options;
  mount_options = ParseCmdLine(argc, argv);
  if (!mount_options) {
    Usage(argv[0]);
    return kFailOptions;
  }
  options::Init();
  if (config_files_) {
    vector<string> tokens = SplitString(*config_files_, ':');
    for (unsigned i = 0, s = tokens.size(); i < s; ++i) {
      options::ParsePath(tokens[i]);
    }
  } else {
    options::ParseDefault(*repository_name_);
  }

  string parameter;

  // Logging
  if (options::GetValue("CVMFS_SYSLOG_LEVEL", &parameter))
    SetLogSyslogLevel(String2Uint64(parameter));
  else
    SetLogSyslogLevel(3);
  SetLogSyslogPrefix(*repository_name_);

  // Number of file descriptors
  if (options::GetValue("CVMFS_NFILES", &parameter)) {
    uint64_t nfiles = String2Uint64(parameter);
    struct rlimit rpl;
    memset(&rpl, 0, sizeof(rpl));
    getrlimit(RLIMIT_NOFILE, &rpl);
    if (rpl.rlim_max < nfiles)
      rpl.rlim_max = nfiles;
    rpl.rlim_cur = nfiles;
    retval = setrlimit(RLIMIT_NOFILE, &rpl);
    if (retval != 0) {
      PrintError("Failed to set maximum number of open files, "
                 "insufficient permissions");
      return kFailPermission;
    }
  }

  // Grab mountpoint
  if (grab_mountpoint_) {
    if ((chown(mount_point_->c_str(), uid_, gid_) != 0) ||
        (chmod(mount_point_->c_str(), 0755) != 0))
    {
      PrintError("Failed to grab mountpoint (" + StringifyInt(errno) + ")");
      return kFailPermission;
    }
  }

  // Drop credentials
  if ((uid_ != 0) || (gid_ != 0)) {
    LogCvmfs(kLogCvmfs, kLogStdout, "CernVM-FS: running with credentials %d:%d",
             uid_, gid_);
    if ((setgid(gid_) != 0) || (setuid(uid_) != 0)) {
      PrintError("Failed to drop credentials");
      return kFailPermission;
    }
  }

  if (single_threaded_) {
    LogCvmfs(kLogCvmfs, kLogStdout,
             "CernVM-FS: running in single threaded mode");
  }
  if (debug_mode_) {
    LogCvmfs(kLogCvmfs, kLogStdout,
             "CernVM-FS: running in debug mode");
  }

  // Options are not needed anymore
  options::Fini();

  // Mount
  LogCvmfs(kLogCvmfs, kLogSyslog,
           "CernVM-FS: linking %s to repository %s",
           mount_point_->c_str(), repository_name_->c_str());
  atomic_init32(&blocking_);
  atomic_cas32(&blocking_, 0, 1);

  struct fuse_chan *channel;
  channel = fuse_mount(mount_point_->c_str(), mount_options);
  if (!channel) {
    PrintError("Failed to create Fuse channel");
    return kFailMount;
  }
  LogCvmfs(kLogCvmfs, kLogStdout, "CernVM-FS: mounted cvmfs on %s",
           mount_point_->c_str());

  struct fuse_lowlevel_ops loader_operations;
  SetFuseOperations(&loader_operations);
  struct fuse_session *session;
  session = fuse_lowlevel_new(mount_options, &loader_operations,
                              sizeof(loader_operations), NULL);
  if (!session) {
    PrintError("Failed to create Fuse session");
    return kFailMount;
  }

  if (!foreground_)
    Daemonize();

  retval = fuse_set_signal_handlers(session);
  assert(retval == 0);
  fuse_session_add_chan(session, channel);
  if (single_threaded_)
    retval = fuse_session_loop(session);
  else
    retval = fuse_session_loop_mt(session);

  // Unmount
  fuse_session_remove_chan(channel);
  fuse_remove_signal_handlers(session);
  fuse_session_destroy(session);
  fuse_unmount(mount_point_->c_str(), channel);
  fuse_opt_free_args(mount_options);

  LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslog, "CernVM-FS: unmounted %s (%s)",
           mount_point_->c_str(), repository_name_->c_str());

  if (retval != 0)
    return kFailFuseLoop;
  return kFailOk;

  printf("start\n");
  void *dl_cvmfs = dlopen("libcvmfs_fuse.so", RTLD_NOW | RTLD_LOCAL);
  if (!dl_cvmfs)
    printf("ERROR: %s\n", dlerror());
  if (dlclose(dl_cvmfs) != 0)
    printf("ERROR: %s\n", dlerror());
  dl_cvmfs = dlopen("libcvmfs_fuse.so", RTLD_NOW | RTLD_LOCAL);
  if (!dl_cvmfs)
    printf("ERROR: %s\n", dlerror());
  if (dlclose(dl_cvmfs) != 0)
    printf("ERROR: %s\n", dlerror());
  printf("stop\n");
  return 0;
}
