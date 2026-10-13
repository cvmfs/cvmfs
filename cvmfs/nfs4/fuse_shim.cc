/**
 * This file is part of the CernVM File System.
 *
 * Implements the subset of the libfuse3 API used by the cvmfs loader and
 * client on top of the built-in NFSv4 server.
 */

#include "nfs4/fuse_shim.h"
#include "nfs4/fuse_shim_api.h"

#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <spawn.h>
#include <sys/mount.h>
#include <sys/param.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <climits>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <strings.h>
#include <string>
#include <vector>

#include "nfs4/filehandle.h"
#include "nfs4/fuse_backend.h"
#include "nfs4/guardian.h"
#include "nfs4/logging.h"
#include "nfs4/mount_util.h"
#include "nfs4/nfs4_server.h"
#include "nfs4/rpc.h"

#ifdef __APPLE__
#include <mach-o/dyld.h>
#include <sys/sysctl.h>
#include <sys/ucred.h>
#else
#include <mntent.h>
#endif

extern char **environ;

using nfs4::IsMounted;
using nfs4::RunCommand;
using nfs4::UnmountNfs;

fuse_req::fuse_req() : reply(kNone), err(0), xattr_size(0) {
  memset(&entry, 0, sizeof(entry));
  memset(&attr, 0, sizeof(attr));
  memset(&fi, 0, sizeof(fi));
  memset(&statfs, 0, sizeof(statfs));
  memset(&ctx, 0, sizeof(ctx));
  ctx.uid = geteuid();
  ctx.gid = getegid();
  ctx.umask = 022;
}

struct fuse_session {
  fuse_session()
      : userdata(NULL), exited(false), init_called(false),
        destroy_called(false), mounted(false), workers(16), backend(NULL),
        server(NULL), rpc(NULL), guardian_fd(-1) {
    memset(&ops, 0, sizeof(ops));
  }

  struct fuse_lowlevel_ops ops;
  void *userdata;
  std::string mountpoint;
  std::atomic<bool> exited;
  bool init_called;
  bool destroy_called;
  bool mounted;
  unsigned workers;
  nfs4::FuseBackend *backend;
  nfs4::Nfs4Server *server;
  nfs4::RpcServer *rpc;
  /** Write end of the pipe whose EOF tells the guardian that we are gone */
  int guardian_fd;
};

#if FUSE_USE_VERSION >= FUSE_MAKE_VERSION(3, 12)
struct fuse_loop_config {
  fuse_loop_config() : max_threads(0) { }
  unsigned max_threads;
};
#endif

namespace {

// Seconds after which an unused open file is released
const unsigned kOpenFileIdleTimeout = 30;

// Written to once the NFS mount is in place (or failed), see
// cvmfs_nfsfuse_set_ready_fd()
int g_ready_fd = -1;

void NotifyReady(bool mounted) {
  if (g_ready_fd < 0)
    return;
  const char status = mounted ? 0 : 1;
  const ssize_t retval = write(g_ready_fd, &status, 1);
  (void)retval;
  close(g_ready_fd);
  g_ready_fd = -1;
  // The daemon kept the terminal for startup errors until now
  const int null_fd = open("/dev/null", O_RDWR);
  if (null_fd >= 0) {
    dup2(null_fd, 0);
    dup2(null_fd, 1);
    dup2(null_fd, 2);
    if (null_fd > 2)
      close(null_fd);
  }
}

// Wakes up the session loop from the signal handler
int g_wakeup_pipe[2] = {-1, -1};
struct fuse_session *g_signal_session = NULL;

void ExitHandler(int /* signal */) {
  if (g_signal_session)
    g_signal_session->exited = true;
  if (g_wakeup_pipe[1] >= 0) {
    const char c = 'x';
    const ssize_t retval = write(g_wakeup_pipe[1], &c, 1);
    (void)retval;
  }
}

bool ExecutablePath(std::string *path) {
  char buf[PATH_MAX];
#ifdef __APPLE__
  uint32_t size = sizeof(buf);
  if (_NSGetExecutablePath(buf, &size) != 0)
    return false;
#else
  const ssize_t len = readlink("/proc/self/exe", buf, sizeof(buf) - 1);
  if (len <= 0)
    return false;
  buf[len] = '\0';
#endif
  *path = buf;
  return true;
}

/**
 * Starts `cvmfs2 __nfs_guardian__`, see nfs4::RunGuardian().  It execs right
 * after fork, because this process may already run threads.
 */
/**
 * Asks the guardian, which runs as root, to mount ('M') or unmount ('U').
 * cvmfs2 itself may have dropped its privileges to CVMFS_USER by then.
 */
bool GuardianRequest(struct fuse_session *se, char command) {
  if (write(se->guardian_fd, &command, 1) != 1)
    return false;
  char status = 1;
  ssize_t n;
  do {
    n = read(se->guardian_fd, &status, 1);
  } while (n < 0 && errno == EINTR);
  return n == 1 && status == 0;
}

bool MountNfs(struct fuse_session *se) {
  if (se->guardian_fd >= 0)
    return GuardianRequest(se, 'M');
  return nfs4::MountNfs(se->mountpoint, se->rpc->port());
}

bool Unmount(struct fuse_session *se) {
  if (se->guardian_fd >= 0)
    return GuardianRequest(se, 'U');
  return UnmountNfs(se->mountpoint);
}

void SpawnGuardian(struct fuse_session *se) {
  std::string self;
  if (!ExecutablePath(&self)) {
    nfs4::Log(nfs4::kLogLevelWarn, "cannot start NFS guardian: no exe path");
    return;
  }
  // control[0] goes to the guardian
  int control[2];
  if (socketpair(AF_UNIX, SOCK_STREAM, 0, control) != 0)
    return;
  const int listen_fd = se->rpc->listen_fd();
  char listen_str[16], control_str[16];
  snprintf(listen_str, sizeof(listen_str), "%d", listen_fd);
  snprintf(control_str, sizeof(control_str), "%d", control[0]);
  const char *const argv[] = {self.c_str(), "__nfs_guardian__", listen_str,
                        control_str, se->mountpoint.c_str(), NULL};
  struct rlimit limit;
  getrlimit(RLIMIT_NOFILE, &limit);
  const int max_fd = (limit.rlim_cur > 65536) ? 65536
                                               : static_cast<int>(limit.rlim_cur);

  const pid_t pid = fork();
  if (pid == 0) {
    // Only async-signal-safe calls until exec
    const int null_fd = open("/dev/null", O_RDWR);
    if (null_fd >= 0) {
      dup2(null_fd, 0);
      dup2(null_fd, 1);
      dup2(null_fd, 2);
    }
    for (int fd = 3; fd < max_fd; ++fd) {
      if (fd != listen_fd && fd != control[0])
        close(fd);
    }
    setsid();
    execv(argv[0], const_cast<char *const *>(argv));
    _exit(127);
  }
  close(control[0]);
  if (pid < 0) {
    close(control[1]);
    return;
  }
  // Children forked later (e.g. mount_nfs) must not keep the pipe open
  fcntl(control[1], F_SETFD, FD_CLOEXEC);
  se->guardian_fd = control[1];
}

/**
 * The FUSE kernel module keeps file descriptors of open files out of the cvmfs
 * process; with NFS, cvmfs holds them itself (see FuseBackend), and macOS'
 * default limit of 256 is quickly exhausted.
 */
void RaiseFileDescriptorLimit() {
  const rlim_t kWanted = 65536;
  struct rlimit limit;
  if (getrlimit(RLIMIT_NOFILE, &limit) != 0)
    return;
#ifdef __APPLE__
  int max_per_proc = 0;
  size_t len = sizeof(max_per_proc);
  const bool capped =
      sysctlbyname("kern.maxfilesperproc", &max_per_proc, &len, NULL, 0) == 0
      && max_per_proc > 0 && static_cast<rlim_t>(max_per_proc) < kWanted;
  const rlim_t target = capped ? static_cast<rlim_t>(max_per_proc) : kWanted;
#else
  const rlim_t target = kWanted;
#endif
  if (limit.rlim_cur >= target)
    return;
  const nfs4::ScopedRoot root;
  struct rlimit wanted = limit;
  wanted.rlim_cur = target;
  if (wanted.rlim_max < target)
    wanted.rlim_max = target;  // only possible as root
  if (setrlimit(RLIMIT_NOFILE, &wanted) != 0) {
    wanted.rlim_max = limit.rlim_max;
    wanted.rlim_cur = limit.rlim_max;
    setrlimit(RLIMIT_NOFILE, &wanted);
  }
  getrlimit(RLIMIT_NOFILE, &limit);
  nfs4::Log(nfs4::kLogLevelInfo, "open file limit %llu/%llu",
            static_cast<unsigned long long>(limit.rlim_cur),  // NOLINT
            static_cast<unsigned long long>(limit.rlim_max));  // NOLINT
}

void CallInit(struct fuse_session *se) {
  if (se->init_called || !se->ops.init)
    return;
  struct fuse_conn_info conn;
  memset(&conn, 0, sizeof(conn));
  conn.max_read = 1u << 20;
  se->ops.init(se->userdata, &conn);
  se->init_called = true;
}

int SessionLoop(struct fuse_session *se, unsigned max_threads) {
  if (max_threads > 0)
    se->rpc->SetWorkers(max_threads);
  CallInit(se);
  se->rpc->Start();
  const bool mounted = MountNfs(se);
  NotifyReady(mounted);
  if (!mounted) {
    se->rpc->Stop();
    return -1;
  }
  se->mounted = true;

  // Runs until a signal arrives or the file system is unmounted from outside
  while (!se->exited) {
    struct pollfd pfd;
    pfd.fd = g_wakeup_pipe[0];
    pfd.events = POLLIN;
    pfd.revents = 0;
    poll(&pfd, g_wakeup_pipe[0] >= 0 ? 1 : 0, 1000);
    se->backend->ReleaseIdle(kOpenFileIdleTimeout);
    if (!IsMounted(se->mountpoint)) {
      nfs4::Log(nfs4::kLogLevelInfo, "%s was unmounted",
                se->mountpoint.c_str());
      se->mounted = false;
      break;
    }
  }

  // Unmount while cvmfs can still answer requests
  int retval = 0;
  if (se->mounted) {
    if (Unmount(se)) {
      se->mounted = false;
    } else {
      nfs4::Log(nfs4::kLogLevelError, "failed to unmount %s",
                se->mountpoint.c_str());
      retval = -1;
    }
  }
  se->rpc->Stop();
  se->backend->ReleaseAll();
  return retval;
}

}  // anonymous namespace


void cvmfs_nfsfuse_set_ready_fd(int fd) { g_ready_fd = fd; }

int cvmfs_nfsfuse_guardian_main(int listen_fd, int control_fd,
                                const char *mountpoint) {
  return nfs4::RunGuardian(listen_fd, control_fd, mountpoint);
}


//------------------------------------------------------------------------------
// Option parsing (fuse_opt.h)


namespace {

struct OptContext {
  void *data;
  const struct fuse_opt *opts;
  fuse_opt_proc_t proc;
  struct fuse_args outargs;
  std::string kept_opts;
  bool nonopt;
};

int AddArg(struct fuse_args *args, const char *arg) {
  return fuse_opt_add_arg(args, arg);
}

bool MatchTemplate(const char *t, const char *arg, unsigned *sep_pos) {
  const size_t arglen = strlen(arg);
  const char *sep = strchr(t, '=');
  if (!sep)
    sep = strchr(t, ' ');
  if (sep && (!sep[1] || sep[1] == '%')) {
    size_t tlen = sep - t;
    if (sep[0] == '=')
      tlen++;
    if (arglen >= tlen && strncmp(arg, t, tlen) == 0) {
      *sep_pos = static_cast<unsigned>(sep - t);
      return true;
    }
  }
  if (strcmp(t, arg) == 0) {
    *sep_pos = 0;
    return true;
  }
  return false;
}

const struct fuse_opt *FindOpt(const struct fuse_opt *opt, const char *arg,
                               unsigned *sep_pos) {
  for (; opt && opt->templ; opt++) {
    if (MatchTemplate(opt->templ, arg, sep_pos))
      return opt;
  }
  return NULL;
}

int CallProc(OptContext *ctx, const char *arg, int key, bool is_opt) {
  if (key == FUSE_OPT_KEY_DISCARD)
    return 0;
  if (key != FUSE_OPT_KEY_KEEP && ctx->proc) {
    const int retval = ctx->proc(ctx->data, arg, key, &ctx->outargs);
    if (retval == -1 || retval == 0)
      return retval;
  }
  if (is_opt) {
    if (!ctx->kept_opts.empty())
      ctx->kept_opts += ",";
    ctx->kept_opts += arg;
    return 0;
  }
  return AddArg(&ctx->outargs, arg);
}

int ProcessOptParam(void *var, const char *format, const char *param) {
  if (strcmp(format, "%s") == 0) {
    char *copy = strdup(param);
    if (!copy)
      return -1;
    *reinterpret_cast<char **>(var) = copy;
    return 0;
  }
  if (sscanf(param, format, var) != 1) {
    fprintf(stderr, "fuse: invalid parameter in option `%s'\n", param);
    return -1;
  }
  return 0;
}

int ProcessGenericOpt(OptContext *ctx, const char *arg, bool is_opt) {
  unsigned sep;
  const struct fuse_opt *opt = FindOpt(ctx->opts, arg, &sep);
  if (!opt)
    return CallProc(ctx, arg, FUSE_OPT_KEY_OPT, is_opt);
  for (; opt; opt = FindOpt(opt + 1, arg, &sep)) {
    int retval;
    if (opt->offset == -1U) {
      retval = CallProc(ctx, arg, opt->value, is_opt);
    } else {
      void *var = static_cast<char *>(ctx->data) + opt->offset;
      if (sep && opt->templ[sep + 1]) {
        const char *param = arg + sep;
        if (opt->templ[sep] == '=')
          param++;
        retval = ProcessOptParam(var, opt->templ + sep + 1, param);
      } else {
        *static_cast<int *>(var) = opt->value;
        retval = 0;
      }
    }
    if (retval == -1)
      return -1;
  }
  return 0;
}

int ProcessOptionGroup(OptContext *ctx, const char *group) {
  std::string current;
  for (const char *p = group;; ++p) {
    if (*p == '\\' && p[1] != '\0') {
      current += *(++p);
      continue;
    }
    if (*p == ',' || *p == '\0') {
      if (!current.empty() && ProcessGenericOpt(ctx, current.c_str(), true) == -1)
        return -1;
      current.clear();
      if (*p == '\0')
        break;
      continue;
    }
    current += *p;
  }
  return 0;
}

}  // anonymous namespace


int fuse_opt_add_arg(struct fuse_args *args, const char *arg) {
  if (!args->allocated && args->argv) {
    // Take ownership of a copy of the caller's argument vector
    char **copy = static_cast<char **>(
        malloc((args->argc + 1) * sizeof(char *)));
    if (!copy)
      return -1;
    for (int i = 0; i < args->argc; ++i)
      copy[i] = strdup(args->argv[i]);
    copy[args->argc] = NULL;
    args->argv = copy;
  }
  char **argv = static_cast<char **>(
      realloc(args->argv, (args->argc + 2) * sizeof(char *)));
  if (!argv)
    return -1;
  argv[args->argc] = strdup(arg);
  argv[args->argc + 1] = NULL;
  args->argv = argv;
  args->argc++;
  args->allocated = 1;
  return 0;
}


void fuse_opt_free_args(struct fuse_args *args) {
  if (args && args->allocated) {
    for (int i = 0; i < args->argc; ++i)
      free(args->argv[i]);
    free(args->argv);
  }
  if (args) {
    args->argc = 0;
    args->argv = NULL;
    args->allocated = 0;
  }
}


int fuse_opt_parse(struct fuse_args *args, void *data,
                   const struct fuse_opt opts[], fuse_opt_proc_t proc) {
  if (!args || !args->argv || !args->argc)
    return 0;

  OptContext ctx;
  ctx.data = data;
  ctx.opts = opts;
  ctx.proc = proc;
  ctx.outargs.argc = 0;
  ctx.outargs.argv = NULL;
  ctx.outargs.allocated = 0;
  ctx.nonopt = false;

  int retval = AddArg(&ctx.outargs, args->argv[0]);
  for (int i = 1; retval != -1 && i < args->argc; ++i) {
    const char *arg = args->argv[i];
    if (ctx.nonopt || arg[0] != '-') {
      retval = CallProc(&ctx, arg, FUSE_OPT_KEY_NONOPT, false);
    } else if (arg[1] == 'o') {
      if (arg[2] != '\0') {
        retval = ProcessOptionGroup(&ctx, arg + 2);
      } else if (i + 1 < args->argc) {
        retval = ProcessOptionGroup(&ctx, args->argv[++i]);
      } else {
        fprintf(stderr, "fuse: missing argument after `-o'\n");
        retval = -1;
      }
    } else if (arg[1] == '-' && arg[2] == '\0') {
      retval = AddArg(&ctx.outargs, arg);
      ctx.nonopt = true;
    } else {
      retval = ProcessGenericOpt(&ctx, arg, false);
    }
  }

  if (retval != -1 && !ctx.kept_opts.empty()) {
    // Like libfuse, kept options go into one "-o a,b,c" right after argv[0]
    struct fuse_args reordered = {0, NULL, 0};
    AddArg(&reordered, ctx.outargs.argv[0]);
    AddArg(&reordered, "-o");
    AddArg(&reordered, ctx.kept_opts.c_str());
    for (int i = 1; i < ctx.outargs.argc; ++i)
      AddArg(&reordered, ctx.outargs.argv[i]);
    fuse_opt_free_args(&ctx.outargs);
    ctx.outargs = reordered;
  }

  if (retval == -1) {
    fuse_opt_free_args(&ctx.outargs);
    return -1;
  }
  fuse_opt_free_args(args);
  *args = ctx.outargs;
  return 0;
}


//------------------------------------------------------------------------------
// Session


#ifdef fuse_session_new
#undef fuse_session_new
#endif

static struct fuse_session *NewSession(struct fuse_args * /* args */,
                                       const struct fuse_lowlevel_ops *op,
                                       size_t op_size, void *userdata) {
  struct fuse_session *se = new fuse_session();
  memcpy(&se->ops, op, std::min(op_size, sizeof(se->ops)));
  se->userdata = userdata;
  return se;
}

#if FUSE_VERSION >= FUSE_MAKE_VERSION(3, 17)
struct fuse_session *fuse_session_new_versioned(
    struct fuse_args *args, const struct fuse_lowlevel_ops *op, size_t op_size,
    struct libfuse_version * /* version */, void *userdata) {
  return NewSession(args, op, op_size, userdata);
}
#else
struct fuse_session *fuse_session_new(struct fuse_args *args,
                                      const struct fuse_lowlevel_ops *op,
                                      size_t op_size, void *userdata) {
  return NewSession(args, op, op_size, userdata);
}
#endif


int fuse_session_mount(struct fuse_session *se, const char *mountpoint) {
  char resolved[PATH_MAX];
  if (realpath(mountpoint, resolved) == NULL) {
    nfs4::Log(nfs4::kLogLevelError, "invalid mount point %s (%d)", mountpoint,
              errno);
    return -1;
  }
  se->mountpoint = resolved;
  RaiseFileDescriptorLimit();

  se->backend = new nfs4::FuseBackend(
      &se->ops, (static_cast<uint64_t>(nfs4::HashName(se->mountpoint)) << 32)
                    | 1);
  nfs4::Nfs4Server::Options server_options;
  // Mounts are hard, so slow requests simply wait; a dead cvmfs2 is covered
  // by the guardian.  Answering NFS4ERR_DELAY instead costs a thread handoff
  // per request and is only useful for soft mounts.
  // On by default; CVMFS_NFS_DELEGATIONS is set by the loader from the
  // client configuration
  const char *delegations = getenv("CVMFS_NFS_DELEGATIONS");
  server_options.delegations =
      (delegations == NULL)
      || (strcmp(delegations, "0") != 0 && strcasecmp(delegations, "no") != 0
          && strcasecmp(delegations, "off") != 0
          && strcasecmp(delegations, "false") != 0);
  const char *delay_ms = getenv("CVMFS_NFS_DELAY_MS");
  if (delay_ms)
    server_options.delay_after_ms = static_cast<uint32_t>(atoi(delay_ms));
  se->server = new nfs4::Nfs4Server(se->backend, server_options);
  nfs4::RpcServer::Options rpc_options;
  rpc_options.workers = se->workers;
  se->rpc = new nfs4::RpcServer(se->server, rpc_options);
  const char *port = getenv("CVMFS_NFS_PORT");
  if (!se->rpc->Listen("127.0.0.1",
                       port ? static_cast<uint16_t>(atoi(port)) : 0)) {
    return -1;
  }
  if (getenv("CVMFS_NFS_NO_GUARDIAN") == NULL)
    SpawnGuardian(se);

  // The NFS mount happens in the session loop: the loader may daemonize
  // (fork) before, and cvmfs must not serve requests before it is fully
  // spawned.  See cvmfs_nfsfuse_set_ready_fd().
  return 0;
}


void fuse_session_unmount(struct fuse_session *se) {
  if (se->mounted && Unmount(se))
    se->mounted = false;
}


void fuse_session_destroy(struct fuse_session *se) {
  if (!se)
    return;
  if (se->init_called && !se->destroy_called && se->ops.destroy) {
    se->ops.destroy(se->userdata);
    se->destroy_called = true;
  }
  delete se->rpc;
  delete se->server;
  delete se->backend;
  delete se;
}


int fuse_session_fd(struct fuse_session *se) {
  return se->rpc ? se->rpc->listen_fd() : -1;
}


void fuse_session_exit(struct fuse_session *se) {
  se->exited = true;
  ExitHandler(0);
}


int fuse_session_exited(struct fuse_session *se) { return se->exited ? 1 : 0; }


int fuse_session_loop(struct fuse_session *se) { return SessionLoop(se, 1); }


#ifdef fuse_session_loop_mt
#undef fuse_session_loop_mt
#endif

extern "C" {
int fuse_session_loop_mt_31(struct fuse_session *se, int /* clone_fd */) {
  return SessionLoop(se, 0);
}
}

#if FUSE_USE_VERSION >= FUSE_MAKE_VERSION(3, 12)
extern "C" {
int fuse_session_loop_mt_312(struct fuse_session *se,
                             struct fuse_loop_config *config) {
  return SessionLoop(se, config ? config->max_threads : 0);
}
int fuse_session_loop_mt(struct fuse_session *se,
                         struct fuse_loop_config *config) {
  return SessionLoop(se, config ? config->max_threads : 0);
}
}


struct fuse_loop_config *fuse_loop_cfg_create(void) {
  return new fuse_loop_config();
}

void fuse_loop_cfg_destroy(struct fuse_loop_config *config) { delete config; }

void fuse_loop_cfg_set_idle_threads(struct fuse_loop_config * /* config */,
                                    unsigned int /* value */) { }

void fuse_loop_cfg_set_max_threads(struct fuse_loop_config *config,
                                   unsigned int value) {
  config->max_threads = value;
}

void fuse_loop_cfg_set_clone_fd(struct fuse_loop_config * /* config */,
                                unsigned int /* value */) { }
#endif  // FUSE_USE_VERSION >= 3.12


int fuse_set_signal_handlers(struct fuse_session *se) {
  if (g_wakeup_pipe[0] < 0) {
    if (pipe(g_wakeup_pipe) != 0)
      return -1;
    fcntl(g_wakeup_pipe[0], F_SETFL, O_NONBLOCK);
    fcntl(g_wakeup_pipe[1], F_SETFL, O_NONBLOCK);
  }
  g_signal_session = se;
  struct sigaction sa;
  memset(&sa, 0, sizeof(sa));
  sa.sa_handler = ExitHandler;
  sigemptyset(&sa.sa_mask);
  sigaction(SIGHUP, &sa, NULL);
  sigaction(SIGINT, &sa, NULL);
  sigaction(SIGTERM, &sa, NULL);
  signal(SIGPIPE, SIG_IGN);
  return 0;
}


void fuse_remove_signal_handlers(struct fuse_session * /* se */) {
  signal(SIGHUP, SIG_DFL);
  signal(SIGINT, SIG_DFL);
  signal(SIGTERM, SIG_DFL);
  g_signal_session = NULL;
}


//------------------------------------------------------------------------------
// Requests and replies


const struct fuse_ctx *fuse_req_ctx(fuse_req_t req) { return &req->ctx; }

int fuse_req_interrupted(fuse_req_t /* req */) { return 0; }

int fuse_reply_err(fuse_req_t req, int err) {
  req->reply = fuse_req::kErr;
  req->err = err;
  return 0;
}

void fuse_reply_none(fuse_req_t req) { req->reply = fuse_req::kErr; }

int fuse_reply_entry(fuse_req_t req, const struct fuse_entry_param *e) {
  req->reply = fuse_req::kEntry;
  req->entry = *e;
  return 0;
}

int fuse_reply_attr(fuse_req_t req, const struct stat *attr,
                    double /* attr_timeout */) {
  req->reply = fuse_req::kAttr;
  req->attr = *attr;
  return 0;
}

int fuse_reply_readlink(fuse_req_t req, const char *link) {
  req->reply = fuse_req::kReadlink;
  req->buf = link;
  return 0;
}

int fuse_reply_open(fuse_req_t req, const struct fuse_file_info *fi) {
  req->reply = fuse_req::kOpen;
  req->fi = *fi;
  return 0;
}

int fuse_reply_buf(fuse_req_t req, const char *buf, size_t size) {
  req->reply = fuse_req::kBuf;
  if (size > 0)
    req->buf.assign(buf, size);
  else
    req->buf.clear();
  return 0;
}

int fuse_reply_statfs(fuse_req_t req, const struct statvfs *stbuf) {
  req->reply = fuse_req::kStatfs;
  req->statfs = *stbuf;
  return 0;
}

int fuse_reply_xattr(fuse_req_t req, size_t count) {
  req->reply = fuse_req::kXattr;
  req->xattr_size = count;
  return 0;
}

size_t fuse_add_direntry(fuse_req_t /* req */, char *buf, size_t bufsize,
                         const char *name, const struct stat *stbuf,
                         off_t off) {
  // Same layout as libfuse (struct fuse_dirent), padded to 8 bytes
  const size_t namelen = strlen(name);
  const size_t entry_size = (24 + namelen + 7) & ~size_t(7);
  if (buf == NULL || entry_size > bufsize)
    return entry_size;
  const uint64_t ino = stbuf->st_ino;
  const uint64_t next = off;
  const uint32_t len = static_cast<uint32_t>(namelen);
  const uint32_t type = (stbuf->st_mode & S_IFMT) >> 12;
  memcpy(buf, &ino, 8);
  memcpy(buf + 8, &next, 8);
  memcpy(buf + 16, &len, 4);
  memcpy(buf + 20, &type, 4);
  memcpy(buf + 24, name, namelen);
  memset(buf + 24 + namelen, 0, entry_size - 24 - namelen);
  return entry_size;
}

// The kernel's NFS client has no invalidation channel; it relies on the
// attribute cache timeout instead
int fuse_lowlevel_notify_inval_inode(struct fuse_session * /* se */,
                                     fuse_ino_t /* ino */, off_t /* off */,
                                     off_t /* len */) {
  return -ENOSYS;
}

int fuse_lowlevel_notify_inval_entry(struct fuse_session * /* se */,
                                     fuse_ino_t /* parent */,
                                     const char * /* name */,
                                     size_t /* namelen */) {
  return -ENOSYS;
}

int fuse_lowlevel_notify_expire_entry(struct fuse_session * /* se */,
                                      fuse_ino_t /* parent */,
                                      const char * /* name */,
                                      size_t /* namelen */) {
  return -ENOSYS;
}

#ifdef FUSE_CAP_PASSTHROUGH
// Never called: the capability is not advertised in fuse_conn_info
int fuse_passthrough_open(fuse_req_t /* req */, int /* fd */) {
  return -ENOSYS;
}

int fuse_passthrough_close(fuse_req_t /* req */, int /* backing_id */) {
  return -ENOSYS;
}
#endif
