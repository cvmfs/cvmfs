/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_CLIENTCTX_H_
#define CVMFS_CLIENTCTX_H_

#include <pthread.h>
#include <unistd.h>

#include <cassert>
#include <vector>

class InterruptCue;

/**
 * A client context associates a file system call with the uid, gid, and pid
 * of the calling process and an InterruptCue of the file system request.
 * For the library, the calling process is just the current process and
 * user, the InterruptCue is the default one.
 * For the Fuse module, the uid and gid are provided by fuse, the pid
 * can be figured out from the system. The InterruptCue is the FuseInterruptCue
 * defined in cvmfs.cc.  A client context is used to download files with the
 * credentials of the caller and to interrupt a download if the file system
 * request got canceled.
 *
 * A ClientCtx encapulates thread-local storage.  It can be set somewhere at the
 * beginning of a file system call and used anywhere during the processing of
 * the call.  It is a singleton.
 */
class ClientCtx {
 public:
  struct ThreadLocalStorage {
    ThreadLocalStorage(uid_t u, gid_t g, pid_t p, InterruptCue *ic)
        : uid(u), gid(g), pid(p), interrupt_cue(ic), is_set(true) { }
    uid_t uid;
    gid_t gid;
    pid_t pid;
    InterruptCue *interrupt_cue;  ///< A non-owning pointer
    bool is_set;                  ///< either not yet set or deliberately unset
  };

  static ClientCtx *GetInstance();
  static void CleanupInstance();
  ~ClientCtx();

  void Set(uid_t uid, gid_t gid, pid_t pid, InterruptCue *ic);
  void Unset();
  void Get(uid_t *uid, gid_t *gid, pid_t *pid, InterruptCue **ic);
  bool IsSet();

 private:
  static ClientCtx *instance_;
  static void TlsDestructor(void *data);

  ClientCtx();

  pthread_key_t thread_local_storage_;
  pthread_mutex_t *lock_tls_blocks_;
  std::vector<ThreadLocalStorage *> tls_blocks_;
};

/**
 * RAII form of the ClientCtx.  On construction, automatically sets the context
 * via the constructor; on destruction, restores to the previous values.
 *
 * Meant to be allocated on the stack.
 */
class ClientCtxGuard {
 public:
  ClientCtxGuard(uid_t uid, gid_t gid, pid_t pid, InterruptCue *ic)
      : set_on_construction_(false)
      , old_uid_(-1)
      , old_gid_(-1)
      , old_pid_(-1)
      , old_interrupt_cue_(NULL) {
    // Implementation guarantees old_ctx is not null.
    ClientCtx *old_ctx = ClientCtx::GetInstance();
    assert(old_ctx);
    if (old_ctx->IsSet()) {
      set_on_construction_ = true;
      old_ctx->Get(&old_uid_, &old_gid_, &old_pid_, &old_interrupt_cue_);
    }
    old_ctx->Set(uid, gid, pid, ic);
  }

  ~ClientCtxGuard() {
    ClientCtx *ctx = ClientCtx::GetInstance();
    if (set_on_construction_) {
      ctx->Set(old_uid_, old_gid_, old_pid_, old_interrupt_cue_);
    } else {
      ctx->Unset();
    }
  }

 private:
  bool set_on_construction_;
  uid_t old_uid_;
  gid_t old_gid_;
  pid_t old_pid_;
  InterruptCue *old_interrupt_cue_;
};

#endif  // CVMFS_CLIENTCTX_H_
