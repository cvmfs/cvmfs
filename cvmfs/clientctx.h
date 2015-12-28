/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_CLIENTCTX_H_
#define CVMFS_CLIENTCTX_H_

#include <pthread.h>
#include <unistd.h>

/**
 * A client context associates a file system call with the uid, gid, and pid
 * of the calling process.  For the library, that's the current process and
 * user.  For the Fuse module, the uid and gid are provided by fuse, the pid
 * can be figured out from the system. A client context is used to download
 * files with the credentials of the caller.
 *
 * A ClientCtx encapulates thread-local storage.  It can be set somewhere at the
 * beginning of a file system call and used anywhere during the processing of
 * the call.  It is a singleton.
 */
class ClientCtx {
 public:
  struct ThreadLocalStorage {
    ThreadLocalStorage(uid_t u, gid_t g, pid_t p)
      : uid(u), gid(g), pid(p), is_set(true) { }
    uid_t uid;
    gid_t gid;
    pid_t pid;
    bool is_set;  ///< either not yet set or deliberately unset
  };

  static ClientCtx *GetInstance();
  static void CleanupInstance();

  void Set(uid_t uid, gid_t gid, pid_t pid);
  void Unset();
  void Get(uid_t *uid, gid_t *gid, pid_t *pid);
  bool IsSet();

 private:
  static ClientCtx *instance_;

  ClientCtx() { }

  pthread_key_t thread_local_storage_;
};

#endif  // CVMFS_CLIENTCTX_H_
