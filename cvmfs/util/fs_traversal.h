/**
 * This file is part of the CernVM File System.
 *
 * It provides a file system traversal framework to abstract the traversal
 * of directories.
 */

#ifndef CVMFS_UTIL_FS_TRAVERSAL_H_
#define CVMFS_UTIL_FS_TRAVERSAL_H_

#include <dirent.h>
#include <errno.h>
#include <pthread.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cassert>
#include <cstdlib>
#include <deque>
#include <string>
#include <vector>

#include "util/exception.h"
#include "util/logging.h"
#include "util/platform.h"

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

/**
 * @brief A simple recursion engine to abstract the recursion of directories.
 * It provides several callback hooks to instrument and control the recursion.
 * Hooks will be called on the provided delegate object of type T
 *
 * Callbacks are called for every directory entry found by the recursion engine.
 * The recursion can be influenced by return values of these callbacks.
 */
template<class T>
class FileSystemTraversal {
 public:
  typedef void (T::*VoidCallback)(const std::string &relative_path,
                                  const std::string &dir_name);
  typedef bool (T::*BoolCallback)(const std::string &relative_path,
                                  const std::string &dir_name);


  VoidCallback fn_enter_dir;
  VoidCallback fn_leave_dir;
  VoidCallback fn_new_file;
  VoidCallback fn_new_symlink;
  VoidCallback fn_new_socket;
  VoidCallback fn_new_block_dev;
  VoidCallback fn_new_character_dev;
  VoidCallback fn_new_fifo;

  /**
   * Optional callback for all files during recursion to decide
   * whether to completely ignore the file.  If this callback returns
   * true then the file will not be processed (this is a replacement
   * for the ignored_files set, and it allows to ignore based on names
   * or something else). If the function is not specified, no files
   * will be ignored (except for "." and "..").
   */
  BoolCallback fn_ignore_file;

  /**
   * Callback if a directory was found.  Depending on the response of
   * the callback, the recursion will continue in the found directory/
   * If this callback is not specified, it will recurse by default.
   */
  BoolCallback fn_new_dir_prefix;

  /**
   * Callback for a found directory after it was already recursed
   * e.g. for deletion of directories: first delete content,
   * then the directory itself
   */
  VoidCallback fn_new_dir_postfix;


  /**
   * Create a new recursion engine
   * @param delegate The object that will receive the callbacks
   * @param relative_to_directory The DirEntries will be created relative
   *        to this directory
   * @param recurse Should the traversal engine recurse? (if not,
   *        it just traverses the given directory)
   */
  FileSystemTraversal(T *delegate,
                      const std::string &relative_to_directory,
                      const bool recurse)
      : fn_enter_dir(NULL)
      , fn_leave_dir(NULL)
      , fn_new_file(NULL)
      , fn_new_symlink(NULL)
      , fn_new_socket(NULL)
      , fn_new_block_dev(NULL)
      , fn_new_character_dev(NULL)
      , fn_new_fifo(NULL)
      , fn_ignore_file(NULL)
      , fn_new_dir_prefix(NULL)
      , fn_new_dir_postfix(NULL)
      , delegate_(delegate)
      , relative_to_directory_(relative_to_directory)
      , recurse_(recurse) {
    Init();
  }

  /**
   * Start the recursion.
   * @param dir_path The directory to start the recursion at
   */
  void Recurse(const std::string &dir_path) const {
    assert(fn_enter_dir != NULL || fn_leave_dir != NULL || fn_new_file != NULL
           || fn_new_symlink != NULL || fn_new_dir_prefix != NULL
           || fn_new_block_dev != NULL || fn_new_character_dev != NULL
           || fn_new_fifo != NULL || fn_new_socket != NULL);

    assert(relative_to_directory_.length() == 0
           || dir_path.substr(0, relative_to_directory_.length())
                  == relative_to_directory_);

    DoRecursion(dir_path, "");
  }

  /**
   * Two-phase parallel traversal of a directory tree.
   *
   * Phase 1 (parallel I/O): num_threads worker threads race through the
   * directory tree with opendir/readdir/lstat, building an in-memory tree of
   * DirScanNode objects.  No callbacks are fired; no shared catalog state is
   * touched.  Independent subtrees are scanned simultaneously.
   *
   * Phase 2 (serial replay): the pre-built tree is walked in exactly the same
   * depth-first order as Recurse()/DoRecursion(), firing all callbacks with
   * identical arguments.  Because Phase 2 is single-threaded, all existing
   * SyncMediator/catalog ordering invariants (EnterDirectory before any child
   * callbacks, LeaveDirectory last) are preserved without any changes to
   * callers.
   *
   * Falls back to the serial Recurse() path when num_threads <= 1 or the
   * host reports fewer than 2 logical CPUs.
   *
   * @param dir_path    root of the tree to traverse
   * @param num_threads I/O threads for Phase 1; 0 = auto (one per CPU)
   */
  void RecurseParallel(const std::string &dir_path,
                       unsigned num_threads = 0) const {
    assert(fn_enter_dir != NULL || fn_leave_dir != NULL || fn_new_file != NULL
           || fn_new_symlink != NULL || fn_new_dir_prefix != NULL
           || fn_new_block_dev != NULL || fn_new_character_dev != NULL
           || fn_new_fifo != NULL || fn_new_socket != NULL);
    assert(relative_to_directory_.length() == 0
           || dir_path.substr(0, relative_to_directory_.length())
                  == relative_to_directory_);

    if (num_threads == 0) {
      const long ncpus = sysconf(_SC_NPROCESSORS_ONLN);
      num_threads = (ncpus > 1) ? static_cast<unsigned>(ncpus) : 1u;
    }
    if (num_threads <= 1) {
      DoRecursion(dir_path, "");
      return;
    }

    LogCvmfs(kLogFsTraversal, kLogVerboseMsg,
             "RecurseParallel: Phase 1 scan with %u threads for [%s]",
             num_threads, dir_path.c_str());

    // Phase 1: parallel I/O scan — pure filesystem reads, no callbacks.
    DirScanNode *root = ScanParallel(dir_path, num_threads);

    LogCvmfs(kLogFsTraversal, kLogVerboseMsg,
             "RecurseParallel: Phase 2 serial replay for [%s]", dir_path.c_str());

    // Phase 2: serial DFS replay — fires callbacks in original DFS order.
    ReplayTree(root, dir_path, "");

    DeleteTree(root);
  }

 private:
  // The delegate all hooks are called on
  T *delegate_;

  /** dir_path in callbacks will be relative to this directory */
  std::string relative_to_directory_;
  bool recurse_;


  void Init() { }

  void DoRecursion(const std::string &parent_path,
                   const std::string &dir_name) const {
    DIR *dip;
    platform_dirent64 *dit;
    const std::string path = parent_path
                             + ((!dir_name.empty()) ? ("/" + dir_name) : "");

    // Change into directory and notify the user
    LogCvmfs(kLogFsTraversal, kLogVerboseMsg, "entering %s (%s -- %s)",
             path.c_str(), parent_path.c_str(), dir_name.c_str());
    dip = opendir(path.c_str());
    if (!dip) {
      PANIC(kLogStderr,
            "Failed to open %s (%d).\n"
            "Please check directory permissions.",
            path.c_str(), errno);
    }
    Notify(fn_enter_dir, parent_path, dir_name);

    // Walk through the open directory notifying the user about contents
    while ((dit = platform_readdir(dip)) != NULL) {
      // Check if file should be ignored
      if (std::string(dit->d_name) == "." || std::string(dit->d_name) == "..") {
        continue;
      } else if (fn_ignore_file != NULL) {
        if (Notify(fn_ignore_file, path, dit->d_name)) {
          LogCvmfs(kLogFsTraversal, kLogVerboseMsg, "ignoring %s/%s",
                   path.c_str(), dit->d_name);
          continue;
        }
      } else {
        LogCvmfs(kLogFsTraversal, kLogVerboseMsg,
                 "not ignoring %s/%s (fn_ignore_file not set)", path.c_str(),
                 dit->d_name);
      }

      // Notify user about found directory entry
      platform_stat64 info;
      const int retval = platform_lstat((path + "/" + dit->d_name).c_str(),
                                        &info);
      if (retval != 0) {
        PANIC(kLogStderr, "failed to lstat '%s' errno: %d",
              (path + "/" + dit->d_name).c_str(), errno);
      }
      if (S_ISDIR(info.st_mode)) {
        LogCvmfs(kLogFsTraversal, kLogVerboseMsg, "passing directory %s/%s",
                 path.c_str(), dit->d_name);
        if (Notify(fn_new_dir_prefix, path, dit->d_name) && recurse_) {
          DoRecursion(path, dit->d_name);
        }
        Notify(fn_new_dir_postfix, path, dit->d_name);
      } else if (S_ISREG(info.st_mode)) {
        LogCvmfs(kLogFsTraversal, kLogVerboseMsg, "passing regular file %s/%s",
                 path.c_str(), dit->d_name);
        Notify(fn_new_file, path, dit->d_name);
      } else if (S_ISLNK(info.st_mode)) {
        LogCvmfs(kLogFsTraversal, kLogVerboseMsg, "passing symlink %s/%s",
                 path.c_str(), dit->d_name);
        Notify(fn_new_symlink, path, dit->d_name);
      } else if (S_ISSOCK(info.st_mode)) {
        LogCvmfs(kLogFsTraversal, kLogVerboseMsg, "passing socket %s/%s",
                 path.c_str(), dit->d_name);
        Notify(fn_new_socket, path, dit->d_name);
      } else if (S_ISBLK(info.st_mode)) {
        LogCvmfs(kLogFsTraversal, kLogVerboseMsg, "passing block-device %s/%s",
                 path.c_str(), dit->d_name);
        Notify(fn_new_block_dev, path, dit->d_name);
      } else if (S_ISCHR(info.st_mode)) {
        LogCvmfs(kLogFsTraversal, kLogVerboseMsg,
                 "passing character-device "
                 "%s/%s",
                 path.c_str(), dit->d_name);
        Notify(fn_new_character_dev, path, dit->d_name);
      } else if (S_ISFIFO(info.st_mode)) {
        LogCvmfs(kLogFsTraversal, kLogVerboseMsg, "passing FIFO %s/%s",
                 path.c_str(), dit->d_name);
        Notify(fn_new_fifo, path, dit->d_name);
      } else {
        LogCvmfs(kLogFsTraversal, kLogVerboseMsg, "unknown file type %s/%s",
                 path.c_str(), dit->d_name);
      }
    }

    // Close directory and notify user
    closedir(dip);
    LogCvmfs(kLogFsTraversal, kLogVerboseMsg, "leaving %s", path.c_str());
    Notify(fn_leave_dir, parent_path, dir_name);
  }

  inline bool Notify(const BoolCallback callback,
                     const std::string &parent_path,
                     const std::string &entry_name) const {
    return (callback == NULL) ? true
                              : (delegate_->*callback)(
                                    GetRelativePath(parent_path), entry_name);
  }

  inline void Notify(const VoidCallback callback,
                     const std::string &parent_path,
                     const std::string &entry_name) const {
    if (callback != NULL) {
      (delegate_->*callback)(GetRelativePath(parent_path), entry_name);
    }
  }

  std::string GetRelativePath(const std::string &absolute_path) const {
    const unsigned int rel_dir_len = relative_to_directory_.length();
    if (rel_dir_len >= absolute_path.length()) {
      return "";
    } else if (rel_dir_len > 1) {
      return absolute_path.substr(rel_dir_len + 1);
    } else if (rel_dir_len == 0) {
      return absolute_path;
    } else if (relative_to_directory_ == "/") {
      return absolute_path.substr(1);
    }

    return "";
  }

  // ── Two-phase parallel traversal internals ──────────────────────────────

  /**
   * A single non-directory entry collected during Phase 1.
   * Stores only the name and st_mode so the replay phase can dispatch to the
   * correct callback without re-stating the file.
   */
  struct DirScanEntry {
    std::string name;
    mode_t      mode;
  };

  /**
   * One node of the pre-scanned directory tree.
   *
   * Written exclusively by the Phase-1 worker thread that pops this node from
   * the work queue (no lock required for writes to entries/subdirs).
   * Read exclusively by the Phase-2 serial replay (no lock required there
   * either, since all workers have joined before Phase 2 starts).
   */
  struct DirScanNode {
    std::string                abs_path;  ///< absolute path to this directory
    std::string                dir_name;  ///< entry name (used in callbacks)
    std::vector<DirScanEntry>  entries;   ///< non-directory entries
    std::vector<DirScanNode *> subdirs;   ///< child directory nodes (in readdir order)
  };

  /**
   * Shared state passed to every Phase-1 worker thread.
   * All fields except delegate/fn_ignore/relative_to are protected by lock.
   */
  struct ScanWorkerArgs {
    // Read-only after construction — no lock needed.
    T            *delegate;
    BoolCallback  fn_ignore;    ///< ignore-file predicate (may be NULL)
    std::string   relative_to;  ///< mirrors relative_to_directory_

    // Protected by lock.
    std::deque<DirScanNode *> queue;
    int                       active;  ///< threads currently scanning a node
    pthread_mutex_t           lock;
    pthread_cond_t            work_available;
  };

  /**
   * Mirrors GetRelativePath() for use inside the static worker thread where
   * the FileSystemTraversal instance is not directly accessible.
   */
  static std::string RelPath(const std::string &abs,
                             const std::string &rel_to) {
    const size_t rlen = rel_to.length();
    if (rlen >= abs.length()) return "";
    if (rlen > 1)             return abs.substr(rlen + 1);
    if (rlen == 0)            return abs;
    if (rel_to == "/")        return abs.substr(1);
    return "";
  }

  /**
   * Phase-1 worker thread entry point.
   *
   * Each thread loops: pop a DirScanNode from the shared work queue, open its
   * directory, lstat every entry, push new DirScanNode children back onto the
   * queue, repeat.  Exits when the queue is empty AND no other thread has a
   * node in flight (active == 0).
   *
   * Thread safety: DirScanNode::entries and ::subdirs are written only by the
   * thread that owns a node (popped it from the queue).  Other threads never
   * touch a node while it is being scanned.  The work queue itself is protected
   * by ScanWorkerArgs::lock.
   */
  static void *ScanWorkerThread(void *raw) {
    ScanWorkerArgs *a = static_cast<ScanWorkerArgs *>(raw);

    for (;;) {
      pthread_mutex_lock(&a->lock);

      // Wait until there is a node to process or all threads have gone idle.
      while (a->queue.empty() && a->active > 0)
        pthread_cond_wait(&a->work_available, &a->lock);

      if (a->queue.empty()) {
        // active == 0 and queue empty: scanning is complete.
        // Wake any siblings still waiting, then exit.
        pthread_cond_broadcast(&a->work_available);
        pthread_mutex_unlock(&a->lock);
        return NULL;
      }

      DirScanNode *node = a->queue.front();
      a->queue.pop_front();
      a->active++;
      pthread_mutex_unlock(&a->lock);

      // ── Scan this directory (lock not held) ──────────────────────────────
      // Collect results locally before touching shared state.
      std::vector<DirScanNode *> new_subdirs;

      DIR *dip = opendir(node->abs_path.c_str());
      if (!dip) {
        PANIC(kLogStderr,
              "Failed to open %s (%d).\n"
              "Please check directory permissions.",
              node->abs_path.c_str(), errno);
      }

      platform_dirent64 *dit;
      while ((dit = platform_readdir(dip)) != NULL) {
        const std::string dname(dit->d_name);
        if (dname == "." || dname == "..")
          continue;

        // Apply the ignore predicate with the same relative-path convention
        // used by DoRecursion → Notify(fn_ignore_file, path, name).
        if (a->fn_ignore != NULL) {
          const std::string rel = RelPath(node->abs_path, a->relative_to);
          if ((a->delegate->*(a->fn_ignore))(rel, dname))
            continue;
        }

        platform_stat64 info;
        const std::string entry_path = node->abs_path + "/" + dname;
        if (platform_lstat(entry_path.c_str(), &info) != 0) {
          PANIC(kLogStderr, "failed to lstat '%s' errno: %d",
                entry_path.c_str(), errno);
        }

        if (S_ISDIR(info.st_mode)) {
          DirScanNode *child = new DirScanNode;
          child->abs_path = entry_path;
          child->dir_name = dname;
          // Write to node->subdirs: safe — this thread owns node exclusively.
          node->subdirs.push_back(child);
          new_subdirs.push_back(child);
        } else {
          DirScanEntry e;
          e.name = dname;
          e.mode = info.st_mode;
          // Write to node->entries: safe — same ownership argument.
          node->entries.push_back(e);
        }
      }
      closedir(dip);

      // ── Push children and release ownership ──────────────────────────────
      // Children are pushed to the queue BEFORE decrementing active so that
      // the termination condition (active==0 && queue.empty()) is never seen
      // spuriously while there is still pending work in new_subdirs.
      pthread_mutex_lock(&a->lock);
      for (size_t i = 0; i < new_subdirs.size(); ++i)
        a->queue.push_back(new_subdirs[i]);
      a->active--;
      pthread_cond_broadcast(&a->work_available);
      pthread_mutex_unlock(&a->lock);
    }
  }

  /** Phase-1 driver: allocate root node, spawn workers, join, return tree. */
  DirScanNode *ScanParallel(const std::string &root_path,
                            unsigned num_threads) const {
    DirScanNode *root = new DirScanNode;
    root->abs_path = root_path;
    root->dir_name = "";

    ScanWorkerArgs args;
    args.delegate    = delegate_;
    args.fn_ignore   = fn_ignore_file;
    args.relative_to = relative_to_directory_;
    args.queue.push_back(root);
    args.active      = 0;
    int rv = pthread_mutex_init(&args.lock, NULL);
    assert(rv == 0);
    rv = pthread_cond_init(&args.work_available, NULL);
    assert(rv == 0);

    std::vector<pthread_t> tids(num_threads);
    for (unsigned i = 0; i < num_threads; ++i) {
      rv = pthread_create(&tids[i], NULL, ScanWorkerThread, &args);
      assert(rv == 0);
    }
    for (unsigned i = 0; i < num_threads; ++i) {
      rv = pthread_join(tids[i], NULL);
      assert(rv == 0);
    }

    pthread_cond_destroy(&args.work_available);
    pthread_mutex_destroy(&args.lock);

    return root;
  }

  /**
   * Phase-2 serial DFS replay.
   *
   * Walks the pre-built tree in exactly the same depth-first order as
   * DoRecursion(), firing all callbacks with identical argument conventions.
   * The serialisation guarantees that all EnterDirectory/LeaveDirectory
   * invariants expected by SyncMediator and the catalog manager are preserved.
   */
  void ReplayTree(const DirScanNode *node,
                  const std::string &parent_path,
                  const std::string &dir_name) const {
    const std::string path = parent_path
                             + (!dir_name.empty() ? ("/" + dir_name) : "");

    Notify(fn_enter_dir, parent_path, dir_name);

    // Non-directory entries: fire the appropriate per-type callback.
    for (size_t i = 0; i < node->entries.size(); ++i) {
      const DirScanEntry &e = node->entries[i];
      if      (S_ISREG(e.mode))  Notify(fn_new_file,          path, e.name);
      else if (S_ISLNK(e.mode))  Notify(fn_new_symlink,       path, e.name);
      else if (S_ISSOCK(e.mode)) Notify(fn_new_socket,        path, e.name);
      else if (S_ISBLK(e.mode))  Notify(fn_new_block_dev,     path, e.name);
      else if (S_ISCHR(e.mode))  Notify(fn_new_character_dev, path, e.name);
      else if (S_ISFIFO(e.mode)) Notify(fn_new_fifo,          path, e.name);
      else LogCvmfs(kLogFsTraversal, kLogVerboseMsg,
                    "unknown file type %s/%s", path.c_str(), e.name.c_str());
    }

    // Subdirectories: respect fn_new_dir_prefix return value, exactly as
    // DoRecursion does at line: if (Notify(fn_new_dir_prefix, ...) && recurse_)
    for (size_t i = 0; i < node->subdirs.size(); ++i) {
      const DirScanNode *child = node->subdirs[i];
      if (Notify(fn_new_dir_prefix, path, child->dir_name) && recurse_) {
        ReplayTree(child, path, child->dir_name);
      }
      Notify(fn_new_dir_postfix, path, child->dir_name);
    }

    Notify(fn_leave_dir, parent_path, dir_name);
  }

  /** Recursively free all DirScanNode allocations. */
  static void DeleteTree(DirScanNode *node) {
    for (size_t i = 0; i < node->subdirs.size(); ++i)
      DeleteTree(node->subdirs[i]);
    delete node;
  }

};  // FileSystemTraversal

#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif

#endif  // CVMFS_UTIL_FS_TRAVERSAL_H_
