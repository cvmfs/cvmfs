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

#include <fuse/fuse_lowlevel.h>
#include <fuse/fuse_opt.h>
#include <dlfcn.h>

#include <cstdlib>
#include <cstring>
#include <string>

using namespace std;  // NOLINT

namespace loader {


static void stub_lookup(fuse_req_t req, fuse_ino_t parent,
                        const char *name)
{

}


static void stub_getattr(fuse_req_t req, fuse_ino_t ino,
                         struct fuse_file_info *fi)
{

}


static void stub_readlink(fuse_req_t req, fuse_ino_t ino) {
}


static void stub_opendir(fuse_req_t req, fuse_ino_t ino,
                         struct fuse_file_info *fi)
{
}


static void stub_releasedir(fuse_req_t req, fuse_ino_t ino,
                            struct fuse_file_info *fi)
{
}


static void stub_readdir(fuse_req_t req, fuse_ino_t ino, size_t size,
                         off_t off, struct fuse_file_info *fi)
{
}


static void stub_open(fuse_req_t req, fuse_ino_t ino,
                      struct fuse_file_info *fi)
{
}


static void stub_read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                       struct fuse_file_info *fi)
{
}


static void stub_release(fuse_req_t req, fuse_ino_t ino,
                         struct fuse_file_info *fi)
{
}


static void stub_statfs(fuse_req_t req, fuse_ino_t ino) {
}


#ifdef __APPLE__
static void stub_getxattr(fuse_req_t req, fuse_ino_t ino, const char *name,
                          size_t size, uint32_t position)
#else
static void stub_getxattr(fuse_req_t req, fuse_ino_t ino, const char *name,
                          size_t size)
#endif
{
}


static void stub_listxattr(fuse_req_t req, fuse_ino_t ino, size_t size) {
}

}  // namespace loader


struct CvmfsOptions {
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


/**
 * Checks whether the given option is one of our own options
 * (if it's not, it probably belongs to fuse).
 */
static int IsCvmfsOption(const char *arg) {
  if (arg[0] != '-') {
    unsigned arglen = strlen(arg);
    const char **o;
    for (o = (const char**)cvmfs_array_opts; *o; o++) {
      unsigned olen = strlen(*o);
      if ((arglen > olen && arg[olen] == '=') &&
          (strncasecmp(arg, *o, olen) == 0))
        return 1;
    }
  }
  return 0;
}

string *g_mountpoint = NULL;


static int ParseFuseOptions(void *data __attribute__((unused)), const char *arg,
                            int key, struct fuse_args *outargs) {
  switch (key) {
    case FUSE_OPT_KEY_OPT:
      if (IsCvmfsOption(arg)) {
        // If this is a "-o" option and is not one of ours, we assume that this
        // must be used for mounting fuse.
        // It can't be one of our option if it doesnt match the template.
        return 0;
      }
      return 1;

    case FUSE_OPT_KEY_NONOPT:
      g_mountpoint = new string(arg);
      return 0;

    case KEY_HELP:
      fuse_opt_add_arg(outargs, "-ho");
      exit(0);

    case KEY_VERSION:
      exit(0);

    case KEY_FOREGROUND:
      return 0;
    case KEY_SINGLETHREAD:
      return 0;
    case KEY_DEBUG:
      fuse_opt_add_arg(outargs, "-d");
      return 0;
    default:
      abort();
  }
}


int main(int argc, char *argv[]) {
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


  struct fuse_args fuse_args;
  CvmfsOptions cvmfs_opts;
  struct fuse_lowlevel_ops cvmfs_operations;
  bool foreground = false;
  bool single_threaded = false;

  fuse_args.argc = argc;
  fuse_args.argv = argv;
  fuse_args.allocated = 0;
  if (fuse_opt_parse(&fuse_args, &cvmfs_opts, cvmfs_array_opts,
                     ParseFuseOptions) != 0)
  {
    return 1;
  }

  memset(&cvmfs_operations, 0, sizeof(cvmfs_operations));
  cvmfs_operations.lookup      = loader::stub_lookup;
  cvmfs_operations.getattr     = loader::stub_getattr;
  cvmfs_operations.readlink    = loader::stub_readlink;
  cvmfs_operations.open        = loader::stub_open;
  cvmfs_operations.read        = loader::stub_read;
  cvmfs_operations.release     = loader::stub_release;
  cvmfs_operations.opendir     = loader::stub_opendir;
  cvmfs_operations.readdir     = loader::stub_readdir;
  cvmfs_operations.releasedir  = loader::stub_releasedir;
  cvmfs_operations.statfs      = loader::stub_statfs;
  cvmfs_operations.getxattr    = loader::stub_getxattr;
  cvmfs_operations.listxattr   = loader::stub_listxattr;

  struct fuse_chan *ch;
  if ((ch = fuse_mount(g_mountpoint->c_str(), &fuse_args)) != NULL) {
    struct fuse_session *se;
    se = fuse_lowlevel_new(&fuse_args, &cvmfs_operations,
                           sizeof(cvmfs_operations), NULL);
    if (se != NULL) {
      if (fuse_set_signal_handlers(se) != -1) {
        fuse_session_add_chan(se, ch);
        //if (g_single_threaded)
        //  result = fuse_session_loop(se);
        //else
        //  result = fuse_session_loop_mt(se);
        fuse_session_loop(se);
        fuse_remove_signal_handlers(se);
        fuse_session_remove_chan(ch);
      }
      fuse_session_destroy(se);
    }
    fuse_unmount(g_mountpoint->c_str(), ch);
  }

  return 0;
}
