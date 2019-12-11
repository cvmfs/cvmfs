#define FUSE_USE_VERSION 31
#include <fuse3/fuse_lowlevel.h>

int main() {
  struct fuse_file_info fi;
  int ret = fi.cache_readdir;
  return 0;
}
