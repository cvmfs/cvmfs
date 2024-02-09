#define FUSE_USE_VERSION 312
#include <fuse3/fuse.h>

int main() {
  struct fuse_loop_config *fuse_loop_cfg = fuse_loop_cfg_create();
  (void) fuse_loop_cfg;
  return 0;
}
