#!/bin/sh

# The output directory, `build_release` is created as a symlink by the
# makefile.
# However, the Ninja generator will pre-create the output directories,
# meaning the makeHook.sh can't make it as a symlink.  Hence, we create
# the output directories here.

make mkdir
