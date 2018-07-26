# To build this "lite" version of libarchive from the root of libarchive itself
# we invoke:

# eliminate whole directories that we don't need.
$ rm -rf contrib/ test_utils/ examples/ doc/

# eliminate all the files that are not necessary while keeping the directory
# structure.
$ find libarchive/test/ -type f -exec rm -ivf {} \;
$ find cat/ -type f -exec rm -ivf {} \;
$ find tar/ -type f -exec rm -ivf {} \;
$ find cpio/ -type f -exec rm -ivf {} \;

# The process is not perfect and in order to build the library, after the
# specific configure we used it was necessary to touch some file as done in
# makeHook.sh
# (Actually just keep running `make` untill it doesn't work will be enough).
