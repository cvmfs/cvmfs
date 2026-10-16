Public headers of libfuse 3.18.2 (https://github.com/libfuse/libfuse,
tag fuse-3.18.2, `include/`), licensed under LGPL-2.1 (see LGPL2.txt).
`libfuse_config.h` replaces the file meson generates.

libcvmfs_nfsfuse implements this API itself, so the NFS frontend builds
against these headers and needs no FUSE installation.
