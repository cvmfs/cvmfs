dnl  -*- Autoconf -*-
dnl  Process this file with autoconf to produce a 
dnl  configure script.

dnl AC_PREREQ(2.50)
AC_INIT([CVMFS], [0.2.78], [cvmfs-talk@cern.ch])
AM_INIT_AUTOMAKE([-Wall -Werror foreign])
AC_CONFIG_SRCDIR([cvmfs/src/cvmfs.cc])
AC_CONFIG_HEADER([config.h])
AH_BOTTOM([#define CVMFS_PATCH_LEVEL "0"])

AC_ARG_ENABLE([cvmfs], AC_HELP_STRING([--disable-cvmfs], [Don't build the CernVM-FS FUSE module]), AC_SUBST([HAVE_CVMFS], [no]))
AC_ARG_ENABLE([server], AC_HELP_STRING([--disable-server], [Don't build writer's end programs]), AC_SUBST([HAVE_SERVER], [no]))
AC_ARG_ENABLE([sqlite3-builtin], AC_HELP_STRING([--enable-sqlite3-builtin], [Don't use system SQLite3]), AC_SUBST([USE_BUILTIN_SQLITE3], [yes]))
AC_ARG_ENABLE([libcurl-builtin], AC_HELP_STRING([--enable-libcurl-builtin], [Don't use system libcurl]), AC_SUBST([USE_BUILTIN_LIBCURL], [yes]))
dnl AC_ARG_ENABLE([libfuse-builtin], AC_HELP_STRING([--enable-libfuse-builtin], [Don't use system libfuse]), AC_SUBST([USE_BUILTIN_LIBFUSE], [yes]))
AC_ARG_ENABLE([zlib-builtin], AC_HELP_STRING([--enable-zlib-builtin], [Don't use system]), AC_SUBST([USE_BUILTIN_ZLIB], [yes]))
AC_ARG_ENABLE([kernel-module], AC_HELP_STRING([--enable-kernel-module], [Build the kernel module for the server part]), AC_SUBST([HAVE_KMODULE], [yes]))
AC_ARG_ENABLE([mount-scripts], AC_HELP_STRING([--enable-mount-scripts], [Install CernVM-FS mount tools in /etc and /sbin]), AC_SUBST([HAVE_MOUNTSCRIPTS], [yes]))
AM_CONDITIONAL([BUILD_CVMFS], [test x$HAVE_CVMFS != xno])
AM_CONDITIONAL([BUILD_SERVER], [test x$HAVE_SERVER != xno])
AM_CONDITIONAL([SYSTEM_SQLITE3], [test x$USE_BUILTIN_SQLITE3 != xyes])
AM_CONDITIONAL([SYSTEM_LIBCURL], [test x$USE_BUILTIN_LIBCURL != xyes])
AM_CONDITIONAL([SYSTEM_LIBFUSE], [test x != x])
AM_CONDITIONAL([SYSTEM_ZLIB], [test x$USE_BUILTIN_ZLIB != xyes])
AM_CONDITIONAL([BUILD_KMODULE], [test x$HAVE_KMODULE == xyes])
AM_CONDITIONAL([BUILD_MEMCACHED], [test x$HAVE_MEMCACHED == xyes])
AM_CONDITIONAL([BUILD_MOUNTSCRIPTS], [test x$HAVE_MOUNTSCRIPTS == xyes])

dnl  Checks for programs.
AC_PROG_CC
AC_PROG_CXX
AM_PROG_CC_C_O
AC_PROG_INSTALL
dnl AC_PROG_LIBTOOL
AC_PROG_MAKE_SET
AC_PROG_RANLIB

CXXFLAGS="$CXXFLAGS -g -fno-exceptions -fno-strict-aliasing -fasynchronous-unwind-tables -fno-omit-frame-pointer -fno-optimize-sibling-calls -Wall \
         -D_REENTRANT -D__EXTENSIONS__ -D_LARGEFILE64_SOURCE -D__LARGE64_FILES"
CFLAGS="$CFLAGS -g -fno-strict-aliasing -fasynchronous-unwind-tables -fno-omit-frame-pointer -fno-optimize-sibling-calls -Wall \
        -D_REENTRANT -D__EXTENSIONS__ -D_LARGEFILE64_SOURCE -D__LARGE64_FILES"
CVMFS_LIBS="$LDFLAGS -ldl -lm"

dnl Check for pthread
AX_PTHREAD(, [AC_MSG_ERROR(pthread.h required)])

PKG_PROG_PKG_CONFIG()

if test x$USE_BUILTIN_LIBCURL != xyes; then
   PKG_CHECK_MODULES([CURL], [libcurl >= 7.15])
else
   CFLAGS="$CFLAGS -D_BUILT_IN_LIBCURL"
   CXXFLAGS="$CXXFLAGS -D_BUILT_IN_LIBCURL"
fi

if test x$USE_BUILTIN_SQLITE3 != xyes; then
  PKG_CHECK_MODULES([SQLITE], [sqlite3 >= 3.3.9])
else
  CFLAGS="$CFLAGS -D_BUILT_IN_SQLITE3"
  CXXFLAGS="$CXXFLAGS -D_BUILT_IN_SQLITE3" 
fi

if test x$USE_BUILTIN_ZLIB == xyes; then
  CFLAGS="$CFLAGS -D_BUILTIN_IN_ZLIB"
  CXXFLAGS="$CXXFLAGS -D_BUILT_IN_ZLIB"
fi

dnl Check for OPENSSL
PKG_CHECK_MODULES([OPENSSL], [libcrypto >= 0.9.7a])

dnl Get OpenMP Options
gt_OPENMP()

dnl  Checks for libraries.
save_LIBS=$LIBS

Z_LIBS=
if test x$USE_BUILTIN_ZLIB != xyes; then 
  AC_CHECK_LIB([z], [inflate], [Z_LIBS="-lz"], [AC_MSG_ERROR(zlib required)])
else
  dnl Remove libz from OpenSSL
  OPENSSL_LIBS=`echo "$OPENSSL_LIBS" | sed s/-lz//g`
  AC_SUBST(OPENSSL_LIBS)
fi

#AC_CHECK_LIB([rt], [sched_yield], ,[AC_MSG_ERROR(librt (Posix real time extensions) required)])

if test x$HAVE_CVMFS != xno; then
dnl   if test x$USE_BUILTIN_LIBFUSE != xyes; then
dnl      AC_CHECK_HEADERS([fuse.h fuse/fuse_opt.h],,[AC_MSG_ERROR(fuse headers required)],[#define _FILE_OFFSET_BITS 64])
dnl      AC_CHECK_LIB([fuse], [fuse_loop], [FUSE_CFLAGS="-DHAS_FUSE"; FUSE_LIBS="-lfuse"], [AC_MSG_ERROR(fuse devel required)])
dnl   else
      FUSE_CFLAGS="-DHAS_FUSE -D_BUILT_IN_LIBFUSE"
dnl   fi
fi

AC_CHECK_HEADERS([sys/xattr.h sys/dir.h],,[AC_MSG_ERROR(missing header)])
if test x$HAVE_SERVER != xno; then
  AC_CHECK_LIB([gomp], [omp_get_num_threads], [GOMP_LIBS="-lgomp"], [AC_MSG_ERROR(libgomp required)])
fi

LIBS=$save_LIBS

AC_SUBST(CVMFS_CXXFLAGS)
AC_SUBST(CVMFS_CFLAGS)
AC_SUBST(CVMFS_LIBS)
AC_SUBST(Z_LIBS)
AC_SUBST(GOMP_LIBS)
AC_SUBST(FUSE_CFLAGS)
AC_SUBST(FUSE_LIBS)

dnl  Checks for header files.
AC_FUNC_ALLOCA
dnl AC_HEADER_DIRENT
dnl AC_HEADER_STDC
dnl AC_HEADER_STDC++
dnl AC_HEADER_SYS_WAIT
AC_HEADER_STDC
AC_CHECK_HEADERS([zlib.h netinet/in.h arpa/inet.h sys/socket.h sys/un.h sys/time.h sys/uio.h sys/stat.h sys/types.h \
                  sys/wait.h sys/select.h pthread.h termios.h utime.h signal.h errno.h dirent.h unistd.h \
                  fcntl.h netdb.h syslog.h sys/resource.h execinfo.h sys/ucontext.h poll.h],,
                 [AC_MSG_ERROR(missing header)])

dnl  Checks for typedefs, structures, and compiler characteristics.
dnl AC_C_CONST
dnl AC_TYPE_UID_T
dnl AC_TYPE_MODE_T
dnl AC_TYPE_OFF_T
dnl AC_TYPE_SIZE_T
dnl AC_TYPE_SSIZE_T
dnl AC_CHECK_MEMBERS([struct stat.st_blksize])
dnl AC_STRUCT_ST_BLOCKS
dnl AC_CHECK_MEMBERS([struct stat.st_rdev])
dnl AC_HEADER_TIME
dnl AC_STRUCT_TM

dnl Checks for library functions.
dnl AC_FUNC_CHOWN
dnl AC_FUNC_CLOSEDIR_VOID
dnl AC_FUNC_FORK
dnl AC_PROG_GCC_TRADITIONAL
dnl AC_FUNC_LSTAT
dnl AC_FUNC_LSTAT_FOLLOWS_SLASHED_SYMLINK
dnl AC_FUNC_MALLOC
dnl AC_FUNC_MMAP
dnl AC_FUNC_REALLOC
dnl AC_FUNC_SELECT_ARGTYPES
dnl AC_TYPE_SIGNAL
dnl AC_FUNC_STAT
dnl AC_FUNC_UTIME_NULL
dnl AC_FUNC_VPRINTF
dnl AC_CHECK_FUNCS([dup2 fchdir ftruncate getcwd gethostbyaddr gethostbyname gettimeofday lchown \
dnl                 memmove memset mkdir pow rmdir select socket strchr strdup strerror strpbrk \
dnl                 strrchr uname utime stat64])

AC_CONFIG_SUBDIRS([zlib/src])
AC_CONFIG_SUBDIRS([libcurl/src])
AC_CONFIG_SUBDIRS([libfuse/src])
AC_CONFIG_SUBDIRS([jemalloc/src])
AC_CONFIG_FILES([Makefile
           sqlite3/Makefile
           sqlite3/src/Makefile
           cvmfs/Makefile
           cvmfs/src/Makefile
           mount/Makefile
           replica/Makefile
           cvmfsd/Makefile
           cvmfsd/src/Makefile])

AC_OUTPUT
