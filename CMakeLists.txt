cmake_minimum_required (VERSION 2.8)
set (PROJECT_NAME "CernVM-FS")
project (${PROJECT_NAME})

#
# The version numbers
#
set (CernVM-FS_VERSION_MAJOR 0)
set (CernVM-FS_VERSION_MINOR 2)
set (CernVM-FS_VERSION_PATCH 77)
set (CernVM-FS_VERSION_STRING "${CernVM-FS_VERSION_MAJOR}.${CernVM-FS_VERSION_MINOR}.${CernVM-FS_VERSION_PATCH}")

#
# set the path where cmake looks for additional modules
#
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/Modules/")

#
# check if we are on Mac OS X
#
if (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
	set (MACOSX TRUE)
else ()
	set (MACOSX FALSE)
endif ()

#
# provide some options to the user
#
option (BUILD_CVMFS "Build the CernVM-FS FUSE module" ON)
option (BUILD_SERVER "Build writer's end programs" ON)
option (SQLITE3_BUILTIN "Don't use system SQLite3" ON)
option (LIBCURL_BUILTIN "Don't use system libcurl" OFF)
option (ZLIB_BUILTIN "Don't use system zlib" ON)
option (USE_JEMALLOC "Using jemalloc instead of ordinary malloc implementation" ON)
option (BUILD_KERNEL_MODULE "Build the kernel module for the server part" OFF)
option (INSTALL_MOUNT_SCRIPTS "Install CernVM-FS mount tools in /etc and /sbin" ON)

#
# run the bootstrap shellscript (not needed in the distributed version of the source)
#

if (EXISTS "${CMAKE_SOURCE_DIR}/bootstrap.sh")
	message (STATUS "running bootstrap.sh ...")
	execute_process (
		COMMAND				sh ${CMAKE_SOURCE_DIR}/bootstrap.sh
		WORKING_DIRECTORY	${CMAKE_SOURCE_DIR}
	)
endif (EXISTS "${CMAKE_SOURCE_DIR}/bootstrap.sh")

#
# set some default flags
#
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -fno-exceptions -fno-strict-aliasing -fasynchronous-unwind-tables -fno-omit-frame-pointer -fno-optimize-sibling-calls -Wall -D_REENTRANT -D__EXTENSIONS__ -D_LARGEFILE64_SOURCE -D__LARGE64_FILES")
set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -g -fno-strict-aliasing -fasynchronous-unwind-tables -fno-omit-frame-pointer -fno-optimize-sibling-calls -Wall -D_REENTRANT -D__EXTENSIONS__ -D_LARGEFILE64_SOURCE -D__LARGE64_FILES")
#set (CVMFS_LIBS "${LDFLAGS} -ldl -lm") ??

set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES})
set (PREBUILD_SCRIPT_LOCATION "${CMAKE_SOURCE_DIR}/prebuildscripts")

set (THIRD_PARTY_LIB_LOCATION	"${CMAKE_SOURCE_DIR}/3rdParty")
set (SQLITE3_BUILTIN_LOCATION	"${THIRD_PARTY_LIB_LOCATION}/sqlite3/src")
set (LIBCURL_BUILTIN_LOCATION	"${THIRD_PARTY_LIB_LOCATION}/libcurl/src")
set (ZLIB_BUILTIN_LOCATION		"${THIRD_PARTY_LIB_LOCATION}/zlib/src")
set (LIBFUSE_BUILTIN_LOCATION	"${THIRD_PARTY_LIB_LOCATION}/libfuse/src")
set (LIBFUSE4X_BUILTIN_LOCATION	"${THIRD_PARTY_LIB_LOCATION}/libfuse4x/src")
set (JEMALLOC_BUILTIN_LOCATION	"${THIRD_PARTY_LIB_LOCATION}/jemalloc/src")

#
# configure usage of some libraries
#

if (NOT MACOSX)
	include (CheckLibraryExists)
	check_library_exists (rt pthread_spinlock_create "" HAVE_LIB_RT) # currently untested
	find_package(rt REQUIRED)
else (NOT MACOSX)
	set (HAVE_LIB_RT FALSE)
endif (NOT MACOSX)

find_package (OpenSSL REQUIRED)
set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${OPENSSL_INCLUDE_DIR})

find_package (OpenMP REQUIRED)

#
# check existence of include files
#
include (CheckIncludeFile)
check_include_file (sys/xattr.h HAVE_XATTR_H)
check_include_file (zlib.h HAVE_ZLIB_H)
check_include_file (netinet/in.h HAVE_NETINET_IN_H)
check_include_file (arpa/inet.h HAVE_ARPA_INET_H)
check_include_file (sys/socket.h HAVE_SYS_SOCKET_H)
check_include_file (sys/un.h HAVE_SYS_UN_H)
check_include_file (sys/time.h HAVE_SYS_TIME_H)
check_include_file (sys/uio.h HAVE_SYS_UIO_H)
check_include_file (sys/stat.h HAVE_SYS_STAT_H)
check_include_file (sys/types.h HAVE_SYS_TYPES_H)
check_include_file (sys/wait.h HAVE_SYS_WAIT_H)
check_include_file (sys/select.h HAVE_SYS_SELECT_H)
check_include_file (pthread.h HAVE_PTHREAD_H)
check_include_file (termios.h HAVE_TERMIOS_H)
check_include_file (utime.h HAVE_UTIME_H)
check_include_file (signal.h HAVE_SIGNAL_H)
check_include_file (errno.h HAVE_ERRNO_H)
check_include_file (dirent.h HAVE_DIRENT_H)
check_include_file (unistd.h HAVE_UNISTD_H)
check_include_file (fcntl.h HAVE_FCNTL_H)
check_include_file (netdb.h HAVE_NETDB_H)
check_include_file (syslog.h HAVE_SYSLOG_H)
check_include_file (sys/resource.h HAVE_SYS_RESOURCE_H)
check_include_file (execinfo.h HAVE_EXECINFO_H)
check_include_file (poll.h HAVE_POLL_H)
if (NOT MACOSX)
	check_include_file (sys/statfs.h HAVE_SYS_STATFS_H)
	check_include_file (ucontext.h HAVE_UCONTEXT_H)
else ()
	check_include_file (sys/ucontext.h HAVE_UCONTEXT_H)
endif ()

#
# configure the config.h.in file
#
configure_file (
	"${CMAKE_SOURCE_DIR}/config_cmake.h.in"
	"${CMAKE_BINARY_DIR}/cvmfs_config.h"
)
set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${CMAKE_BINARY_DIR})

include_directories (${INCLUDE_DIRECTORIES})

#
# go for the actual compilation
#
add_subdirectory (cvmfs)
add_subdirectory (cvmfsd)

if (INSTALL_MOUNT_SCRIPTS)
	add_subdirectory (mount)
endif (INSTALL_MOUNT_SCRIPTS)

if (NOT MACOSX AND BUILD_KERNEL_MODULE)
	add_subdirectory (kernel)
endif (NOT MACOSX AND BUILD_KERNEL_MODULE)

#
# configure the packageing stuff
#
include (CPackLists.txt)