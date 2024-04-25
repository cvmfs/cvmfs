#
# CMake build script to configure and build CernVM-FS and all it's
# external dependencies, if they are statically linked into the binaries
#
# See externals/CMake-Register_External_Lib.txt for details on external inclusion
#

cmake_minimum_required (VERSION 2.6.2...3.12)
set (PROJECT_NAME "CernVM-FS")
project (${PROJECT_NAME})

message ("Running CMake version ${CMAKE_VERSION}")

#
# The version numbers
#
# DON'T DELETE
## CVMFS_VERSION 2.12.0
#---------------------
set (CernVM-FS_VERSION_MAJOR 2)
set (CernVM-FS_VERSION_MINOR 12)
set (CernVM-FS_VERSION_PATCH 0)
set (CernVM-FS_VERSION_STRING "${CernVM-FS_VERSION_MAJOR}.${CernVM-FS_VERSION_MINOR}.${CernVM-FS_VERSION_PATCH}")

#
# set the path where cmake looks for additional modules
#
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/Modules/")

#
# detect the operating system and the distribution we are compiling on
#
if (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
  set (MACOSX TRUE)
else ()
  set (MACOSX FALSE)
endif ()

if (${CMAKE_SYSTEM_NAME} MATCHES "Linux")
  set (LINUX TRUE)
  if (EXISTS /etc/debian_version)
    set (DEBIAN TRUE)
  endif ()
  if (EXISTS /etc/arch-release OR EXISTS /etc/manjaro-release)
    set (ARCHLINUX TRUE)
  endif ()
else ()
  set (LINUX FALSE)
  set (DEBIAN FALSE)
  set (ARCHLINUX FALSE)
endif ()

#
# Set install prefix to /usr by default.
#
if (LINUX AND CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)
  set (CMAKE_INSTALL_PREFIX "/usr" CACHE PATH "CVMFS install path default is /usr" FORCE)
  message("Setting default install prefix to ${CMAKE_INSTALL_PREFIX} on Linux")
endif ()

#
# Explicitly check that the install prefix is not /usr/local.
#
if (LINUX AND "${CMAKE_INSTALL_PREFIX}" STREQUAL "/usr/local")
  message("######################################################################")
  message(WARNING "The installation path has been set to ${CMAKE_INSTALL_PREFIX}.")
  message("######################################################################")
endif ()

#
# check if we use Clang
#
if (CMAKE_CXX_COMPILER MATCHES ".*clang")
  set(USING_CLANG 1)
else (CMAKE_CXX_COMPILER MATCHES ".*clang")
  set(USING_CLANG 0)
endif (CMAKE_CXX_COMPILER MATCHES ".*clang")

#
# figure out if we are on a 64bit system
#
if (CMAKE_SIZEOF_VOID_P EQUAL 4)
  set (IS_64_BIT FALSE)
else ()
  set (IS_64_BIT TRUE)
endif ()

#
# check if we are compiling on ARM
#
if (${CMAKE_SYSTEM_PROCESSOR} MATCHES "^arm.*$")
  set (ARM TRUE)
else ()
  set (ARM FALSE)
endif ()

#
# define the installation location of the shared library files
# Note: We do not support multi-arch on Debian systems for the time being and
#       hence cannot use GNUInstallDirs as this would assume multi-arch.
#       (https://wiki.debian.org/Multiarch)
#
if (MACOSX)
  set (CMAKE_INSTALL_LIBDIR "lib")
  set (CMAKE_MOUNT_INSTALL_BINDIR "${CMAKE_INSTALL_PREFIX}/sbin")
  set (CVMFS_LIBEXEC_DIR "libexec/cvmfs")
  set (CMAKE_MACOSX_RPATH false)
else (MACOSX) # --> Linux
  if (DEBIAN OR ARCHLINUX)
    if (ARCHLINUX)
      set (CMAKE_MOUNT_INSTALL_BINDIR "/usr/bin")
      set (CVMFS_LIBEXEC_DIR "lib/cvmfs")
    else (ARCHLINUX)
      set (CMAKE_MOUNT_INSTALL_BINDIR "/sbin")
      set (CVMFS_LIBEXEC_DIR "libexec/cvmfs")
    endif (ARCHLINUX)
    set (CMAKE_INSTALL_LIBDIR "lib")
  else (DEBIAN OR ARCHLINUX) # --> RedHat, Fedora, CentOS, SuSE
    set (CMAKE_MOUNT_INSTALL_BINDIR "/sbin")
    set (CVMFS_LIBEXEC_DIR "libexec/cvmfs")
    if (IS_64_BIT)
      set (CMAKE_INSTALL_LIBDIR "lib64")
    else (IS_64_BIT) # --> 32 Bit
      set (CMAKE_INSTALL_LIBDIR "lib")
    endif (IS_64_BIT)
  endif (DEBIAN OR ARCHLINUX)
endif (MACOSX)
message ("Installing shared libraries to: ${CMAKE_INSTALL_LIBDIR}")

#
# set the system configuration directory depending on CMAKE_INSTALL_PREFIX
# Note: Found here http://osdir.com/ml/kde-commits/2011-05/msg01375.html
#
if (NOT DEFINED SYSCONF_INSTALL_DIR)
  if ("${CMAKE_INSTALL_PREFIX}" STREQUAL "/usr")
    set (SYSCONF_INSTALL_DIR "/etc") # conform to LFSH
  else ()
    set(SYSCONF_INSTALL_DIR "${CMAKE_INSTALL_PREFIX}/etc")
  endif ()
else ()
  set (SYSCONF_INSTALL_DIR "${SYSCONF_INSTALL_DIR}" CACHE STRING "The sysconfig install dir")
endif ()

#
# include file with user-defined options
#
include (cvmfs_options)

#
# set some default flags
#
# flags in CMAKE_C**_FLAGS are always passed to the compiler
#
include (cvmfs_compiler)

#
# define where to find the external dependencies
#
set (EXTERNALS_LIB_LOCATION       "${CMAKE_SOURCE_DIR}/externals")
if (NOT EXTERNALS_PREFIX)
  set(EXTERNALS_PREFIX ${CMAKE_SOURCE_DIR})
endif()
set (EXTERNALS_BUILD_LOCATION     "${EXTERNALS_PREFIX}/externals_build")
set (EXTERNALS_INSTALL_LOCATION   "${EXTERNALS_PREFIX}/externals_install")

#
# run the bootstrap shellscript (not needed in the distributed version of the source)
#
if (BUILTIN_EXTERNALS)
  if (EXISTS "${CMAKE_SOURCE_DIR}/bootstrap.sh")
    # Set some environment variables for the bootstrap script
    set(ENV{EXTERNALS_LIB_LOCATION} ${EXTERNALS_LIB_LOCATION})
    set(ENV{EXTERNALS_BUILD_LOCATION} ${EXTERNALS_BUILD_LOCATION})
    set(ENV{EXTERNALS_INSTALL_LOCATION} ${EXTERNALS_INSTALL_LOCATION})
    set(ENV{CVMFS_BASE_C_FLAGS} ${CVMFS_BASE_C_FLAGS})
    set(ENV{CVMFS_BASE_CXX_FLAGS} ${CVMFS_BASE_CXX_FLAGS})
    set(ENV{IS_64_BIT} ${IS_64_BIT})

    if (BUILD_UBENCHMARKS)
      set(ENV{BUILD_UBENCHMARKS} "true")
    endif ()
    if (BUILD_SERVER OR BUILD_SERVER_DEBUG)
      set(ENV{BUILD_SERVER} "true")
    endif ()
    if (BUILD_GEOAPI)
      set(ENV{BUILD_GEOAPI} "true")
    endif ()
    if (BUILD_QC_TESTS)
      set(ENV{BUILD_QC_TESTS} "true")
    endif()
    if (BUILD_GATEWAY)
      set(ENV{BUILD_GATEWAY} "true")
    endif ()
    if (BUILD_DUCC)
      set(ENV{BUILD_DUCC} "true")
    endif ()
    if (BUILD_SNAPSHOTTER)
      set(ENV{BUILD_SNAPSHOTTER} "true")
    endif()

    message (STATUS "running bootstrap.sh ...")
    execute_process (
      COMMAND           sh ${CMAKE_SOURCE_DIR}/bootstrap.sh
      WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
      RESULT_VARIABLE   BOOTSTRAPPING_RESULT
    )
    if (BOOTSTRAPPING_RESULT GREATER 0)
      message (FATAL_ERROR "bootstrapping failed")
    endif ()

    # Unset environment variables after the bootstrap script was executed
    set(ENV{EXTERNALS_LIB_LOCATION} "")
    set(ENV{EXTERNALS_BUILD_LOCATION} "")
    set(ENV{EXTERNALS_INSTALL_LOCATION} "")
    set(ENV{CVMFS_BASE_C_FLAGS} "")
    set(ENV{CVMFS_BASE_CXX_FLAGS} "")
    set(ENV{IS_64_BIT} "")
    set(ENV{BUILD_UBENCHMARKS} "")
    set(ENV{BUILD_SERVER} "")
    set(ENV{BUILD_QC_TESTS} "")
    set(ENV{BUILD_GATEWAY} "")
    set(ENV{BUILD_DUCC} "")
    set(ENV{BUILD_SNAPSHOTTER} "")
  endif (EXISTS "${CMAKE_SOURCE_DIR}/bootstrap.sh")

  # In the case of built-in external libraries, we need to set CMAKE_PREFIX_PATH to
  # point to the prefix where the libraries are installed. This path will take precedence
  # over the system prefix when find_package(****) is called later
  set(CMAKE_PREFIX_PATH ${EXTERNALS_INSTALL_LOCATION})
  message("Project prefix path: ${CMAKE_PREFIX_PATH}")
  message("System prefix path: ${CMAKE_SYSTEM_PREFIX_PATH}")
endif(BUILTIN_EXTERNALS)

#
# include some common functionality
#
include (FindPythonModule)


set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} cvmfs)

#
# check existence of include files
#
include (CheckIncludeFile)

macro (pedantic_include_check HEADERFILE VARIABLENAME)
  check_include_file (${HEADERFILE} ${VARIABLENAME})
  if (NOT ${VARIABLENAME})
    message (FATAL_ERROR "${HEADERFILE} is missing on your system")
  endif ()
endmacro (pedantic_include_check)

macro (permissive_include_check HEADERFILE VARIABLENAME)
  check_include_file (${HEADERFILE} ${VARIABLENAME})
  if (${VARIABLENAME})
    set (CMAKE_C_FLAGS   "${CMAKE_C_FLAGS} -D${VARIABLENAME}")
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D${VARIABLENAME}")
  endif ()
endmacro (permissive_include_check)

macro (look_for_required_include_files)
  foreach (HEADER ${ARGV})
    string (REGEX REPLACE "/|\\." "_" HEADER_VAR_LOWER ${HEADER})
    string (TOUPPER "have_${HEADER_VAR_LOWER}" HEADER_VAR)
    pedantic_include_check (${HEADER} ${HEADER_VAR})
  endforeach (HEADER IN ITEMS ${ARGV})
endmacro (look_for_required_include_files)

macro (look_for_optional_include_files)
  foreach (HEADER ${ARGV})
    string (REGEX REPLACE "/|\\." "_" HEADER_VAR_LOWER ${HEADER})
    string (TOUPPER "have_${HEADER_VAR_LOWER}" HEADER_VAR)
    permissive_include_check (${HEADER} ${HEADER_VAR})
  endforeach (HEADER IN ITEMS ${ARGV})
endmacro (look_for_optional_include_files)

set (REQUIRED_HEADERS sys/xattr.h zlib.h netinet/in.h arpa/inet.h sys/socket.h
                      sys/un.h sys/time.h sys/uio.h sys/stat.h sys/types.h
                      sys/wait.h sys/select.h pthread.h termios.h utime.h
                      signal.h errno.h dirent.h unistd.h fcntl.h netdb.h
                      syslog.h sys/resource.h execinfo.h poll.h pwd.h grp.h)
set (OPTIONAL_HEADERS )
if (NOT MACOSX)
  set (REQUIRED_HEADERS ${REQUIRED_HEADERS}
                        sys/statfs.h)

  # As of attr-2.4.48, the attr/xattr.h header disappeard in favor of sys/xattr.h
  #
  # Unfortunately, attr/xattr.h fails to compile without including sys/types.h
  # before including attr/xattr.h (it uses size_t and ssize_t).
  # CMake searches for include files by compiling a minimal *.c file like:
  #    #include <${SEARCHED_HEADER_FILE}>
  #    int main(int argc, char **argv) { return 0; }
  #
  # We pre-define the include guard of attr/xattr.h and thus still check, if the
  # file is found by the compiler but mitigating the compiler errors caused by
  # a standalone inclusion of attr/xattr.h
  set (CMAKE_REQUIRED_DEFINITIONS "-D__XATTR_H__")
  set (OPTIONAL_HEADERS ${OPTIONAL_HEADERS}
                        attr/xattr.h)
endif (NOT MACOSX)

look_for_required_include_files (${REQUIRED_HEADERS})
look_for_optional_include_files (${OPTIONAL_HEADERS})

#
# configure the config.h.in file
#
configure_file (
  "${CMAKE_SOURCE_DIR}/config_cmake.h.in"
  "${CMAKE_BINARY_DIR}/cvmfs_config.h"
)
set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${CMAKE_BINARY_DIR})


#
# set properties for configurable libraries
#

find_package (Valgrind)
if (VALGRIND_FOUND)
  set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${VALGRIND_INCLUDE_DIR})
  add_definitions (-DHAS_VALGRIND_HEADERS)
endif ()

if (NOT MACOSX)
  set (HAVE_LIB_RT TRUE)
  set (RT_LIBRARY "rt")
else ()
  set (HAVE_LIB_RT FALSE)
  set (RT_LIBRARY "")
endif ()

# Required libraries depending on build target

find_package (OpenSSL REQUIRED)
set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${OPENSSL_INCLUDE_DIR})

find_package (Libcrypto REQUIRED)
# We do not add the Libcrypto include directories to INCLUDE_DIRECTORIES
# to avoid a clash with the system openssl. We only use it for libcvmfs_crypto
message("Using libcrypto for libcvmfs_crypto from ${Libcrypto_INCLUDE_DIRS}")

find_package (GTest REQUIRED)
set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${GTEST_INCLUDE_DIRS})

find_package (VJSON REQUIRED)
set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${VJSON_INCLUDE_DIRS})

find_package (LibArchive REQUIRED)
set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${LibArchive_INCLUDE_DIRS})

# Almost all build targets require zlib and sha3
if (BUILD_CVMFS OR BUILD_LIBCVMFS OR BUILD_SERVER OR BUILD_SERVER_DEBUG OR
    BUILD_UNITTESTS OR BUILD_UNITTESTS_DEBUG OR BUILD_PRELOADER OR
    BUILD_UBENCHMARKS OR BUILD_SHRINKWRAP)
  find_package (ZLIB REQUIRED)
  set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${ZLIB_INCLUDE_DIRS})

  find_package (SHA3 REQUIRED)
  set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${SHA3_INCLUDE_DIRS})
endif ()


# Most build target require also curl/c-ares, sqlite, uuid, leveldb
if (BUILD_CVMFS OR BUILD_LIBCVMFS OR BUILD_SERVER OR BUILD_SERVER_DEBUG OR
    BUILD_UNITTESTS OR BUILD_UNITTESTS_DEBUG OR BUILD_PRELOADER OR
    BUILD_SHRINKWRAP)
  find_package (CARES REQUIRED)
  set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${CARES_INCLUDE_DIRS})

  find_package (CURL REQUIRED)
  set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${CURL_INCLUDE_DIRS})

  if (CURL_INCLUDE_DIRS)
    find_program(CURL_CONFIG_EXEC NAMES curl-config)
    if (CURL_CONFIG_EXEC)
      execute_process(
        COMMAND ${CURL_CONFIG_EXEC} --features
        OUTPUT_VARIABLE _libcurl_features
        ERROR_VARIABLE _libcurl_features_error
        )
      if (NOT ${_libcurl_features} MATCHES AsynchDNS)
        message(FATAL_ERROR "libcurl was not compiled with c-ares")
      endif ()
    else (CURL_CONFIG_EXEC)
      message(SEND_ERROR "Command \"${CURL_CONFIG_EXEC} --features\" failed with output:\n${_libcurl_features_error}")
    endif ()
  endif (CURL_INCLUDE_DIRS)

  find_package (SQLite3 REQUIRED)
  set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${SQLITE3_INCLUDE_DIR})

  find_package(leveldb)
  set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${LEVELDB_INCLUDE_DIR})

  find_package (UUID REQUIRED)
endif ()

# Client/libcvmfs/preloader only: pacparser, sparsehash
if (BUILD_CVMFS OR BUILD_LIBCVMFS OR BUILD_PRELOADER OR
    BUILD_UNITTESTS OR BUILD_UNITTESTS_DEBUG OR BUILD_SHRINKWRAP)
  find_package (pacparser REQUIRED)
  set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${PACPARSER_INCLUDE_DIR})

  find_package(Sparsehash)
  set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${SPARSEHASH_INCLUDE_DIR})
endif ()

if (BUILD_CVMFS OR BUILD_LIBCVMFS OR BUILD_LIBCVMFS_CACHE OR
    BUILD_UNITTESTS OR BUILD_UNITTESTS_DEBUG OR BUILD_UBENCHMARKS OR
    BUILD_SHRINKWRAP)
  find_package(Protobuf REQUIRED)
  set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${PROTOBUF_INCLUDE_DIRS})
endif ()

# Fuse client only
if (BUILD_CVMFS)
  if (MACOSX)
    find_package (OSXFuse)
    set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${MACOS_FUSE_INCLUDE_DIR})
    set (FUSE_LIBRARIES ${MACOS_FUSE_LIBRARIES}) # just abstract the difference here... they are compatible
  else (MACOSX)
    find_package (FUSE REQUIRED)
    set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${FUSE_INCLUDE_DIR})
    find_package (FUSE3)
    if (FUSE3_FOUND)
      set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${FUSE3_INCLUDE_DIR})
      add_definitions(-DHAS_FUSE3)
    endif ()
  endif (MACOSX)
endif ()

# Server only: unzip, libcap
if (BUILD_SERVER OR BUILD_SERVER_DEBUG)
  find_package(PythonLibs REQUIRED)
  find_package(LibCAP REQUIRED)
  find_program(UNZIP_EXE NAMES unzip)
  if (${UNZIP_EXE} STREQUAL "UNZIP_EXE-NOTFOUND")
    message(FATAL_ERROR "unzip utility missing. Please install unzip...")
  endif ()
endif ()


# Only micro benchmarks need google bench
if (BUILD_UBENCHMARKS)
  find_package(GOOGLEBENCH REQUIRED)
  set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${GOOGLEBENCH_INCLUDE_DIRS})
endif ()

include_directories (${INCLUDE_DIRECTORIES})

#
# go for the actual compilation
#
add_subdirectory (cvmfs)

if (INSTALL_MOUNT_SCRIPTS)
  add_subdirectory (mount)
endif ()

#
# compile the unit tests, if necessary
#
if (BUILD_UNITTESTS OR BUILD_UNITTESTS_DEBUG)
  enable_testing ()
  add_custom_target (check ${CMAKE_CTEST_COMMAND} -VV)
  add_subdirectory (test/unittests)
endif ()

if (BUILD_UBENCHMARKS)
  add_subdirectory (test/micro-benchmarks)
endif ()

if (BUILD_QC_TESTS)
  find_package(RapidCheck REQUIRED)
  set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${RAPIDCHECK_INCLUDE_DIRS})
  add_subdirectory (test/quickcheck)
endif ()

if (BUILD_GATEWAY OR BUILD_DUCC OR BUILD_SNAPSHOTTER)
  find_program(GO_COMPILER go)
  if(NOT GO_COMPILER)
    message(FATAL_ERROR "go compiler not found")
  else()
    message("Found go compiler: ${GO_COMPILER}")
  endif()
endif ()

if (BUILD_GATEWAY)
  add_subdirectory (gateway)
endif ()

if (BUILD_DUCC)
  add_subdirectory (ducc)
endif ()

if (BUILD_SNAPSHOTTER)
  add_subdirectory (snapshotter)
endif ()

if (BUILD_STRESS_TESTS)
  include(test/stress/CMakeLists.txt)
endif ()

#
# Documentation
#
install (
  FILES                   README.md AUTHORS ChangeLog COPYING
  DESTINATION             share/doc/cvmfs-${CernVM-FS_VERSION_STRING}
  PERMISSIONS             OWNER_WRITE OWNER_READ GROUP_READ WORLD_READ
)

#
# Generate the documentation using doxygen
#
if (BUILD_DOCUMENTATION)
  message (STATUS "Generating Doxygen configuration ...")
  set (CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR})
  set (DOXYFILE_SOURCE_DIR "${CMAKE_SOURCE_DIR}/cvmfs")
  set (DOXYFILE_OUTPUT_DIR "${CMAKE_BINARY_DIR}/")
  set (DOXYFILE_LATEX "OFF")
  execute_process (
    COMMAND sh -c "cp ${CMAKE_SOURCE_DIR}/doc/Doxyfile.in ${CMAKE_BINARY_DIR}/Doxyfile.in"
  )
  execute_process (
    COMMAND sh -c "/bin/sed -i -e 's/@VERSION@/${CernVM-FS_VERSION_STRING}/g' ${CMAKE_BINARY_DIR}/Doxyfile.in"
  )
  execute_process (
    COMMAND sh -c "/bin/sed -i -e 's,@SRC_DIR@,${CMAKE_SOURCE_DIR},g' ${CMAKE_BINARY_DIR}/Doxyfile.in"
  )
  include (cmake/Modules/UseDoxygen)
endif ()
