#
# CMake build script to configure and build CernVM-FS and all it's
# external dependencies, if they are statically linked into the binaries
#
# See externals/CMake-Register_External_Lib.txt for details on external inclusion
#

cmake_minimum_required (VERSION 2.6.2)
set (PROJECT_NAME "CernVM-FS")
project (${PROJECT_NAME})

message ("Running CMake version ${CMAKE_VERSION}")

#
# The version numbers
#
# DON'T DELETE
## CVMFS_VERSION 2.4.0
#---------------------
set (CernVM-FS_VERSION_MAJOR 2)
set (CernVM-FS_VERSION_MINOR 4)
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
else (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
  set (MACOSX FALSE)
endif (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")

if (${CMAKE_SYSTEM_NAME} MATCHES "Linux")
  set (LINUX TRUE)
  if (EXISTS /etc/debian_version)
    set (DEBIAN TRUE)
  endif (EXISTS /etc/debian_version)
  if (EXISTS /etc/arch-release)
    set (ARCHLINUX TRUE)
  endif (EXISTS /etc/arch-release)
else (${CMAKE_SYSTEM_NAME} MATCHES "Linux")
  set (LINUX FALSE)
  set (DEBIAN FALSE)
  set (ARCHLINUX FALSE)
endif (${CMAKE_SYSTEM_NAME} MATCHES "Linux")

#
# Set install prefix to /usr by default.
#
if (LINUX AND CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)
  set (CMAKE_INSTALL_PREFIX "/usr" CACHE PATH "CVMFS install path default is /usr" FORCE)
  message("Setting default install prefix to ${CMAKE_INSTALL_PREFIX} on Linux")
endif (LINUX AND CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)

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
else (CMAKE_SIZEOF_VOID_P EQUAL 4)
  set (IS_64_BIT TRUE)
endif (CMAKE_SIZEOF_VOID_P EQUAL 4)

#
# check if we are compiling on ARM
#
if (${CMAKE_SYSTEM_PROCESSOR} MATCHES "^arm.*$")
  set (ARM TRUE)
else (${CMAKE_SYSTEM_PROCESSOR} MATCHES "^arm.*$")
  set (ARM FALSE)
endif (${CMAKE_SYSTEM_PROCESSOR} MATCHES "^arm.*$")

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
  else ("${CMAKE_INSTALL_PREFIX}" STREQUAL "/usr")
    set(SYSCONF_INSTALL_DIR "${CMAKE_INSTALL_PREFIX}/etc")
  endif ("${CMAKE_INSTALL_PREFIX}" STREQUAL "/usr")
else (NOT DEFINED SYSCONF_INSTALL_DIR)
  set (SYSCONF_INSTALL_DIR "${SYSCONF_INSTALL_DIR}" CACHE STRING "The sysconfig install dir")
endif (NOT DEFINED SYSCONF_INSTALL_DIR)

#
# Workaround for Debian packaging debhelper trying to pass -D_FORTIFY_SOURCE=2
# through CPPFLAGS that is not officially supported by CMake. Hence, debhelper
# appends CPPFLAGS to CFLAGS which breaks the build of the c-ares external.
# This filters out flags that scares c-ares's ./configure script.
#
set (TMP_C_FLAGS "${CFLAGS} ${CMAKE_C_FLAGS}")
set (CFLAGS        "") # this should better be `unset()` but CMake 2.6.2 doesn't
set (ENV{CFLAGS}   "") # support it. Should be changed as soon as SLES 11 brings
set (CMAKE_C_FLAGS "") # an update for CMake or we drop support for the platform
set (LDFLAGS $ENV{LDFLAGS})
separate_arguments (TMP_C_FLAGS)
foreach (CMPLR_FLAG ${TMP_C_FLAGS})
  if (${CMPLR_FLAG} MATCHES ^-[DIU].*)
    message ("Moving ${CMPLR_FLAG} from CFLAGS into CPPFLAGS")
    set (CPPFLAGS "${CPPFLAGS} ${CMPLR_FLAG}")
  elseif (${CMPLR_FLAG} MATCHES ^-[Ll].*)
    message ("Moving ${CMPLR_FLAG} from CFLAGS into LDFLAGS")
    set (LDFLAGS "${LDFLAGS} ${CMPLR_FLAG}")
    set (CMAKE_LD_FLAGS "${CMAKE_LD_FLAGS} ${CMPLR_FLAG}")
  else (${CMPLR_FLAG} MATCHES ^-[DUILl].*)
    set (CFLAGS "${CFLAGS} ${CMPLR_FLAG}")
  endif (${CMPLR_FLAG} MATCHES ^-[DIU].*)
endforeach (CMPLR_FLAG)
set (ENV{CPPFLAGS}   "${CPPFLAGS}")
set (CMAKE_C_FLAGS   "${CFLAGS}")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CPPFLAGS}")
set (ENV{CFLAGS}     "${CFLAGS}")
set (ENV{LDFLAGS}    "${LDFLAGS}")

#
# set name of fuse library (-losxfuse for osxfuse)
#
if (MACOSX)
  set(LIBFUSE osxfuse)
else (MACOSX)
  set(LIBFUSE fuse)
endif(MACOSX)

#
# include file with user-defined options
#
include (options.cmake)

#
# define where to find the external dependencies
#
# for the external libraries there should be a CVMFS-CMakeLists.txt file provided
# in the given directory, in order to configure and compile them properly
#
set (EXTERNALS_LIB_LOCATION       "${CMAKE_SOURCE_DIR}/externals")
set (EXTERNALS_BUILD_LOCATION     "${CMAKE_SOURCE_DIR}/externals_build")
set (EXTERNALS_INSTALL_LOCATION)  "${CMAKE_SOURCE_DIR}/externals_install")

# create the directory for controlled out of source building of external stuff
file (MAKE_DIRECTORY ${EXTERNALS_BUILD_LOCATION})

#
# run the bootstrap shellscript (not needed in the distributed version of the source)
#
if (BUILD_EXTERNALS)
  if (EXISTS "${CMAKE_SOURCE_DIR}/bootstrap.sh")
    message (STATUS "running bootstrap.sh ...")
    execute_process (
      COMMAND           sh ${CMAKE_SOURCE_DIR}/bootstrap.sh ${EXTERNALS_BUILD_LOCATION}
      WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
      RESULT_VARIABLE   BOOTSTRAPPING_RESULT
    )
    if (BOOTSTRAPPING_RESULT GREATER 0)
      message (FATAL_ERROR "bootstrapping failed")
    endif (BOOTSTRAPPING_RESULT GREATER 0)
  endif (EXISTS "${CMAKE_SOURCE_DIR}/bootstrap.sh")
endif(BUILD_EXTERNALS)

#
# include some common functionality
#
include (FindPythonModule)


#
# set some default flags
#
# flags in CMAKE_C**_FLAGS are always passed to the compiler
#
include (compiler)

if (BUILD_COVERAGE AND NOT USING_CLANG)
  set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fprofile-arcs -ftest-coverage")
  set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0 -g -fprofile-arcs -ftest-coverage -fPIC")
endif (BUILD_COVERAGE AND NOT USING_CLANG)

set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} cvmfs)

#
# check existence of include files
#
include (CheckIncludeFile)

macro (pedantic_include_check HEADERFILE VARIABLENAME)
  check_include_file (${HEADERFILE} ${VARIABLENAME})
  if (NOT ${VARIABLENAME})
    message (FATAL_ERROR "${HEADERFILE} is missing on your system")
  endif (NOT ${VARIABLENAME})
endmacro (pedantic_include_check)

macro (look_for_include_files)
  foreach (HEADER ${ARGV})
    string (REGEX REPLACE "/|\\." "_" HEADER_VAR_LOWER ${HEADER})
    string (TOUPPER "have_${HEADER_VAR_LOWER}" HEADER_VAR)
    pedantic_include_check (${HEADER} ${HEADER_VAR})
  endforeach (HEADER IN ITEMS ${ARGV})
endmacro (look_for_include_files)

set (REQUIRED_HEADERS sys/xattr.h zlib.h netinet/in.h arpa/inet.h sys/socket.h
                      sys/un.h sys/time.h sys/uio.h sys/stat.h sys/types.h
                      sys/wait.h sys/select.h pthread.h termios.h utime.h
                      signal.h errno.h dirent.h unistd.h fcntl.h netdb.h
                      syslog.h sys/resource.h execinfo.h poll.h pwd.h grp.h)
if (NOT MACOSX)
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
  set (REQUIRED_HEADERS ${REQUIRED_HEADERS}
                        sys/statfs.h
                        attr/xattr.h)
endif (NOT MACOSX)

look_for_include_files (${REQUIRED_HEADERS})

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
# some libraries are statically linked and has to be compiled from source
# therefore we have to include the specific CVMFS-CMakeLists.txt from the
# different 3rd-party locations.
# If dynamic linking is desired we have to look for the presence of the libs
# in the system and set the LIBRARY and INCLUDE variables properly
#
# either ***_LIBRARIES or ***_ARCHIVE has to be defined for all externals
# in order to successfully link the targets.
#

# Optional libraries

find_package (Valgrind)
if (VALGRIND_FOUND)
  set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${VALGRIND_INCLUDE_DIR})
  add_definitions(-DHAS_VALGRIND_HEADERS)
endif (VALGRIND_FOUND)

if (NOT MACOSX)
  set (HAVE_LIB_RT TRUE)
  set (RT_LIBRARY "rt")
else (NOT MACOSX)
  set (HAVE_LIB_RT FALSE)
  set (RT_LIBRARY "")
endif (NOT MACOSX)

# Required libraries depending on build target

# Use builtin SSL, otherwise use the system OpenSSL
if (MACOSX)
    include (${SSL_BUILTIN_LOCATION}/CVMFS-CMakeLists.txt)
    set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${SSL_BUILTIN_LOCATION}/include)
    set (OPENSSL_LIBRARIES ${SSL_ARCHIVE})
else (MACOSX)
    find_package (OpenSSL REQUIRED)
    set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${OPENSSL_INCLUDE_DIR})
endif(MACOSX)

if (GOOGLETEST_BUILTIN)
  include (${GOOGLETEST_BUILTIN_LOCATION}/CVMFS-CMakeLists.txt)
  set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${GOOGLETEST_BUILTIN_LOCATION}/include)
else (GOOGLETEST_BUILTIN)
  find_package (GTest REQUIRED)
  set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${GTEST_INCLUDE_DIRS})
endif (GOOGLETEST_BUILTIN)

include (${VJSON_BUILTIN_LOCATION}/CVMFS-CMakeLists.txt)
set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${VJSON_BUILTIN_LOCATION}/src)


# Almost all build targets require zlib, sha2/3
if (BUILD_CVMFS OR BUILD_LIBCVMFS OR BUILD_SERVER OR BUILD_SERVER_DEBUG OR
    BUILD_UNITTESTS OR BUILD_UNITTESTS_DEBUG OR BUILD_PRELOADER OR
    BUILD_UBENCHMARKS)
  if (ZLIB_BUILTIN)
    include (${ZLIB_BUILTIN_LOCATION}/CVMFS-CMakeLists.txt)
    set (ZLIB_LIBRARIES "")
  else (ZLIB_BUILTIN)
    find_package (ZLIB REQUIRED)
    set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${ZLIB_INCLUDE_DIRS})
  endif (ZLIB_BUILTIN)

  include (${SHA2_BUILTIN_LOCATION}/CVMFS-CMakeLists.txt)
  include (${SHA3_BUILTIN_LOCATION}/CVMFS-CMakeLists.txt)
endif (BUILD_CVMFS OR BUILD_LIBCVMFS OR BUILD_SERVER OR BUILD_SERVER_DEBUG OR
       BUILD_UNITTESTS OR BUILD_UNITTESTS_DEBUG OR BUILD_PRELOADER OR
       BUILD_UBENCHMARKS)


# Most build target require also curl/c-ares, sqlite, uuid, leveldb
if (BUILD_CVMFS OR BUILD_LIBCVMFS OR BUILD_SERVER OR BUILD_SERVER_DEBUG OR
    BUILD_UNITTESTS OR BUILD_UNITTESTS_DEBUG OR BUILD_PRELOADER)
  if (LIBCURL_BUILTIN)
    include (${CARES_BUILTIN_LOCATION}/CVMFS-CMakeLists.txt)
    include (${LIBCURL_BUILTIN_LOCATION}/CVMFS-CMakeLists.txt)
    set (CURL_LIBRARIES "")
    set (CARES_LIBRARIES "")
  else (LIBCURL_BUILTIN)
    find_package (CURL REQUIRED)
    find_package (CARES REQUIRED)
    set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${CURL_INCLUDE_DIRS})
    set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${CARES_INCLUDE_DIRS})
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
        endif (NOT ${_libcurl_features} MATCHES AsynchDNS)
      else (CURL_CONFIG_EXEC)
        message(SEND_ERROR "Command \"${CURL_CONFIG_EXEC} --features\" failed with output:\n${_libcurl_features_error}")
      endif (CURL_CONFIG_EXEC)
    endif(CURL_INCLUDE_DIRS)
  endif (LIBCURL_BUILTIN)

  if (SQLITE3_BUILTIN)
    include (${SQLITE3_BUILTIN_LOCATION}/CVMFS-CMakeLists.txt)
    set (SQLITE3_LIBRARY "")
  else (SQLITE3_BUILTIN)
    find_package (SQLite3 REQUIRED)
    set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${SQLITE3_INCLUDE_DIR})
  endif (SQLITE3_BUILTIN)

  if (LEVELDB_BUILTIN)
    include (${LEVELDB_BUILTIN_LOCATION}/CVMFS-CMakeLists.txt)
    set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${LEVELDB_BUILTIN_LOCATION}/src)
  else (LEVELDB_BUILTIN)
    find_package(leveldb)
    set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${LEVELDB_INCLUDE_DIR})
  endif (LEVELDB_BUILTIN)

  find_package (UUID REQUIRED)
endif (BUILD_CVMFS OR BUILD_LIBCVMFS OR BUILD_SERVER OR BUILD_SERVER_DEBUG OR
       BUILD_UNITTESTS OR BUILD_UNITTESTS_DEBUG OR BUILD_PRELOADER)

# Client/libcvmfs/preloader only: pacparser, sparsehash
if (BUILD_CVMFS OR BUILD_LIBCVMFS OR BUILD_PRELOADER OR
    BUILD_UNITTESTS OR BUILD_UNITTESTS_DEBUG)
  if (PACPARSER_BUILTIN)
    include (${PACPARSER_BUILTIN_LOCATION}/CVMFS-CMakeLists.txt)
    set (PACPARSER_LIBRARIES "")
  else (PACPARSER_BUILTIN)
    find_package (pacparser REQUIRED)
    set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${PACPARSER_INCLUDE_DIR})
  endif (PACPARSER_BUILTIN)

  if (SPARSEHASH_BUILTIN)
    include (${SPARSEHASH_BUILTIN_LOCATION}/CVMFS-CMakeLists.txt)
    set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${SPARSEHASH_BUILTIN_LOCATION}/src)
  else (SPARSEHASH_BUILTIN)
    find_package(Sparsehash)
    set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${SPARSEHASH_INCLUDE_DIR})
  endif (SPARSEHASH_BUILTIN)
endif (BUILD_CVMFS OR BUILD_LIBCVMFS OR BUILD_PRELOADER OR
       BUILD_UNITTESTS OR BUILD_UNITTESTS_DEBUG)

# Client/libcvmfs only
if (BUILD_CVMFS OR BUILD_LIBCVMFS OR BUILD_LIBCVMFS_CACHE OR
    BUILD_UNITTESTS OR BUILD_UNITTESTS_DEBUG OR BUILD_UBENCHMARKS)
  include (${PROTOBUF_BUILTIN_LOCATION}/CVMFS-CMakeLists.txt)
endif (BUILD_CVMFS OR BUILD_LIBCVMFS OR BUILD_LIBCVMFS_CACHE OR
       BUILD_UNITTESTS OR BUILD_UNITTESTS_DEBUG OR BUILD_UBENCHMARKS)

# Fuse client only
if (BUILD_CVMFS)
  if (MACOSX)
    find_package (OSXFuse REQUIRED)
    set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${OSXFUSE_INCLUDE_DIR})
    set (FUSE_LIBRARIES ${OSXFUSE_LIBRARIES}) # just abstract the difference here... they are compatible
  else (MACOSX)
    find_package (FUSE REQUIRED)
    set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${FUSE_INCLUDE_DIR})
  endif (MACOSX)
endif (BUILD_CVMFS)

# Server/preloader only: tbb
if (BUILD_SERVER OR BUILD_SERVER_DEBUG OR BUILD_PRELOADER OR
    BUILD_UNITTESTS OR BUILD_UNITTESTS_DEBUG)
  if (TBB_PRIVATE_LIB)
    include (${TBB_BUILD_LOCATION}/CVMFS-CMakeLists.txt)
    set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${TBB_BUILD_LOCATION}/include)
  else (TBB_PRIVATE_LIB)
    find_package(TBB REQUIRED)
    set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${TBB_INCLUDE_DIRS})
  endif (TBB_PRIVATE_LIB)
endif (BUILD_SERVER OR BUILD_SERVER_DEBUG OR BUILD_PRELOADER OR
       BUILD_UNITTESTS OR BUILD_UNITTESTS_DEBUG)

# Server only: python/geoip, unzip, libcap
if (BUILD_SERVER OR BUILD_SERVER_DEBUG)
  find_package(PythonLibs REQUIRED)
  find_package(LibCAP REQUIRED)
  find_program(UNZIP_EXE NAMES unzip)
  if (${UNZIP_EXE} STREQUAL "UNZIP_EXE-NOTFOUND")
    message(FATAL_ERROR "unzip utility missing. Please install unzip...")
  endif (${UNZIP_EXE} STREQUAL "UNZIP_EXE-NOTFOUND")

  if (GEOIP_BUILTIN)
    include (${LIBGEOIP_BUILTIN_LOCATION}/CVMFS-CMakeLists.txt)
    include (${PYTHON_GEOIP_BUILTIN_LOCATION}/CVMFS-CMakeLists.txt)
    add_dependencies (python-geoip libgeoip)
  else (GEOIP_BUILTIN)
    find_package (LibGeoIP REQUIRED)
    find_python_module (GeoIP REQUIRED)
  endif (GEOIP_BUILTIN)
endif (BUILD_SERVER OR BUILD_SERVER_DEBUG)


# Only micro benchmarks need google bench
if (BUILD_UBENCHMARKS)
  include (${GOOGLEBENCH_BUILTIN_LOCATION}/CVMFS-CMakeLists.txt)
  set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${GOOGLEBENCH_BUILTIN_LOCATION}/include)
endif (BUILD_UBENCHMARKS)

include_directories (${INCLUDE_DIRECTORIES})

#
# go for the actual compilation
#
add_subdirectory (cvmfs)

if (INSTALL_MOUNT_SCRIPTS)
  add_subdirectory (mount)
endif (INSTALL_MOUNT_SCRIPTS)

#
# compile the unit tests, if necessary
#
if (BUILD_UNITTESTS OR BUILD_UNITTESTS_DEBUG)
  enable_testing ()
  add_custom_target (check ${CMAKE_CTEST_COMMAND} -VV)
  add_subdirectory (test/unittests)
endif (BUILD_UNITTESTS OR BUILD_UNITTESTS_DEBUG)

if (BUILD_UBENCHMARKS)
  add_subdirectory (test/micro-benchmarks)
endif (BUILD_UBENCHMARKS)

#
# Documentation
#
install (
    FILES                   README AUTHORS ChangeLog COPYING
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
endif (BUILD_DOCUMENTATION)
