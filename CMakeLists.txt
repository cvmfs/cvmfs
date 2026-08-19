#
# CMake build script to configure and build CernVM-FS and all it's
# external dependencies, if they are statically linked into the binaries
#
# See externals/CMake-Register_External_Lib.txt for details on external inclusion
#

cmake_minimum_required (VERSION 3.24..3.30)

# Enable creation of compile_commands.json by default. Needed by LSPs, useful for devs
# Must be set before project() so the generator picks it up
set(CMAKE_EXPORT_COMPILE_COMMANDS ON CACHE INTERNAL "Export compile_commands.json for LSPs")

set (PROJECT_NAME "CernVM-FS")
project (${PROJECT_NAME})

message ("Running CMake version ${CMAKE_VERSION}")

#
# The version numbers
#
# DON'T DELETE
## CVMFS_VERSION 2.15.0
## CVMFS_PRERELEASE ~pre1
#---------------------
set (CernVM-FS_VERSION_MAJOR 2)
set (CernVM-FS_VERSION_MINOR 15)
set (CernVM-FS_VERSION_PATCH 0)
set (CernVM-FS_VERSION_STRING "${CernVM-FS_VERSION_MAJOR}.${CernVM-FS_VERSION_MINOR}.${CernVM-FS_VERSION_PATCH}")
if (CMAKE_VERSION VERSION_LESS "3.12.0")
  add_definitions(-DCVMFS_VERSION="${CernVM-FS_VERSION_STRING}")
  add_definitions(-DCVMFS_VERSION_MAJOR=${CernVM-FS_VERSION_MAJOR})
  add_definitions(-DCVMFS_VERSION_MINOR=${CernVM-FS_VERSION_MINOR})
  add_definitions(-DCVMFS_VERSION_PATCH=${CernVM-FS_VERSION_PATCH})
  add_definitions(-DCVMFS_PATCH_LEVEL="0")
else()
  add_compile_definitions(CVMFS_VERSION="${CernVM-FS_VERSION_STRING}")
  add_compile_definitions(CVMFS_VERSION_MAJOR=${CernVM-FS_VERSION_MAJOR})
  add_compile_definitions(CVMFS_VERSION_MINOR=${CernVM-FS_VERSION_MINOR})
  add_compile_definitions(CVMFS_VERSION_PATCH=${CernVM-FS_VERSION_PATCH})
  add_compile_definitions(CVMFS_PATCH_LEVEL="0")
endif()


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
  if(EXISTS "/etc/os-release")
    file(STRINGS "/etc/os-release" OS_RELEASE_LINES)
    foreach(line ${OS_RELEASE_LINES})
        if(line MATCHES "^ID=(.*)$")
            set(DISTRO_ID ${CMAKE_MATCH_1})
            string(REPLACE "\"" "" DISTRO_ID ${DISTRO_ID})
        endif()
        if(line MATCHES "^VERSION_ID=(.*)$")
            set(DISTRO_VERSION ${CMAKE_MATCH_1})
            string(REPLACE "\"" "" DISTRO_VERSION ${DISTRO_VERSION})
        endif()
        if(line MATCHES "^PLATFORM_ID=(.*)$")
            set(PLATFORM_ID ${CMAKE_MATCH_1})
            string(REPLACE "\"" "" PLATFORM_ID ${PLATFORM_ID})
        endif()
    endforeach()

    # Use PLATFORM_ID if available, otherwise fall back to ID+VERSION_ID
    if(PLATFORM_ID AND NOT PLATFORM_ID STREQUAL "")
        # Extract the part after the colon from PLATFORM_ID (e.g., "platform:el9" -> "el9")
        if(PLATFORM_ID MATCHES "^[^:]*:(.*)$")
            set(DISTRO ${CMAKE_MATCH_1})
        else()
            set(DISTRO ${PLATFORM_ID})
        endif()
    else()
        set(DISTRO "${DISTRO_ID}${DISTRO_VERSION}")
    endif()
  endif()
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
  set (CMAKE_INSTALL_LIBDIR "lib" CACHE STRING "library installation directory")
  set (CMAKE_MOUNT_INSTALL_BINDIR "${CMAKE_INSTALL_PREFIX}/sbin")
  set (CVMFS_LIBEXEC_DIR "libexec/cvmfs")
  set (CMAKE_MACOSX_RPATH true)
  list( APPEND CMAKE_INSTALL_RPATH ${CMAKE_INSTALL_PREFIX}/lib )
else (MACOSX) # --> Linux
  if (DEBIAN OR ARCHLINUX)
    if (ARCHLINUX)
      set (CMAKE_MOUNT_INSTALL_BINDIR "/usr/bin")
      set (CVMFS_LIBEXEC_DIR "lib/cvmfs")
    else (ARCHLINUX)
      set (CMAKE_MOUNT_INSTALL_BINDIR "/sbin")
      set (CVMFS_LIBEXEC_DIR "libexec/cvmfs")
    endif (ARCHLINUX)
    set (CMAKE_INSTALL_LIBDIR "lib" CACHE STRING "library installation directory")
  else (DEBIAN OR ARCHLINUX) # --> RedHat, Fedora, CentOS, SuSE
    set (CMAKE_MOUNT_INSTALL_BINDIR "/sbin")
    set (CVMFS_LIBEXEC_DIR "libexec/cvmfs")
    if (IS_64_BIT)
      set (CMAKE_INSTALL_LIBDIR "lib64" CACHE STRING "library installation directory")
    else (IS_64_BIT) # --> 32 Bit
      set (CMAKE_INSTALL_LIBDIR "lib" CACHE STRING "library installation directory")
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

# workaround for an issue with the libfuse3 target
if (MACOSX)
  set(BUILD_MANPAGES_DEFAULT OFF)
  set(BUILTIN_EXTERNALS_EXCLUDE_DEFAULT "sqlite3;zlib")
else()
  set(BUILD_MANPAGES_DEFAULT ON)
  set(BUILTIN_EXTERNALS_EXCLUDE_DEFAULT "")
endif()

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

# Download cache for FetchContent-based externals. Defaults into the source
# tree so that a checkout with a populated download cache can be packed into a
# self-contained source tarball. Override (e.g. in read-only container builds
# where the source is bind-mounted read-only) to point at a writable location.
set (EXTERNALS_DOWNLOAD_LOCATION  "${EXTERNALS_LIB_LOCATION}/download"
     CACHE PATH "Download cache directory for vendored externals")

# For convenience in CI workflows, allow to set EXTERNALS_PREFIX via environment
if (NOT EXTERNALS_PREFIX)
  if (NOT $ENV{CVMFS_EXTERNALS_PREFIX} STREQUAL "")
    set(EXTERNALS_PREFIX $ENV{CVMFS_EXTERNALS_PREFIX})
  else()
    set(EXTERNALS_PREFIX ${CMAKE_SOURCE_DIR})
  endif()
endif()
# Check if legacy directory names exist to try to avoid unnecessary rebuilds
if(EXISTS "${EXTERNALS_PREFIX}/externals_build" AND EXISTS "${EXTERNALS_PREFIX}/externals_install")
    set (EXTERNALS_BUILD_LOCATION     "${EXTERNALS_PREFIX}/externals_build")
    set (EXTERNALS_INSTALL_LOCATION   "${EXTERNALS_PREFIX}/externals_install")
else()
    set (EXTERNALS_BUILD_LOCATION     "${EXTERNALS_PREFIX}/externals_build.${CMAKE_SYSTEM_PROCESSOR}")
    set (EXTERNALS_INSTALL_LOCATION   "${EXTERNALS_PREFIX}/externals_install.${CMAKE_SYSTEM_PROCESSOR}")
    if(DISTRO AND NOT DISTRO STREQUAL "")
        string(APPEND EXTERNALS_INSTALL_LOCATION ".${DISTRO}")
        string(APPEND EXTERNALS_BUILD_LOCATION ".${DISTRO}")
    endif()
endif()

#
# run the bootstrap shellscript (not needed in the distributed version of the source)
#
if (BUILTIN_EXTERNALS)
  if (EXISTS "${CMAKE_SOURCE_DIR}/externals/bootstrap.sh")
    # Set some environment variables for the bootstrap script
    set(ENV{EXTERNALS_LIB_LOCATION} ${EXTERNALS_LIB_LOCATION})
    set(ENV{EXTERNALS_BUILD_LOCATION} ${EXTERNALS_BUILD_LOCATION})
    set(ENV{EXTERNALS_INSTALL_LOCATION} ${EXTERNALS_INSTALL_LOCATION})
    set(ENV{CVMFS_BASE_C_FLAGS} ${CVMFS_BASE_C_FLAGS})
    set(ENV{CVMFS_BASE_CXX_FLAGS} ${CVMFS_BASE_CXX_FLAGS})
    set(ENV{IS_64_BIT} ${IS_64_BIT})

    if (BUILD_SERVER OR BUILD_SERVER_DEBUG)
      set(ENV{BUILD_SERVER} "true")
    endif ()
    if (BUILD_GEOAPI)
      set(ENV{BUILD_GEOAPI} "true")
    endif ()
    if (BUILD_GATEWAY)
      set(ENV{BUILD_GATEWAY} "true")
    endif ()
    if (BUILD_DUCC)
      set(ENV{BUILD_DUCC} "true")
    endif ()
    if (BUILD_SNAPSHOTTER)
      set(ENV{BUILD_SNAPSHOTTER} "true")
    endif()
    if (BUILTIN_EXTERNALS_LIST)
      set(ENV{BUILTIN_EXTERNALS_LIST} "${BUILTIN_EXTERNALS_LIST}")
    endif()
    if (BUILTIN_EXTERNALS_EXCLUDE)
      set(ENV{BUILTIN_EXTERNALS_EXCLUDE} "${BUILTIN_EXTERNALS_EXCLUDE}")
    endif()

    message (STATUS "running bootstrap.sh ...")
    execute_process (
      COMMAND           ${CMAKE_SOURCE_DIR}/externals/bootstrap.sh
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
    set(ENV{BUILD_SERVER} "")
    set(ENV{BUILD_GATEWAY} "")
    set(ENV{BUILD_DUCC} "")
    set(ENV{BUILD_SNAPSHOTTER} "")
    set(ENV{BUILTIN_EXTERNALS_LIST} "")
    set(ENV{BUILTIN_EXTERNALS_EXCLUDE} "")
  endif (EXISTS "${CMAKE_SOURCE_DIR}/externals/bootstrap.sh")

  # In the case of built-in external libraries, we need to set CMAKE_PREFIX_PATH to
  # point to the prefix where the libraries are installed. This path will take precedence
  # over the system prefix when find_package(****) is called later
  set(CMAKE_PREFIX_PATH ${EXTERNALS_INSTALL_LOCATION})
  message("Project prefix path: ${CMAKE_PREFIX_PATH}")
  message("System prefix path: ${CMAKE_SYSTEM_PREFIX_PATH}")
endif(BUILTIN_EXTERNALS)


# Migrate bootstrap.sh to cmake
if(BUILTIN_EXTERNALS)
  message(STATUS "Entering external dependencies setup")
  # Some vendored externals (e.g. libarchive) force their preferred
  # CMAKE_BUILD_TYPE into the cache; save the configured value and restore
  # it afterwards.
  set(_cvmfs_build_type "${CMAKE_BUILD_TYPE}")
  add_subdirectory(externals)
  set(CMAKE_BUILD_TYPE "${_cvmfs_build_type}" CACHE STRING "Build Type" FORCE)
endif()

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

if (CHECK_SYSTEM_HEADERS)
  look_for_required_include_files (${REQUIRED_HEADERS})
  look_for_optional_include_files (${OPTIONAL_HEADERS})
endif()


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


# Almost all build targets require zlib and sha3
if (BUILD_CVMFS OR BUILD_LIBCVMFS OR BUILD_SERVER OR BUILD_SERVER_DEBUG OR
    BUILD_UNITTESTS OR BUILD_UNITTESTS_DEBUG OR BUILD_PRELOADER OR
    BUILD_UBENCHMARKS OR BUILD_SHRINKWRAP)
  # zlib is provided by the externals/zlib FetchContent module (or the system,
  # via BUILTIN_EXTERNALS_EXCLUDE) as the ZLIB::ZLIB target; it also sets
  # ZLIB_LIBRARIES and ZLIB_INCLUDE_DIRS for the global include list below.
  set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${ZLIB_INCLUDE_DIRS})

  # Libnettle_* is set by externals/libnettle (system or vendored build)
  set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${Libnettle_INCLUDE_DIRS})
endif ()


# Most build target require also curl/c-ares, sqlite, uuid, leveldb
if (BUILD_CVMFS OR BUILD_LIBCVMFS OR BUILD_SERVER OR BUILD_SERVER_DEBUG OR
    BUILD_UNITTESTS OR BUILD_UNITTESTS_DEBUG OR BUILD_PRELOADER OR
    BUILD_SHRINKWRAP)
  # c-ares and libcurl are provided as the c-ares::cares and CURL::libcurl
  # targets by add_subdirectory(externals) above (FetchContent). libcurl is
  # always built with c-ares as its async DNS resolver, so the former
  # curl-config AsynchDNS check is no longer needed.

  # sqlite3 is provided by the externals/sqlite3 FetchContent module (or the
  # system, via BUILTIN_EXTERNALS_EXCLUDE) as the SQLite::SQLite3 target; it also
  # sets SQLITE3_INCLUDE_DIR for the global include list below.
  set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${SQLITE3_INCLUDE_DIR})

  # leveldb is provided by the externals/leveldb FetchContent module (or the
  # system, via BUILTIN_EXTERNALS_EXCLUDE) as the leveldb::leveldb target.

  find_package (UUID REQUIRED)
endif ()

# Client/libcvmfs/preloader only: pacparser, sparsehash
# pacparser is provided by the externals/pacparser FetchContent module (or the
# system, via BUILTIN_EXTERNALS_EXCLUDE) as the pacparser::pacparser target.
# sparsehash is provided the same way (externals/sparsehash), as the header-only
# sparsehash::sparsehash target. It is header-only, so we also expose its
# include directory globally here; SPARSEHASH_INCLUDE_DIR is set either by that
# module or, for BUILTIN_EXTERNALS=OFF builds, by the system find_package below.
if (BUILD_CVMFS OR BUILD_LIBCVMFS OR BUILD_PRELOADER OR
    BUILD_UNITTESTS OR BUILD_UNITTESTS_DEBUG OR BUILD_SHRINKWRAP)
  if (NOT SPARSEHASH_INCLUDE_DIR)
    find_package(Sparsehash REQUIRED)
  endif ()
  set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${SPARSEHASH_INCLUDE_DIR})
endif ()

if (BUILD_CVMFS OR BUILD_LIBCVMFS OR BUILD_LIBCVMFS_CACHE OR
    BUILD_UNITTESTS OR BUILD_UNITTESTS_DEBUG OR BUILD_UBENCHMARKS OR
    BUILD_SHRINKWRAP)
  # protobuf (protobuf-lite + protoc) is provided by the externals/protobuf
  # FetchContent module (or the system, via BUILTIN_EXTERNALS_EXCLUDE) as the
  # protobuf::libprotobuf-lite / protobuf::protoc targets, exposed through the
  # PROTOBUF_LITE_LIBRARY / PROTOBUF_PROTOC_EXECUTABLE / PROTOBUF_INCLUDE_DIRS
  # variables.
  set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${PROTOBUF_INCLUDE_DIRS})
endif ()

# Fuse client only
if (BUILD_CVMFS)
  if (MACOSX)
    set(BUILD_LIBFUSE2 ON)
    find_package (OSXFuse REQUIRED)
    set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${MACOS_FUSE_INCLUDE_DIR})
    set (FUSE_LIBRARIES ${MACOS_FUSE_LIBRARIES}) # just abstract the difference here... they are compatible
    if (USE_MACFUSE_KEXT)
      add_definitions(-DUSE_MACFUSE_KEXT)
    endif()
  else (MACOSX)
    if (BUILD_LIBFUSE2)
      find_package (FUSE REQUIRED)
      if (FUSE_FOUND)
        set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${FUSE_INCLUDE_DIR})
      endif ()
    endif()
    find_package (FUSE3 REQUIRED)
    set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${FUSE3_INCLUDE_DIR})
    add_definitions(-DHAS_FUSE3)
  endif (MACOSX)
endif ()

# libcap is needed by every target that links cvmfs/util/capabilities.cc,
# i.e. everything that builds libcvmfs-util.
# macOS has stubbed-out implementations in capabilities.{cc,h} that don't
# reference libcap symbols, so the lookup is skipped there.
if (NOT MACOSX AND (BUILD_CVMFS OR BUILD_LIBCVMFS OR BUILD_LIBCVMFS_CACHE OR
                    BUILD_SERVER OR BUILD_SERVER_DEBUG OR BUILD_RECEIVER OR
                    BUILD_SHRINKWRAP OR BUILD_PRELOADER OR BUILD_UNITTESTS OR
                    BUILD_STRESS_TESTS OR INSTALL_MOUNT_SCRIPTS))
  find_package(LibCAP REQUIRED)
endif ()

# Server only: python, unzip, libacl
if (BUILD_SERVER OR BUILD_SERVER_DEBUG)
  find_package(Python COMPONENTS Interpreter REQUIRED)
  if (BUILD_COMPARE_TO_LIBACL)
    add_definitions(-DCOMPARE_TO_LIBACL)
    find_package(LibACL REQUIRED)
  endif()
  find_program(UNZIP_EXE NAMES unzip)
  if (${UNZIP_EXE} STREQUAL "UNZIP_EXE-NOTFOUND")
    message(FATAL_ERROR "unzip utility missing. Please install unzip...")
  endif ()
endif ()


# Only micro benchmarks need google bench. It is provided by the
# externals/googlebench FetchContent module (or the system, via
# BUILTIN_EXTERNALS_EXCLUDE) as the benchmark::benchmark target, which the
# micro-benchmark executable links directly (its include dirs propagate
# transitively, so nothing needs adding to INCLUDE_DIRECTORIES here).

include_directories (${INCLUDE_DIRECTORIES})

#
# go for the actual compilation
#
add_subdirectory (cvmfs)

if (INSTALL_MOUNT_SCRIPTS)
  add_subdirectory (mount)
endif ()

#
# GoogleTest is needed by both the unit tests and the micro benchmarks
#
if (BUILD_UNITTESTS OR BUILD_UNITTESTS_DEBUG OR BUILD_UBENCHMARKS)
  include(SetupGoogleTest)
endif ()

#
# compile the unit tests, if necessary
#
if (BUILD_UNITTESTS OR BUILD_UNITTESTS_DEBUG)
  add_compile_definitions(_CVMFS_UNITTESTS_ENABLED)
  enable_testing ()
  # FuzzTest pulls in Abseil, whose absl/log defines a target named `check`
  # (absl::check). Skip our `make check` convenience target when the quickcheck
  # tests are built to avoid a target name collision
  if (NOT BUILD_QC_TESTS)
    add_custom_target (check ${CMAKE_CTEST_COMMAND} -VV)
  endif ()
  add_subdirectory (test/unittests)
endif ()

if (BUILD_UBENCHMARKS)
  add_subdirectory (test/micro-benchmarks)
endif ()

if (BUILD_QC_TESTS)
  add_subdirectory (test/quickcheck)
endif ()

if (BUILD_STRESS_TESTS)
  include(test/stress/CMakeLists.txt)
endif ()

#
# Builds of golang components
# First check if we have golang available
#
if (BUILD_GATEWAY OR BUILD_DUCC OR BUILD_SNAPSHOTTER)
  if (NOT CVMFS_GO_DIR AND NOT "$ENV{CVMFS_GO_DIR}" STREQUAL "")
    set (CVMFS_GO_DIR "$ENV{CVMFS_GO_DIR}")
  endif ()
  find_program(GO_COMPILER go
               HINTS ${CVMFS_GO_DIR} "${EXTERNALS_INSTALL_LOCATION}/go/bin")
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


#
# Documentation
#
install (
  FILES                   README.md CITATION.cff ChangeLog COPYING
  DESTINATION             share/doc/cvmfs-${CernVM-FS_VERSION_STRING}
  PERMISSIONS             OWNER_WRITE OWNER_READ GROUP_READ WORLD_READ
)

# Man pages
if (BUILD_MANPAGES)
  add_subdirectory(doc/manpages)
endif()

# Doxygen
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
