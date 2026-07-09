# CMake module for vendoring sparsehash 
include(FetchContent)

set(SPARSEHASH_VERSION "1.12")
set(SPARSEHASH_URL "https://github.com/sparsehash/sparsehash/archive/refs/tags/sparsehash-${SPARSEHASH_VERSION}.tar.gz" "https://ecsft.cern.ch/dist/cvmfs/build_externals/sparsehash-${SPARSEHASH_VERSION}.tar.gz")
set(SPARSEHASH_HASH "MD5=c8343d730fd7d890799f7b668e4ec263")

# -> sparsehash <-
# NOTE: sparsehash is header-only to *consume*, but upstream ships an
# autotools-generated google/sparsehash/sparseconfig.h. Rather than run
# ./configure we regenerate that one header below with a couple of CMake
# probes.
#
# The result is exposed as the header-only imported target
#
#   sparsehash::sparsehash

setup_external_build_mode(sparsehash)

FetchContent_Declare(
  Sparsehash
  URL "${SPARSEHASH_URL}"
  URL_HASH "${SPARSEHASH_HASH}"
  DOWNLOAD_EXTRACT_TIMESTAMP TRUE
  PATCH_COMMAND patch -p0 < ${CMAKE_CURRENT_SOURCE_DIR}/src/fix_sl4_compilation.patch
        COMMAND patch -p0 < ${CMAKE_CURRENT_SOURCE_DIR}/src/fix_warning_gcc48.patch
  EXCLUDE_FROM_ALL
  FIND_PACKAGE_ARGS
)

# sparsehash has no CMakeLists.txt: FetchContent_MakeAvailable only downloads,
# extracts and patches the sources (it skips the add_subdirectory step); we
# define the header-only target against ${sparsehash_SOURCE_DIR} below.
FetchContent_MakeAvailable(Sparsehash)
unset(FETCHCONTENT_TRY_FIND_PACKAGE_MODE)

if(Sparsehash_FOUND)
  message(STATUS "Found system sparsehash: ${SPARSEHASH_INCLUDE_DIR}")
  # FindSparsehash.cmake exposes the uniform sparsehash::sparsehash target.
else()
  message(STATUS "Building sparsehash from vendored sources.")

  # Regenerate google/sparsehash/sparseconfig.h (normally produced by
  # ./configure). cvmfs always passes an explicit hash functor, so only the
  # HAVE_* probes below actually influence the compiled headers.
  include(CheckIncludeFileCXX)
  include(CheckTypeSize)
  include(CheckCXXSymbolExists)
  check_include_file_cxx("stdint.h"    HAVE_STDINT_H)
  check_include_file_cxx("inttypes.h"  HAVE_INTTYPES_H)
  check_include_file_cxx("sys/types.h" HAVE_SYS_TYPES_H)
  check_type_size("long long" LONG_LONG LANGUAGE CXX)   # -> HAVE_LONG_LONG
  check_type_size("uint16_t"  UINT16_T  LANGUAGE CXX)    # -> HAVE_UINT16_T
  set(CMAKE_EXTRA_INCLUDE_FILES sys/types.h)
  check_type_size("u_int16_t" U_INT16_T LANGUAGE CXX)    # -> HAVE_U_INT16_T
  unset(CMAKE_EXTRA_INCLUDE_FILES)
  check_cxx_symbol_exists(memcpy "cstring" HAVE_MEMCPY)

  configure_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/sparseconfig.h.in
    ${sparsehash_SOURCE_DIR}/src/google/sparsehash/sparseconfig.h
    @ONLY)

  set(SPARSEHASH_INCLUDE_DIR "${sparsehash_SOURCE_DIR}/src"
      CACHE INTERNAL "sparsehash include directory")

  add_library(sparsehash INTERFACE)
  target_include_directories(sparsehash INTERFACE
    $<BUILD_INTERFACE:${SPARSEHASH_INCLUDE_DIR}>
    $<INSTALL_INTERFACE:include>)
  add_library(sparsehash::sparsehash ALIAS sparsehash)

  # Install the google/ headers into the externals
  # prefix so `make install_externals` populates a self-contained include tree.
  install(DIRECTORY ${sparsehash_SOURCE_DIR}/src/google
          DESTINATION ${EXTERNALS_INSTALL_LOCATION}/include
          COMPONENT external_deps EXCLUDE_FROM_ALL)
  set(EXTERNALS_INSTALL_TARGETS ${EXTERNALS_INSTALL_TARGETS} "sparsehash" PARENT_SCOPE)
endif()
