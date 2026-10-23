# CMake module for vendoring the Google Benchmark library
include(FetchContent)


# -> googlebench <-
# Google Benchmark is a development-only dependency, used by the micro
# benchmarks (test/micro-benchmarks).
#

set(GOOGLEBENCH_VERSION "1.5.6")
set(GOOGLEBENCH_URL "https://github.com/google/benchmark/archive/refs/tags/v${GOOGLEBENCH_VERSION}.tar.gz")
set(GOOGLEBENCH_URL_MIRROR "https://ecsft.cern.ch/dist/cvmfs/build_externals/benchmark-${GOOGLEBENCH_VERSION}.tar.gz")
set(GOOGLEBENCH_HASH "MD5=668b9e10d8b0795e5d461894db18db3c")


setup_external_build_mode(googlebench)

# benchmark's CMakeLists would otherwise build its own unit tests
set(BENCHMARK_ENABLE_TESTING     OFF CACHE BOOL "" FORCE)
set(BENCHMARK_ENABLE_GTEST_TESTS OFF CACHE BOOL "" FORCE)
set(BENCHMARK_ENABLE_INSTALL     OFF CACHE BOOL "" FORCE)

FetchContent_Declare(
  benchmark
  URL "${GOOGLEBENCH_URL}" "${GOOGLEBENCH_URL_MIRROR}"
  URL_HASH "${GOOGLEBENCH_HASH}"
  DOWNLOAD_EXTRACT_TIMESTAMP TRUE
  DOWNLOAD_DIR ${EXTERNALS_DOWNLOAD_LOCATION}
  INSTALL_DIR ${EXTERNALS_INSTALL_LOCATION}
  EXCLUDE_FROM_ALL
  FIND_PACKAGE_ARGS CONFIG GLOBAL
)

FetchContent_MakeAvailable(benchmark)
unset(FETCHCONTENT_TRY_FIND_PACKAGE_MODE)

if(benchmark_FOUND)
  message(STATUS "Found system google benchmark (benchmark::benchmark).")
else()
  message(STATUS "Building google benchmark from vendored sources.")

  # cvmfs is compiled with -fno-exceptions globally but google benchmark needs exceptions
  #  Re-enable them for the vendored
  target_compile_options(benchmark PRIVATE -fexceptions)

  install(TARGETS benchmark DESTINATION ${EXTERNALS_INSTALL_LOCATION}/lib COMPONENT external_deps EXCLUDE_FROM_ALL)
  install(DIRECTORY ${benchmark_SOURCE_DIR}/include/benchmark DESTINATION ${EXTERNALS_INSTALL_LOCATION}/include COMPONENT external_deps EXCLUDE_FROM_ALL)
  set(EXTERNALS_INSTALL_TARGETS ${EXTERNALS_INSTALL_TARGETS} "benchmark" PARENT_SCOPE)
endif()
