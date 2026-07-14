# Provisions the GoogleTest targets (GTest::gtest / GTest::gmock).
#
# These are needed both by the unit tests (test/unittests) and by the micro
# benchmarks (test/micro-benchmarks), whose shared test helpers in test/common
# include <gtest/gtest.h>. Keeping the acquisition here -- rather than inside
# test/unittests -- means the micro benchmarks can be built with BUILD_UBENCHMARKS
# alone, without also enabling BUILD_UNITTESTS.
#
# Idempotent: does nothing if the GTest targets already exist, so it is safe to
# call from several places in the same configure run.

if (NOT TARGET GTest::gtest)
  set(GOOGLETEST_MIN_VERSION "1.10.0")
  if(USE_EXTERNAL_GOOGLETEST)
    if (USE_EXTERNAL_GOOGLETEST STREQUAL AUTO)
      find_package(GTest ${GOOGLETEST_MIN_VERSION})
    else()
      find_package(GTest ${GOOGLETEST_MIN_VERSION} REQUIRED)
    endif()
  endif()

  if(NOT GTest_FOUND)
    message(STATUS "Fetching local copy of Googletest library v1.17.0 for unit-tests...")
    Include(FetchContent)
    set(INSTALL_GTEST OFF)

    # Check if local file exists first. Prefer the shared FetchContent cache,
    # but also accept the historic location directly under externals/.
    set(GOOGLETEST_LOCAL_PATH "${PROJECT_SOURCE_DIR}/externals/download/googletest-1.17.0.tar.gz")
    if(NOT EXISTS "${GOOGLETEST_LOCAL_PATH}")
      set(GOOGLETEST_LOCAL_PATH "${PROJECT_SOURCE_DIR}/externals/googletest-1.17.0.tar.gz")
    endif()

    if(EXISTS "${GOOGLETEST_LOCAL_PATH}")
        message(STATUS "Using local GoogleTest archive: ${GOOGLETEST_LOCAL_PATH}")
        FetchContent_Declare(
            GoogleTest
            #DOWNLOAD_EXTRACT_TIMESTAMP TRUE
            DOWNLOAD_DIR ${PROJECT_SOURCE_DIR}/externals/download
            URL "file://${GOOGLETEST_LOCAL_PATH}"
            URL_HASH MD5=b6f100bc2a5853a48046aa168ececf84
        )
    else()
        message(STATUS "Local GoogleTest archive not found, downloading from remote")
        FetchContent_Declare(
            GoogleTest
            #DOWNLOAD_EXTRACT_TIMESTAMP TRUE
            DOWNLOAD_DIR ${PROJECT_SOURCE_DIR}/externals/download
            URL https://github.com/google/googletest/releases/download/v1.17.0/googletest-1.17.0.tar.gz
                https://ecsft.cern.ch/dist/cvmfs/build_externals/googletest-1.17.0.tar.gz
            URL_HASH MD5=b6f100bc2a5853a48046aa168ececf84
        )
    endif()

    FetchContent_MakeAvailable(GoogleTest)
  endif()
endif()
