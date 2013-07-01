
#
# global test configuration
#
set (PROJECT_TEST_NAME "${PROJECT_NAME}_test")

#
# unit test source files
#
set (CVMFS_SOURCE_DIR "${CMAKE_SOURCE_DIR}/cvmfs")
set (CVMFS_UNITTEST_SOURCES
  # test steering
  main.cc

  # unit test files
  t_atomic.cc
  t_smallhash.cc
  t_bigvector.cc
  t_util_concurrency.cc 
  t_catalog_counters.cc
  t_fs_traversal.cc
  t_pipe.cc
  t_managed_exec.cc
  t_test_utils.cc

  # test utility functions
  testutil.cc testutil.h

  # test dependencies
  ${CVMFS_SOURCE_DIR}/atomic.h
  ${CVMFS_SOURCE_DIR}/logging.h
  ${CVMFS_SOURCE_DIR}/logging.cc
  ${CVMFS_SOURCE_DIR}/murmur.h
  ${CVMFS_SOURCE_DIR}/smallhash.h
  ${CVMFS_SOURCE_DIR}/bigvector.h
  ${CVMFS_SOURCE_DIR}/smalloc.h
  ${CVMFS_SOURCE_DIR}/util_concurrency.h
  ${CVMFS_SOURCE_DIR}/util_concurrency.cc
  ${CVMFS_SOURCE_DIR}/util_concurrency_impl.h

  ${CVMFS_SOURCE_DIR}/platform.h
  ${CVMFS_SOURCE_DIR}/platform_linux.h
  ${CVMFS_SOURCE_DIR}/platform_osx.h
  ${CVMFS_SOURCE_DIR}/util.h
  ${CVMFS_SOURCE_DIR}/util.cc
  ${CVMFS_SOURCE_DIR}/hash.h
  ${CVMFS_SOURCE_DIR}/hash.cc
  ${CVMFS_SOURCE_DIR}/shortstring.h

  ${CVMFS_SOURCE_DIR}/catalog_counters.h
  ${CVMFS_SOURCE_DIR}/catalog_counters.cc
)

#
# build googletest framework
#
include (${GOOGLETEST_BUILTIN_LOCATION}/CVMFS-CMakeLists.txt)
set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${GOOGLETEST_BUILTIN_LOCATION}/include)

#
# find external libraries
#
find_package (OpenSSL REQUIRED)
set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${OPENSSL_INCLUDE_DIR})

#
# Compiler and Linker Flags for unit tests
#
set (CVMFS_UNITTESTS_CFLAGS "${CVMFS_UNITTESTS_CFLAGS}")
set (CVMFS_UNITTESTS_LD_FLAGS "${CVMFS_UNITTESTS_LD_FLAGS}")

#
# build CernVM-FS test cases
#
add_executable (${PROJECT_TEST_NAME} ${CVMFS_UNITTEST_SOURCES})
add_dependencies (${PROJECT_TEST_NAME} googletest)

if (SQLITE3_BUILTIN)
  add_dependencies (${PROJECT_TEST_NAME} sqlite3)
endif (SQLITE3_BUILTIN)

set_target_properties (${PROJECT_TEST_NAME} PROPERTIES COMPILE_FLAGS "${CVMFS_UNITTESTS_CFLAGS}" LINK_FLAGS "${CVMFS_UNITTESTS_LD_FLAGS}")

# link the stuff (*_LIBRARIES are dynamic link libraries)
target_link_libraries (${PROJECT_TEST_NAME} ${GOOGLETEST_ARCHIVE} ${OPENSSL_LIBRARIES} 
                       ${SQLITE3_LIBRARY} ${SQLITE3_ARCHIVE} pthread)

#
# Integrate the test running into CMake
#
add_test (NAME unittests COMMAND ${PROJECT_TEST_NAME})
