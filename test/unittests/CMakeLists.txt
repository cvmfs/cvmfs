
#
# global test configuration
#
set (PROJECT_TEST_NAME "cvmfs_unittests")

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
  t_prng.cc
  t_buffer.cc
  t_chunk_detectors.cc
  t_file_processing.cc
  t_async_reader.cc
  t_test_utils.cc
  t_sanitizer.cc
  t_file_sandbox.cc
  t_blocking_counter.cc

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
  ${CVMFS_SOURCE_DIR}/prng.h
  ${CVMFS_SOURCE_DIR}/util.h
  ${CVMFS_SOURCE_DIR}/util.cc
  ${CVMFS_SOURCE_DIR}/hash.h
  ${CVMFS_SOURCE_DIR}/hash.cc
  ${CVMFS_SOURCE_DIR}/shortstring.h
  ${CVMFS_SOURCE_DIR}/sanitizer.h
  ${CVMFS_SOURCE_DIR}/sanitizer.cc

  ${CVMFS_SOURCE_DIR}/catalog_counters.h
  ${CVMFS_SOURCE_DIR}/catalog_counters.cc

  ${CVMFS_SOURCE_DIR}/file_processing/chunk_detector.cc
  ${CVMFS_SOURCE_DIR}/file_processing/file_processor.cc
  ${CVMFS_SOURCE_DIR}/file_processing/io_dispatcher.cc
  ${CVMFS_SOURCE_DIR}/file_processing/processor.cc
  ${CVMFS_SOURCE_DIR}/file_processing/file.cc
  ${CVMFS_SOURCE_DIR}/file_processing/chunk.cc
  ${CVMFS_SOURCE_DIR}/file_processing/async_reader.cc
  ${CVMFS_SOURCE_DIR}/upload_facility.cc
  ${CVMFS_SOURCE_DIR}/upload_local.cc
  ${CVMFS_SOURCE_DIR}/upload_spooler_definition.cc
  ${CVMFS_SOURCE_DIR}/file_chunk.cc
  ${CVMFS_SOURCE_DIR}/compression.cc
)

#
# Compiler and Linker Flags for unit tests
#
set (CVMFS_UNITTESTS_CFLAGS "${CVMFS_UNITTESTS_CFLAGS}")
set (CVMFS_UNITTESTS_LD_FLAGS "${CVMFS_UNITTESTS_LD_FLAGS}")

#
# build CernVM-FS test cases
#
include_directories (${INCLUDE_DIRECTORIES})
add_executable (${PROJECT_TEST_NAME} ${CVMFS_UNITTEST_SOURCES})
add_dependencies (${PROJECT_TEST_NAME} googletest)

if (SQLITE3_BUILTIN)
  add_dependencies (${PROJECT_TEST_NAME} sqlite3)
endif (SQLITE3_BUILTIN)

if (TBB_PRIVATE_LIB)
  add_dependencies (${PROJECT_TEST_NAME} libtbb)
endif (TBB_PRIVATE_LIB)

set_target_properties (${PROJECT_TEST_NAME} PROPERTIES COMPILE_FLAGS "${CVMFS_UNITTESTS_CFLAGS}" LINK_FLAGS "${CVMFS_UNITTESTS_LD_FLAGS}")

# link the stuff (*_LIBRARIES are dynamic link libraries)
target_link_libraries (${PROJECT_TEST_NAME} ${GTEST_LIBRARIES} ${GOOGLETEST_ARCHIVE} ${OPENSSL_LIBRARIES}
                       ${SQLITE3_LIBRARY} ${SQLITE3_ARCHIVE} ${TBB_LIBRARIES} ${ZLIB_LIBRARIES} ${ZLIB_ARCHIVE} pthread)

#
# Install the generated unit test binary
#
if (INSTALL_UNITTESTS)
  install (
    TARGETS        ${PROJECT_TEST_NAME}
    RUNTIME
    DESTINATION    bin
  )
endif (INSTALL_UNITTESTS)

#
# Integrate the test running into CMake
#
add_test (NAME unittests COMMAND ${PROJECT_TEST_NAME})
