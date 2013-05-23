
#
# unit test source files
#
set (CVMFS_UNITTEST_SOURCES
  main.cc

  atomic.cc
)

#
# build googletest framework
#
include (${GOOGLETEST_BUILTIN_LOCATION}/CVMFS-CMakeLists.txt)
set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${GOOGLETEST_BUILTIN_LOCATION}/include)

#
# Compiler and Linker Flags for unit tests
#
set (CVMFS_UNITTESTS_CFLAGS "${CVMFS_UNITTESTS_CFLAGS} -I ${CMAKE_SOURCE_DIR}/cvmfs")
set (CVMFS_UNITTESTS_LD_FLAGS "${CVMFS_UNITTESTS_LD_FLAGS}")

#
# build CernVM-FS test cases
#
add_executable (cvmfs_unittests ${CVMFS_UNITTEST_SOURCES})
add_dependencies (cvmfs_unittests googletest)

set_target_properties (cvmfs_unittests PROPERTIES COMPILE_FLAGS "${CVMFS_UNITTESTS_CFLAGS}" LINK_FLAGS "${CVMFS_UNITTESTS_LD_FLAGS}")

# link the stuff (*_LIBRARIES are dynamic link libraries)
target_link_libraries (cvmfs_unittests ${GOOGLETEST_ARCHIVE} pthread)
