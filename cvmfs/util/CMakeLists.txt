if (BUILD_CVMFS OR BUILD_LIBCVMFS OR BUILD_LIBCVMFS_CACHE OR
    BUILD_SERVER OR
    BUILD_RECEIVER OR
    BUILD_SHRINKWRAP OR
    BUILD_PRELOADER OR
    BUILD_UNITTESTS OR
    BUILD_STRESS_TESTS)

set (LIBCVMFS_UTIL_SOURCES
  util/algorithm.cc
  util/capabilities.cc
  util/concurrency.cc
  util/exception.cc
  util/file_backed_buffer.cc
  util/logging.cc
  util/mmap_file.cc
  util/namespace.cc
  util/posix.cc
  util/raii_temp_dir.cc
  util/string.cc
  util/uuid.cc
)

set (LIBCVMFS_UTIL_CFLAGS "-D_FILE_OFFSET_BITS=64 -DCVMFS_LIBRARY -DCVMFS_RAISE_EXCEPTIONS -fexceptions")
set (LIBCVMFS_UTIL_LINK_LIBRARIES "")
list (APPEND LIBCVMFS_UTIL_LINK_LIBRARIES
      ${UUID_LIBRARIES}
      ${CAP_LIBRARIES}
      ${RT_LIBRARY}
      pthread
      dl)

add_library(cvmfs_fuzzy
    SHARED
    util/fuzzy.cc
    util/fuzzy.h
)

set_target_properties(cvmfs_fuzzy PROPERTIES
    VERSION ${CernVM-FS_VERSION_STRING}
    COMPILE_FLAGS "${LIBCVMFS_UTIL_CFLAGS} -DCVMFS_UTIL_EXPORTS"
    SOVERSION ${CernVM-FS_VERSION_MAJOR}
)

target_include_directories(cvmfs_fuzzy
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<INSTALL_INTERFACE:include>
)

install(TARGETS cvmfs_fuzzy
    EXPORT cvmfsTargets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    PUBLIC_HEADER DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)

add_library(cvmfs_util SHARED ${LIBCVMFS_UTIL_SOURCES})
set_target_properties (cvmfs_util PROPERTIES
    VERSION ${CernVM-FS_VERSION_STRING}
    COMPILE_FLAGS "${LIBCVMFS_UTIL_CFLAGS}")
target_link_libraries (cvmfs_util ${LIBCVMFS_UTIL_LINK_LIBRARIES})

install (TARGETS cvmfs_util LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})


if (BUILD_CVMFS OR BUILD_SERVER_DEBUG OR BUILD_RECEIVER_DEBUG OR
    BUILD_UNITTESTS_DEBUG OR BUILD_LIBCVMFS_CACHE)

  add_library(cvmfs_util_debug SHARED ${LIBCVMFS_UTIL_SOURCES})
  set_target_properties (cvmfs_util_debug PROPERTIES
      VERSION ${CernVM-FS_VERSION_STRING}
      COMPILE_FLAGS "${LIBCVMFS_UTIL_CFLAGS} -DDEBUGMSG -g -O0")
  target_link_libraries (cvmfs_util_debug ${LIBCVMFS_UTIL_LINK_LIBRARIES})
  install (TARGETS cvmfs_util_debug LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})

  add_library(cvmfs_fuzzy_debug
      SHARED
      util/fuzzy.cc
      util/fuzzy.h
  )

  set_target_properties(cvmfs_fuzzy_debug PROPERTIES
      VERSION ${CernVM-FS_VERSION_STRING}
      COMPILE_FLAGS "${LIBCVMFS_UTIL_CFLAGS} -DCVMFS_UTIL_EXPORTS -DDEBUGMSG -g -O0"
  )

  target_include_directories(cvmfs_fuzzy_debug
      PUBLIC
          $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
          $<INSTALL_INTERFACE:include>
  )

  install(TARGETS cvmfs_fuzzy_debug
      EXPORT cvmfsTargets
      LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
      ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
      PUBLIC_HEADER DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
  )
endif () # debug binaries


endif () # components that need libcvmfs-util
