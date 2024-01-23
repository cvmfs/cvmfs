if (BUILD_CVMFS)

set (LIBCVMFS_BRIDGE_SOURCES
  bridge/ds_stubs.cc
  bridge/marshal.cc
  bridge/migrate.cc
)

set (LIBCVMFS_BRIDGE_CFLAGS "-D_FILE_OFFSET_BITS=64 -DCVMFS_LIBRARY -DCVMFS_RAISE_EXCEPTIONS -fexceptions")
set (LIBCVMFS_BRIDGE_LINK_LIBRARIES "")
list (APPEND LIBCVMFS_BRIDGE_LINK_LIBRARIES
      ${SPARSEHASH_LIBRARIES}
      ${RT_LIBRARY}
      pthread
      dl)

add_library(cvmfs_bridge SHARED ${LIBCVMFS_BRIDGE_SOURCES})
set_target_properties (cvmfs_bridge PROPERTIES
    VERSION ${CernVM-FS_VERSION_STRING}
    COMPILE_FLAGS "${LIBCVMFS_BRIDGE_CFLAGS}")
target_link_libraries (cvmfs_bridge
                       ${LIBCVMFS_BRIDGE_LINK_LIBRARIES})

install (TARGETS cvmfs_bridge LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})

# Debug binaries
add_library(cvmfs_bridge_debug SHARED ${LIBCVMFS_BRIDGE_SOURCES})
set_target_properties (cvmfs_bridge_debug PROPERTIES
    VERSION ${CernVM-FS_VERSION_STRING}
    COMPILE_FLAGS "${LIBCVMFS_BRIDGE_CFLAGS} -DDEBUGMSG -g -O0")
target_link_libraries (cvmfs_bridge_debug
                       ${LIBCVMFS_BRIDGE_LINK_LIBRARIES})
install (TARGETS cvmfs_bridge_debug LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})

endif () # components that need libcvmfs-bridge
