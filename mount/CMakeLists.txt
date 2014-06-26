set (CVMFS_MOUNT_SOURCES
  ../cvmfs/smalloc.h
  ../cvmfs/platform.h ../cvmfs/platform_linux.h ../cvmfs/platform_osx.h
  ../cvmfs/util.h ../cvmfs/util.cc
  ../cvmfs/logging.h ../cvmfs/logging_internal.h ../cvmfs/logging.cc
  ../cvmfs/options.h ../cvmfs/options.cc
  ../cvmfs/sanitizer.h ../cvmfs/sanitizer.cc
  mount.cvmfs.cc
)

if (MACOSX)
  set (MOUNT_TARGET_NAME "mount_cvmfs")
else (MACOSX)
  set (MOUNT_TARGET_NAME "mount.cvmfs")
endif (MACOSX)

add_executable (${MOUNT_TARGET_NAME} ${CVMFS_MOUNT_SOURCES})
target_link_libraries (${MOUNT_TARGET_NAME} pthread)
set (CVMFS_MOUNT_CFLAGS "${CVMFS_MOUNT_CFLAGS} -I${CMAKE_SOURCE_DIR}/cvmfs")
set_target_properties (${MOUNT_TARGET_NAME} PROPERTIES COMPILE_FLAGS "${CVMFS_MOUNT_CFLAGS}")

if (MACOSX)
  install (
    FILES         auto_cvmfs
    DESTINATION   "/etc"
  )

  install (
    TARGETS       ${MOUNT_TARGET_NAME}
    DESTINATION   "/sbin"
    PERMISSIONS   OWNER_READ OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
  )

  install (
    FILES         config.sh default.conf
    DESTINATION   "/etc/cvmfs"
    PERMISSIONS   OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
  )

  install (
    FILES         default.d/README
    DESTINATION   "/etc/cvmfs/default.d"
    PERMISSIONS   OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
  )

else (MACOSX)
  set (CVMFS_AUTO_SOURCES
    auto.cvmfs.cc
  )
  add_executable (auto.cvmfs ${CVMFS_AUTO_SOURCES})

  install (
    TARGETS       auto.cvmfs
    DESTINATION   "/etc"
    PERMISSIONS   OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
  )

  install (
    TARGETS       ${MOUNT_TARGET_NAME}
    DESTINATION   "/sbin"
    PERMISSIONS   OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
  )

  install (
    FILES         config.sh default.conf
    DESTINATION   "/etc/cvmfs"
    PERMISSIONS   OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
  )

  install (
    FILES         default.d/README
    DESTINATION   "/etc/cvmfs/default.d"
    PERMISSIONS   OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
  )

endif (MACOSX)
