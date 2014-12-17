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
    FILES         default.d/50-cern.conf
    DESTINATION   "/etc/cvmfs/default.d"
    PERMISSIONS   OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
  )

  install (
    FILES         default.d/README
    DESTINATION   "/etc/cvmfs/default.d"
    PERMISSIONS   OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
  )

  install (
    FILES         domain.d/cern.ch.conf
    DESTINATION   "/etc/cvmfs/domain.d"
    PERMISSIONS   OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
  )

  if (INSTALL_CERN_KEYS)
    install (
      FILES         keys/cern.ch.pub
      DESTINATION   "/etc/cvmfs/keys/cern.ch"
      PERMISSIONS   OWNER_READ GROUP_READ WORLD_READ
    )

    install (
      FILES         keys/cern-it1.cern.ch.pub
      DESTINATION   "/etc/cvmfs/keys/cern.ch"
      PERMISSIONS   OWNER_READ GROUP_READ WORLD_READ
    )

    install (
      FILES         keys/cern-it2.cern.ch.pub
      DESTINATION   "/etc/cvmfs/keys/cern.ch"
      PERMISSIONS   OWNER_READ GROUP_READ WORLD_READ
    )

    install (
      FILES         keys/cern-it3.cern.ch.pub
      DESTINATION   "/etc/cvmfs/keys/cern.ch"
      PERMISSIONS   OWNER_READ GROUP_READ WORLD_READ
    )
  endif (INSTALL_CERN_KEYS)

  install (
    FILES         serverorder.sh
    DESTINATION   "/etc/cvmfs"
    PERMISSIONS   OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
  )

  install (
    FILES         domain.d/egi.eu.conf
    DESTINATION   "/etc/cvmfs/domain.d"
    PERMISSIONS   OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
  )

  install (
    FILES         domain.d/opensciencegrid.org.conf
    DESTINATION   "/etc/cvmfs/domain.d"
    PERMISSIONS   OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
  )

  install (
    FILES         keys/egi.eu.pub
    DESTINATION   "/etc/cvmfs/keys/egi.eu"
    PERMISSIONS   OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
  )

  install (
    FILES         keys/opensciencegrid.org.pub
    DESTINATION   "/etc/cvmfs/keys/opensciencegrid.org"
    PERMISSIONS   OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
  )

else (MACOSX)
  set (CVMFS_AUTO_SOURCES
    auto.cvmfs.cc
  )
  add_executable (auto.cvmfs ${CVMFS_AUTO_SOURCES})

  SET( autofsmap_link    /etc/auto.cvmfs)
  SET( autofsmap_dest    /usr/libexec/cvmfs)
  SET( autofsmap_target  ${autofsmap_dest}/auto.cvmfs)
  install (
    TARGETS       auto.cvmfs
    DESTINATION   ${autofsmap_dest}
    PERMISSIONS   OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
  )
  # Symlink /etc/auto.cvmfs --> /sbin/auto.cvmfs
  install(CODE "
    if (\"\$ENV{DESTDIR}\" STREQUAL \"\")
      execute_process(COMMAND ln -sf
        ${autofsmap_target}
        ${autofsmap_link})
    else ()
      execute_process(COMMAND ln -sf
        ${autofsmap_target}
        \$ENV{DESTDIR}/${autofsmap_link})
    endif ()
  ")

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
    FILES         default.d/50-cern.conf
    DESTINATION   "/etc/cvmfs/default.d"
    PERMISSIONS   OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
  )

  install (
    FILES         domain.d/cern.ch.conf
    DESTINATION   "/etc/cvmfs/domain.d"
    PERMISSIONS   OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
  )

  install (
    FILES         default.d/README
    DESTINATION   "/etc/cvmfs/default.d"
    PERMISSIONS   OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
  )

  if (INSTALL_CERN_KEYS)
    install (
      FILES         keys/cern.ch.pub
      DESTINATION   "/etc/cvmfs/keys/cern.ch"
      PERMISSIONS   OWNER_READ GROUP_READ WORLD_READ
    )

    install (
      FILES         keys/cern-it1.cern.ch.pub
      DESTINATION   "/etc/cvmfs/keys/cern.ch"
      PERMISSIONS   OWNER_READ GROUP_READ WORLD_READ
    )

    install (
      FILES         keys/cern-it2.cern.ch.pub
      DESTINATION   "/etc/cvmfs/keys/cern.ch"
      PERMISSIONS   OWNER_READ GROUP_READ WORLD_READ
    )

    install (
      FILES         keys/cern-it3.cern.ch.pub
      DESTINATION   "/etc/cvmfs/keys/cern.ch"
      PERMISSIONS   OWNER_READ GROUP_READ WORLD_READ
    )
  endif (INSTALL_CERN_KEYS)
endif (MACOSX)
