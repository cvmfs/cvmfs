set (CVMFS_SOURCE_DIR "${CMAKE_SOURCE_DIR}/cvmfs")
set (CVMFS_MOUNT_SOURCES
  ${CVMFS_SOURCE_DIR}/options.cc
  ${CVMFS_SOURCE_DIR}/sanitizer.cc
  ${CVMFS_SOURCE_DIR}/util/exception.cc
  ${CVMFS_SOURCE_DIR}/util/logging.cc
  ${CVMFS_SOURCE_DIR}/util/posix.cc
  ${CVMFS_SOURCE_DIR}/util/string.cc
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
    DESTINATION   "${SYSCONF_INSTALL_DIR}"
  )

  install (
    TARGETS       ${MOUNT_TARGET_NAME}
    DESTINATION   ${CMAKE_MOUNT_INSTALL_BINDIR}
    PERMISSIONS   OWNER_READ OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
  )

  install (
    FILES         config.sh default.conf
    DESTINATION   "${SYSCONF_INSTALL_DIR}/cvmfs"
    PERMISSIONS   OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
  )

  install (
    FILES         default.d/10-osx.conf
    DESTINATION   "${SYSCONF_INSTALL_DIR}/cvmfs/default.d"
    PERMISSIONS   OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
  )

  install (
    FILES         default.d/50-cern.conf
    DESTINATION   "${SYSCONF_INSTALL_DIR}/cvmfs/default.d"
    PERMISSIONS   OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
  )

  install (
    FILES         default.d/README
    DESTINATION   "${SYSCONF_INSTALL_DIR}/cvmfs/default.d"
    PERMISSIONS   OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
  )

  install (
    FILES         domain.d/cern.ch.conf
    DESTINATION   "${SYSCONF_INSTALL_DIR}/cvmfs/domain.d"
    PERMISSIONS   OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
  )

  install (
    FILES         domain.d/egi.eu.conf
    DESTINATION   "${SYSCONF_INSTALL_DIR}/cvmfs/domain.d"
    PERMISSIONS   OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
  )

  install (
    FILES         domain.d/opensciencegrid.org.conf
    DESTINATION   "${SYSCONF_INSTALL_DIR}/cvmfs/domain.d"
    PERMISSIONS   OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
  )

  install (
    FILES         config.d/grid.cern.ch.conf
    DESTINATION   "${SYSCONF_INSTALL_DIR}/cvmfs/config.d"
    PERMISSIONS   OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
  )

  install (
    FILES         config.d/README
    DESTINATION   "${SYSCONF_INSTALL_DIR}/cvmfs/config.d"
    PERMISSIONS   OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
  )

  install (
    FILES         config.d/cvmfs-config.cern.ch.conf
    DESTINATION   "${SYSCONF_INSTALL_DIR}/cvmfs/config.d"
    PERMISSIONS   OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
  )

  if (INSTALL_PUBLIC_KEYS)
    install (
      FILES         keys/cern-it1.cern.ch.pub
      DESTINATION   "${SYSCONF_INSTALL_DIR}/cvmfs/keys/cern.ch"
      PERMISSIONS   OWNER_READ GROUP_READ WORLD_READ
    )

    install (
      FILES         keys/cern-it4.cern.ch.pub
      DESTINATION   "${SYSCONF_INSTALL_DIR}/cvmfs/keys/cern.ch"
      PERMISSIONS   OWNER_READ GROUP_READ WORLD_READ
    )

    install (
      FILES         keys/cern-it5.cern.ch.pub
      DESTINATION   "${SYSCONF_INSTALL_DIR}/cvmfs/keys/cern.ch"
      PERMISSIONS   OWNER_READ GROUP_READ WORLD_READ
    )

    install (
      FILES         keys/egi.eu.pub
      DESTINATION   "${SYSCONF_INSTALL_DIR}/cvmfs/keys/egi.eu"
      PERMISSIONS   OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
    )

    install (
      FILES         keys/opensciencegrid.org.pub
      DESTINATION   "${SYSCONF_INSTALL_DIR}/cvmfs/keys/opensciencegrid.org"
      PERMISSIONS   OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
    )
  endif (INSTALL_PUBLIC_KEYS)

  install (
    FILES         serverorder.sh
    DESTINATION   "${SYSCONF_INSTALL_DIR}/cvmfs"
    PERMISSIONS   OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
  )

else (MACOSX)
  set (CVMFS_AUTO_SOURCES
    auto.cvmfs.cc
  )
  add_executable (auto.cvmfs ${CVMFS_AUTO_SOURCES})

  install (
    TARGETS       auto.cvmfs
    DESTINATION   ${CVMFS_LIBEXEC_DIR}
    PERMISSIONS   OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
  )

  if (ARCHLINUX)
    SET( autofsmap_link    /etc/autofs/auto.cvmfs)
  else (ARCHLINUX)
    SET( autofsmap_link    /etc/auto.cvmfs)
  endif (ARCHLINUX)
  SET( autofsmap_dest    ${CMAKE_INSTALL_PREFIX}/${CVMFS_LIBEXEC_DIR})
  SET( autofsmap_target  ${autofsmap_dest}/auto.cvmfs)
  # Symlink /etc/(autofs)/auto.cvmfs --> /usr/lib(exec)/cvmfs/auto.cvmfs
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
    DESTINATION   ${CMAKE_MOUNT_INSTALL_BINDIR}
    PERMISSIONS   OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
  )

  install (
    FILES         config.sh default.conf
    DESTINATION   "${SYSCONF_INSTALL_DIR}/cvmfs"
    PERMISSIONS   OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
  )

  install (
    FILES         default.d/README
    DESTINATION   "${SYSCONF_INSTALL_DIR}/cvmfs/default.d"
    PERMISSIONS   OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
  )

  if (ARCHLINUX)
    install (
      FILES         default.d/10-arch.conf
      DESTINATION   "${SYSCONF_INSTALL_DIR}/cvmfs/default.d"
      PERMISSIONS   OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
    )
  endif (ARCHLINUX)

  if (DEBIAN)
    install (
      FILES         default.d/50-cern-debian.conf
      DESTINATION   "${SYSCONF_INSTALL_DIR}/cvmfs/default.d"
      PERMISSIONS   OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
    )
  else (DEBIAN)
    install (
      FILES         default.d/50-cern.conf
      DESTINATION   "${SYSCONF_INSTALL_DIR}/cvmfs/default.d"
      PERMISSIONS   OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
    )
  endif (DEBIAN)

  install (
    FILES         domain.d/cern.ch.conf
    DESTINATION   "${SYSCONF_INSTALL_DIR}/cvmfs/domain.d"
    PERMISSIONS   OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
  )

  install (
    FILES         domain.d/egi.eu.conf
    DESTINATION   "${SYSCONF_INSTALL_DIR}/cvmfs/domain.d"
    PERMISSIONS   OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
  )

  install (
    FILES         domain.d/opensciencegrid.org.conf
    DESTINATION   "${SYSCONF_INSTALL_DIR}/cvmfs/domain.d"
    PERMISSIONS   OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
  )

  install (
    FILES         config.d/README
    DESTINATION   "${SYSCONF_INSTALL_DIR}/cvmfs/config.d"
    PERMISSIONS   OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
  )

  install (
    FILES         config.d/cvmfs-config.cern.ch.conf
    DESTINATION   "${SYSCONF_INSTALL_DIR}/cvmfs/config.d"
    PERMISSIONS   OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
  )

  if (INSTALL_PUBLIC_KEYS)
    install (
      FILES         keys/cern-it1.cern.ch.pub
      DESTINATION   "${SYSCONF_INSTALL_DIR}/cvmfs/keys/cern.ch"
      PERMISSIONS   OWNER_READ GROUP_READ WORLD_READ
    )

    install (
      FILES         keys/cern-it4.cern.ch.pub
      DESTINATION   "${SYSCONF_INSTALL_DIR}/cvmfs/keys/cern.ch"
      PERMISSIONS   OWNER_READ GROUP_READ WORLD_READ
    )

    install (
      FILES         keys/cern-it5.cern.ch.pub
      DESTINATION   "${SYSCONF_INSTALL_DIR}/cvmfs/keys/cern.ch"
      PERMISSIONS   OWNER_READ GROUP_READ WORLD_READ
    )

    install (
      FILES         keys/egi.eu.pub
      DESTINATION   "${SYSCONF_INSTALL_DIR}/cvmfs/keys/egi.eu"
      PERMISSIONS   OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
    )

    install (
      FILES         keys/opensciencegrid.org.pub
      DESTINATION   "${SYSCONF_INSTALL_DIR}/cvmfs/keys/opensciencegrid.org"
      PERMISSIONS   OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
    )
  endif (INSTALL_PUBLIC_KEYS)

  install (
    FILES         serverorder.sh
    DESTINATION   "${SYSCONF_INSTALL_DIR}/cvmfs"
    PERMISSIONS   OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
  )
endif (MACOSX)
