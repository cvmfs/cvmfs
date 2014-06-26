# only install on Mac; Linux distros use separate cvmfs-keys package
if (MACOSX)
  install (
    FILES         serverorder.sh
    DESTINATION   "/etc/cvmfs"
    PERMISSIONS   OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
  )

  install (
    FILES         domain.d/egi.eu.conf domain.d/cern.ch.conf
    DESTINATION   "/etc/cvmfs/domain.d"
    PERMISSIONS   OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
  )

  install (
    FILES         domain.d/opensciencegrid.org.conf
    DESTINATION   "/etc/cvmfs/domain.d"
    PERMISSIONS   OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
  )

  install (
    FILES         keys/egi.eu.pub keys/cern.ch.pub
    DESTINATION   "/etc/cvmfs/keys"
    PERMISSIONS   OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
  )

  install (
    FILES         keys/egi.eu.pub keys/cern.ch.pub
    DESTINATION   "/etc/cvmfs/keys"
    PERMISSIONS   OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
  )

  install (
    FILES         keys/cern-it1.cern.ch.pub
    DESTINATION   "/etc/cvmfs/keys"
    PERMISSIONS   OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
  )

  install (
    FILES         keys/cern-it2.cern.ch.pub
    DESTINATION   "/etc/cvmfs/keys"
    PERMISSIONS   OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
  )

  install (
    FILES         keys/cern-it3.cern.ch.pub
    DESTINATION   "/etc/cvmfs/keys"
    PERMISSIONS   OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
  )

  install (
    FILES         keys/opensciencegrid.org.pub
    DESTINATION   "/etc/cvmfs/keys"
    PERMISSIONS   OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
  )

endif (MACOSX)
