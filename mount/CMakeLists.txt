if (MACOSX)
	install (
		FILES			auto_cvmfs
		DESTINATION		"/etc"
	)

	install (
		FILES			mount.cvmfs
		DESTINATION		"/sbin"
		RENAME			mount_cvmfs
		PERMISSIONS		OWNER_READ OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
	)

	install (
		FILES 			config.sh default.conf
		DESTINATION		"/etc/cvmfs"
		PERMISSIONS		OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
	)

	install (
		FILES			domain.d/cern.ch.conf
		DESTINATION		"/etc/cvmfs/domain.d"
		PERMISSIONS		OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
	)

	install (
		FILES			keys/cern.ch.pub
		DESTINATION		"/etc/cvmfs/keys"
		PERMISSIONS		OWNER_READ GROUP_READ WORLD_READ
	)

  install (
		FILES			keys/cern-it1.cern.ch.pub
		DESTINATION		"/etc/cvmfs/keys"
		PERMISSIONS		OWNER_READ GROUP_READ WORLD_READ
	)

  install (
		FILES			keys/cern-it2.cern.ch.pub
		DESTINATION		"/etc/cvmfs/keys"
		PERMISSIONS		OWNER_READ GROUP_READ WORLD_READ
	)

  install (
		FILES			keys/cern-it3.cern.ch.pub
		DESTINATION		"/etc/cvmfs/keys"
		PERMISSIONS		OWNER_READ GROUP_READ WORLD_READ
	)
else (MACOSX)
	install (
		FILES			auto.cvmfs
		DESTINATION		"/etc"
		PERMISSIONS		OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
	)

	install (
		FILES			mount.cvmfs
		DESTINATION		"/sbin"
		PERMISSIONS		OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
	)

	install (
		FILES 			config.sh default.conf
		DESTINATION		"/etc/cvmfs"
		PERMISSIONS		OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ 
	)

	install (
		FILES			domain.d/cern.ch.conf
		DESTINATION		"/etc/cvmfs/domain.d"
		PERMISSIONS		OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
	)

	install (
		FILES			keys/cern.ch.pub
		DESTINATION		"/etc/cvmfs/keys"
		PERMISSIONS		OWNER_READ GROUP_READ WORLD_READ
	)

  install (
		FILES			keys/cern-it1.cern.ch.pub
		DESTINATION		"/etc/cvmfs/keys"
		PERMISSIONS		OWNER_READ GROUP_READ WORLD_READ
	)

  install (
		FILES			keys/cern-it2.cern.ch.pub
		DESTINATION		"/etc/cvmfs/keys"
		PERMISSIONS		OWNER_READ GROUP_READ WORLD_READ
	)

  install (
		FILES			keys/cern-it3.cern.ch.pub
		DESTINATION		"/etc/cvmfs/keys"
		PERMISSIONS		OWNER_READ GROUP_READ WORLD_READ
	)
endif (MACOSX)
