if (BUILD_SERVER)
        install (
                FILES                   cvmfs_snapshot
                DESTINATION             bin
                PERMISSIONS             OWNER_READ OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
        )
	install (
		FILES			etc.cvmfs.replica.conf
		DESTINATION		"/etc/cvmfs"
		RENAME			replica.conf
		PERMISSIONS		OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ	
	)
endif (BUILD_SERVER)
