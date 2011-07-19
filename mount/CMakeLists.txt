if (MACOSX)
	# some work has to be done...
else (MACOSX)
	install (
		FILES			auto.cvmfs
		DESTINATION		"${CMAKE_INSTALL_PREFIX}/etc"
	)
	
	install (
		FILES			mount.cvmfs umount.cvmfs
		DESTINATION		"${CMAKE_INSTALL_PREFIX}/sbin"
	)
	
	install (
		FILES			cvmfs.initd
		DESTINATION		"${CMAKE_INSTALL_PREFIX}/etc/init.d"
		RENAME			cvmfs
	)
	
	install (
		FILES 			config.sh default.sh
		DESTINATION		"${CMAKE_INSTALL_PREFIX}/etc/cvmfs"
		PERMISSIONS		0644
	)
	
	install (
		FILES			domain.d/cern.ch.conf
		DESTINATION		"${CMAKE_INSTALL_PREFIX}/etc/cvmfs/domain.d"
		PERMISSIONS		0644
	)
	
	install (
		FILES			keys/cern.ch.pub
		DESTINATION		"${CMAKE_INSTALL_PREFIX}/etc/cvmfs/keys"
		PERMISSIONS		0444
	)
endif (MACOSX)