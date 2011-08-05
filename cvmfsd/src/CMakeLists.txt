if (BUILD_SERVER)
	set (CVMFSD_CTRL_SOURCES cvmfsd_ctrl.c)
	
	set (CVMFSD_CTRL_CFLAGS)
	set (CVMFSD_CTRL_LD_FLAGS)
	
	add_executable (cvmfsd_ctrl ${CVMFSD_CTRL_SOURCES})
	set_target_properties (cvmfsd_ctrl PROPERTIES COMPILE_FLAGS "${CVMFSD_CTRL_CFLAGS}" LINK_FLAGS "${CVMFSD_CTRL_LD_FLAGS}")
	
	install (
		TARGETS			cvmfsd_ctrl
		RUNTIME
		DESTINATION		bin
	)
endif (BUILD_SERVER)