add_custom_target (
	kernelmodules ALL
	COMMAND make
	COMMENT "Building kernel modules..."
	WORKING_DIRECTORY ${KERNEL_MODULE_LOCATION}
)

install (
	CODE "execute_process (COMMAND ${CMAKE_MAKE_PROGRAM} install WORKING_DIRECTORY ${KERNEL_MODULE_LOCATION})"
)