add_custom_target (
	kernelmodules ALL
	COMMAND make
	COMMENT "Building kernel modules..."
)

install (
	CODE "execute_process (COMMAND make install)"
)