cvmfs_ducc: cmd/*.go config/*.go cvmfs/*.go daemon/*.go db/*.go registry/*.go util/*.go rest/*.go webhook/*.go vendor/ unpacker/*.go
	go build -o cvmfs_ducc
	sudo setcap "cap_dac_override=ep cap_dac_read_search=ep cap_fowner=ep cap_chown=ep cap_sys_admin=ep cap_mknod=ep" cvmfs_ducc

clean:
	rm cvmfs_ducc

install: cvmfs_ducc
	sudo cp cvmfs_ducc /usr/bin
	sudo cp unitfiles/cvmfs_ducc.service.in /usr/local/lib/systemd/system/cvmfs_ducc.service
	sudo setcap "cap_dac_override=ep cap_dac_read_search=ep cap_fowner=ep cap_chown=ep cap_sys_admin=ep cap_mknod=ep" /usr/bin/cvmfs_ducc
