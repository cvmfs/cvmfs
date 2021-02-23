cvmfs_ducc: cmd/*.go constants/*.go cvmfs/*.go docker-api/*.go exec/*.go lib/*.go log/*.go singularity/*.go temp/*.go notification/*.go vendor/
	go build -o cvmfs_ducc
	sudo setcap "cap_dac_override=ep cap_dac_read_search=ep cap_fowner=ep cap_chown=ep cap_sys_admin=ep cap_mknod=ep" cvmfs_ducc

clean:
	rm cvmfs_ducc

install: cvmfs_ducc
	sudo cp cvmfs_ducc /usr/bin
	sudo setcap "cap_dac_override=ep cap_dac_read_search=ep cap_fowner=ep cap_chown=ep cap_sys_admin=ep cap_mknod=ep" /usr/bin/cvmfs_ducc
