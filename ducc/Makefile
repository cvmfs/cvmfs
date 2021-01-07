cvmfs_ducc: cmd/*.go constants/*.go cvmfs/*.go docker-api/*.go exec/*.go filesystem/*.go lib/*.go log/*.go singularity/*.go temp/*.go vendor/
	go build -o cvmfs_ducc
	sudo setcap "cap_dac_override=ep cap_dac_read_search=ep cap_fowner=ep cap_chown=ep" cvmfs_ducc

clean:
	rm cvmfs_ducc
