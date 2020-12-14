cvmfs_ducc: main.go go.mod go.sum lib/*.go cmd/*.go docker-api/*.go singularity/*.go
	go build -o cvmfs_ducc
	sudo setcap "cap_dac_override=ep cap_dac_read_search=ep cap_fowner=ep" cvmfs_ducc

clean:
	rm cvmfs_ducc
