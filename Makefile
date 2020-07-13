PREFIX ?= out/

all: cvmfs-snapshotter

cvmfs-snapshotter: 
	go build -o $(PREFIX)$@
