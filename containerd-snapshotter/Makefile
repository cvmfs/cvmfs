PREFIX ?= out/

all: cvmfs-snapshotter

cvmfs-snapshotter: 
	go build -o $(PREFIX)$@


release: cvmfs-snapshotter
	mkdir -p release/usr/local/bin
	cp -r out/* release/usr/local/bin
	cp -r script/config/* release/
