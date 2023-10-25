PREFIX ?= out/
VERSION ?= 2.11

all: cvmfs_snapshotter

cvmfs_snapshotter:         
	go build -o $(PREFIX)$@ -ldflags '-X main.Version=$(VERSION)'

release: cvmfs_snapshotter
	mkdir -p release/usr/local/bin
	cp -r out/* release/usr/local/bin
	cp -r script/config/* release/
