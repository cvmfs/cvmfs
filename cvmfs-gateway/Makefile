
build:
	go build -o cvmfs_gateway

test: build
	go test ./...

install: build
	cp cvmfs_gateway /usr/bin/cvmfs_gateway
