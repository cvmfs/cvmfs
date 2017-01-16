all: json.cpp block_allocator.cpp
	g++ $(CVMFS_BASE_CXX_FLAGS) -I. -fPIC -c -o json.o json.cpp
	g++ $(CVMFS_BASE_CXX_FLAGS) -I. -fPIC -c -o block_allocator.o block_allocator.cpp
	ar cru libvjson.a json.o block_allocator.o
	ranlib libvjson.a

clean:
	rm -f *.o
	rm -f libvjson.a

