all: memcached_test

memcached_test: memcached_test.cc memcached.cc memcached.h smalloc.c smalloc.h debug.c debug.h
	gcc -Wall -c -o debug.o debug.c
	gcc -Wall -c -o smalloc.o smalloc.c
	gcc -Wall -c -o md5.o md5.c
	gcc -Wall -c -o sha1.o sha1.c
	gcc -Wall -c -o compression.o compression.c
	g++ -Wall -I../../ -c -o hash.o hash.cc
	g++ -Wall -I../../ -c -o util.o util.cc
	g++ -Wall -I../../ -c -o memcached.o memcached.cc
	g++ -Wall -c -o memcached_test.o memcached_test.cc
	g++ -Wall -lpthread -lz -o memcached_test memcached_test.o memcached.o util.o hash.o compression.o md5.o sha1.o smalloc.o debug.o

clean:
	rm -f memcached.o debug.o memcached_test.o
