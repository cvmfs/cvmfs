all:
	g++ -I. -I../.. -g -O2 -Wall -c -o murmur.o MurmurHash2.cpp
	ar cru libmurmur.a murmur.o
	ranlib libmurmur.a

clean:
	rm -f *.o
	rm -f libmurmur.a

