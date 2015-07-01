all: libsha2.a

libsha2.a: sha2.o
	ar cru libsha2.a sha2.o
	ranlib libsha2.a

sha2: sha2.o
	$(CC) $< -o $@

sha2.o: sha2.c sha2.h
	$(CC) -I. -g -O2 -fPIC -Wall -c $<

clean:
	rm -f *.o sha2 libsha2.a

