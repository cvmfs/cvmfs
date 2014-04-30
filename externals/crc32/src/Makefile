all:
	gcc -DHAVE_CONFIG_H -I. -I../.. -w -fPIC -O2 -g -fno-strict-aliasing -fasynchronous-unwind-tables -fno-omit-frame-pointer -fno-optimize-sibling-calls -fvisibility=hidden -Wall -D_REENTRANT -D__EXTENSIONS__ -D_LARGEFILE64_SOURCE -D__LARGE64_FILES -MT crc32.o -MD -MP -c -o bulk_crc32.o bulk_crc32.c
	ar cru libcrc32.a bulk_crc32.o
	ranlib libcrc32.a

clean:
	rm -f *.o *.d
	rm -f libcrc32.a
