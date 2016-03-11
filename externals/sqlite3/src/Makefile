all:
	gcc -DHAVE_CONFIG_H -I. -I../.. -w -fPIC -DSQLITE_THREADSAFE=2 -DHAVE_MALLOC_USABLE_SIZE -DSQLITE_ENABLE_MEMORY_MANAGEMENT -DSQLITE_ENABLE_MEMSYS5 -g -O2 -g -fno-strict-aliasing -fasynchronous-unwind-tables -fno-omit-frame-pointer -fno-optimize-sibling-calls -fvisibility=hidden -Wall -D_REENTRANT -D__EXTENSIONS__ -D_LARGEFILE64_SOURCE -D__LARGE64_FILES -D_BUILT_IN_LIBCURL -D_BUILT_IN_SQLITE3 -D_BUILTIN_IN_ZLIB -MT sqlite3.o -MD -MP -c -o sqlite3.o sqlite3.c
	ar cru libsqlite3.a sqlite3.o
	ranlib libsqlite3.a

clean:
	rm -f *.o
	rm -f libsqlite3.a
