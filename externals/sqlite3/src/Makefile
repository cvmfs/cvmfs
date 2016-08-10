all:
	gcc $(CVMFS_BASE_C_FLAGS) -DHAVE_CONFIG_H -I. -I../.. -w -fPIC -DSQLITE_THREADSAFE=2 -DHAVE_MALLOC_USABLE_SIZE -DSQLITE_ENABLE_MEMORY_MANAGEMENT -DSQLITE_ENABLE_MEMSYS5 -D_REENTRANT -D__EXTENSIONS__ -D_LARGEFILE64_SOURCE -D__LARGE64_FILES -MT sqlite3.o -MD -MP -c -o sqlite3.o sqlite3.c
	ar cru libsqlite3.a sqlite3.o
	ranlib libsqlite3.a

clean:
	rm -f *.o
	rm -f libsqlite3.a
