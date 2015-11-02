all: libsha3.a

SOURCES = KeccakF-1600-opt64.c \
  KeccakF-1600-64.macros \
  KeccakF-1600-unrolling.macros \
  KeccakF-1600-opt64-settings.h \
  KeccakF-1600-interface.h \
  KeccakHash.c \
  KeccakHash.h \
  KeccakSponge.c \
  KeccakSponge.h \
  SnP-FBWL-default.c \
  SnP-FBWL-default.h \
  SnP-Relaned.h \
  SnP-interface.h \
  brg_endian.h

OBJECT_FILES = KeccakF-1600-opt64.o \
  KeccakHash.o \
  KeccakSponge.o \
  SnP-FBWL-default.o

CFLAGS = -I. -g -O2 -fPIC -Wall

libsha3.a: $(OBJECT_FILES)
	ar cru libsha3.a $(OBJECT_FILES)
	ranlib libsha3.a

KeccakF-1600-opt64.o: $(SOURCES)
	$(CC) $(CFLAGS) -c KeccakF-1600-opt64.c

KeccakHash.o: $(SOURCES)
	$(CC) $(CFLAGS) -c KeccakHash.c

KeccakSponge.o: $(SOURCES)
	$(CC) $(CFLAGS) -c KeccakSponge.c

SnP-FBWL-default.o: $(SOURCES)
	$(CC) $(CFLAGS) -c SnP-FBWL-default.c

clean:
	rm -f *.o libsha3.a
