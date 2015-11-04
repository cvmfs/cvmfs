all: libsha3.a

ARCH = 64opt

SOURCES_64OPT = 64opt/KeccakF-1600-opt64.c \
  64opt/KeccakF-1600-64.macros \
  64opt/KeccakF-1600-unrolling.macros \
  64opt/KeccakF-1600-opt64-settings.h \
  64opt/SnP-interface.h

SOURCES_GENERIC = KeccakF-1600-interface.h \
  KeccakHash.c \
  KeccakHash.h \
  KeccakSponge.c \
  KeccakSponge.h \
  SnP-FBWL-default.c \
  SnP-FBWL-default.h \
  SnP-Relaned.h \
  brg_endian.h

OBJECT_FILES = KeccakF-1600-$(ARCH).o \
  KeccakHash.o \
  KeccakSponge.o \
  SnP-FBWL-default.o

CFLAGS = -I. -I$(ARCH) -g -O2 -fPIC -Wall

libsha3.a: $(OBJECT_FILES)
	ar cru libsha3.a $(OBJECT_FILES)
	ranlib libsha3.a

KeccakF-1600-$(ARCH).o: $(SOURCES_GENERIC) $(SOURCES_64OPT)
	$(CC) $(CFLAGS) -o KeccakF-1600-$(ARCH).o -c 64opt/KeccakF-1600-opt64.c

KeccakHash.o: $(SOURCES_GENERIC)
	$(CC) $(CFLAGS) -c KeccakHash.c

KeccakSponge.o: $(SOURCES_GENERIC)
	$(CC) $(CFLAGS) -c KeccakSponge.c

SnP-FBWL-default.o: $(SOURCES_GENERIC)
	$(CC) $(CFLAGS) -c SnP-FBWL-default.c

clean:
	rm -f *.o libsha3.a
