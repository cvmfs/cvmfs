all: libsha3.a

# ARCH = 64opt (set by makeHook.sh)

SOURCES_64OPT = 64opt/KeccakF-1600-opt64.c \
  64opt/KeccakF-1600-64.macros \
  64opt/KeccakF-1600-unrolling.macros \
  64opt/KeccakF-1600-opt64-settings.h \
  64opt/SnP-interface.h

SOURCES_64COMPACT = 64compact/KeccakF-1600-compact64.c \
  64compact/SnP-interface.h

SOURCES_32BI = 32BI/KeccakF-1600-inplace32BI.c \
  32BI/SnP-interface.h

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

KeccakF-1600-64opt.o: $(SOURCES_GENERIC) $(SOURCES_64OPT)
	$(CC) $(CFLAGS) -o KeccakF-1600-64opt.o -c 64opt/KeccakF-1600-opt64.c

KeccakF-1600-64compact.o: $(SOURCES_GENERIC) $(SOURCES_64COMPACT)
	$(CC) $(CFLAGS) -o KeccakF-1600-64compact.o -c 64compact/KeccakF-1600-compact64.c

KeccakF-1600-32BI.o: $(SOURCES_GENERIC) $(SOURCES_32BI)
	$(CC) $(CFLAGS) -o KeccakF-1600-32BI.o -c 32BI/KeccakF-1600-inplace32BI.c

KeccakHash.o: $(SOURCES_GENERIC)
	$(CC) $(CFLAGS) -c KeccakHash.c

KeccakSponge.o: $(SOURCES_GENERIC)
	$(CC) $(CFLAGS) -c KeccakSponge.c

SnP-FBWL-default.o: $(SOURCES_GENERIC)
	$(CC) $(CFLAGS) -c SnP-FBWL-default.c

clean:
	rm -f *.o libsha3.a
