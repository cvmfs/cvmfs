OBJECTS = main.o chunk.o util.o file.o processor.o io_dispatcher.o

%.o: %.cc
	clang++ -c -g -O0 $<

all: $(OBJECTS) buffer.h chunk.h chunk_detector.h io_dispatcher.h processor.h spooler.h util.h
	clang++ -o spike ../cvmfs/logging.cc $(OBJECTS) -ltbb_debug -ltbbmalloc_debug -lrt -lcrypto

clean:
	rm -f *.o spike