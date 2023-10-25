#!/bin/sh

# the only thing which needs to be built is a config.h file
# simply calling `make` would actually mainly build unit tests...
make clean
make src/google/sparsehash/sparseconfig.h -j

cp -rv src/google $EXTERNALS_INSTALL_LOCATION/include/
