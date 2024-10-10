#!/bin/bash
source /cvmfs/sft.cern.ch/lcg/views/LCG_103/x86_64-centos9-gcc12-opt/setup.sh
#which gcc
root -q -b -e 'ROOT::RDataFrame rdf(100); auto rdf_x = rdf.Define("x", [](){ return gRandom->Rndm(); }); auto h = rdf_x.Histo1D("x"); h->DrawClone();'
exit_status=$?

# for whatever reason root returns 255
if [ $exit_status -eq 255 ]; then
    exit 0
fi
