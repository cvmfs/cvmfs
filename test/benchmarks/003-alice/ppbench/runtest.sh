#!/bin/sh
# Root
# export ROOTSYS=/afs/cern.ch/alice/library/root/new
# export PATH=$ROOTSYS/bin:$PATH
# export LD_LIBRARY_PATH=$ROOTSYS/lib:$LD_LIBRARY_PATH
# AliRoot
# export ALICE=/afs/cern.ch/alice/library
# export ALICE_LEVEL=new
# export ALICE_ROOT=$ALICE/$ALICE_LEVEL
# export ALICE_TARGET=`$ROOTSYS/bin/root-config --arch`
# export PATH=$ALICE_ROOT/bin/tgt_${ALICE_TARGET}:$PATH
# export LD_LIBRARY_PATH=$ALICE_ROOT/lib/tgt_${ALICE_TARGET}:$LD_LIBRARY_PATH
# Geant3
# export LD_LIBRARY_PATH=$ALICE/geant3/lib/tgt_${ALICE_TARGET}:$LD_LIBRARY_PATH

rm -rf *.root *.dat *.log fort* hlt hough raw* recraw/*.root recraw/*.log GRP QAImageSim0.ps QAImageRec0.ps
aliroot -b -q $1sim.C      2>&1 | tee sim.log
# mv syswatch.log simwatch.log
aliroot -b -q $1rec.C      2>&1 | tee rec.log
mv syswatch.log recwatch.log
aliroot -b -q ${ALICE_ROOT}/STEER/CheckESD.C 2>&1 | tee check.log
aliroot -b -q aod.C 2>&1 | tee aod.log

cd recraw
ln -s ../raw.root .
aliroot -b -q rec.C      2>&1 | tee rec.log
mv syswatch.log ../rawwatch.log
aliroot -b -q aod.C 2>&1 | tee aod.log



