#!/bin/bash
source /cvmfs/sft.cern.ch/lcg/views/LCG_103/x86_64-centos9-gcc11-opt/setup.sh; root -q -b "./dd4hep_load_detector.C(\"/cvmfs/sft.cern.ch/lcg/releases/DD4hep/01.25.01-5059b/x86_64-centos7-gcc11-opt/DDDetectors/compact/SiD.xml\")"
