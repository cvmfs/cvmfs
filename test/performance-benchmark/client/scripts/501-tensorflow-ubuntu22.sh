#!/bin/bash
. /cvmfs/sft.cern.ch/lcg/views/LCG_106/x86_64-ubuntu2204-gcc11-opt/setup.sh
TF_CPP_MIN_LOG_LEVEL=3 python3 -c "import numpy as np; import tensorflow as tf;"


#which gcc
#TF_CPP_MIN_LOG_LEVEL=3 python3 -c "import os; os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'; import numpy as np; import tensorflow as tf"

#0 = all messages are logged (default behavior)
#1 = INFO messages are not printed
#2 = INFO and WARNING messages are not printed
#3 = INFO, WARNING, and ERROR messages are not printed
