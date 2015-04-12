# Config for performance testing 
from Gaudi.Configuration import *

importOptions("$BRUNELROOT/options/Brunel-Default.py")
importOptions("$PRCONFIGOPTS/Brunel/PR-COLLISION10-Beam3500GeV-VeloClosed-MagDown.py")

from Configurables import Brunel
Brunel().EvtMax=1

