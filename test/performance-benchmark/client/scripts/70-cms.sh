workdir=$1
mkdir -p $workdir
cd $workdir

FQRN=cms-ib.cern.ch

#SWVERSION=CMSSW_13_0_3
SWVERSION=CMSSW_13_1_0_pre3
set -e
source /cvmfs/cms-ib.cern.ch/sw/x86_64/week0/cmsset_default.sh

# IFF SWVERSION is not available anymore, list all available version and select
# a good one

#list current cmssw versions
#scram list CMSSW


scramv1 project CMSSW $SWVERSION
cd $SWVERSION
eval `scram runtime -sh`

# new cmd recommended by CMS GEN/SIM team
cmsDriver.py TTbar_14TeV_TuneCP5_cfi --era Run3 -s GEN,SIM --customise=Validation/Performance/TimeMemoryInfo.py --pileup=NoPileUp --conditions auto:phase1_2022_realistic  --beamspot Run3RoundOptics25ns13TeVLowSigmaZ --geometry DB:Extended --eventcontent=RAWSIM --relval 9000,100 --datatier GEN-SIM --dirout=./ -n 1 --fileout file:ttbar14_2022.root  --mc --no_exec

cd -
