#!/bin/sh
#if [ -z $PATH ]
#then
#    export PATH=/afs/cern.ch/cms/caf/scripts
#else
#    export PATH=/afs/cern.ch/cms/caf/scripts:${PATH}
#fi
if [ -z $CVS_RSH ]
then
    export CVS_RSH=ssh
fi
#alias bsub=/afs/cern.ch/cms/caf/scripts/cmsbsub
#source /afs/cern.ch/project/eos/installation/cms/etc/setup.sh
#alias eoscms=eos
#alias cms_adler32=/afs/cern.ch/cms/caf/bin/cms_adler32
