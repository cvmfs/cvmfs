#!/bin/csh
if ( $?PATH ) then
    setenv PATH /afs/cern.ch/cms/caf/scripts:${PATH}
else
    setenv PATH /afs/cern.ch/cms/caf/scripts
endif
if ( ! $?CVS_RSH ) then
    setenv CVS_RSH ssh
endif
alias bsub /afs/cern.ch/cms/caf/scripts/cmsbsub
source /afs/cern.ch/project/eos/installation/cms/etc/setup.csh
alias eoscms eos
alias cms_adler32 /afs/cern.ch/cms/caf/bin/cms_adler32
