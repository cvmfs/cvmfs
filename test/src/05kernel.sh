
cvmfs_test_name="Linux Kernel Compile"

cvmfs_run_test() {
  logfile=$1

  setup_sft || return 1

  logdir=`dirname $logfile`
  full_logpath="`cd $logdir; pwd`/`basename $logfile`"
  outdir=/tmp/kbuild
  if [ "x$CVMFS_TEMP_DIR" != "x" ]; then
    outdir=$CVMFS_TEMP_DIR/kbuild
  fi

  rm -rf $outdir
  cd /cvmfs/sft.cern.ch/lcg/external/experimental/linux
  ./compileKernel.sh 2.6.32.57 $outdir 8 >> $full_logpath 2>&1 || return 2
  ./compileKernel.sh 2.6.32.57 $outdir 8 >> $full_logpath 2>&1 || return 3
  sudo cvmfs-talk -i sft cleanup 0 >> $full_logpath || return 4 
  ./compileKernel.sh 2.6.32.57 $outdir 8 >> $full_logpath 2>&1 || return 5

  ps aux | grep cvmfs2 | grep sft.cern.ch >> $logfile
  check_memory sft.cern.ch 50000 || return 6 
  cvmfs_config stat -v sft.cern.ch >> $logfile

  return 0
}

