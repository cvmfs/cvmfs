#
# This file is part of the CernVM File System.
#

TRACE_DIR=/tmp/cvmfs-trace
SPEC_DIR=/tmp/cvmfs-spec
POLICY="pdir"
FILTERS=""

print_help() {
  echo "trace.sh [--trace_dir=<trace directory>] [--spec_dir=<specification directory>] [--policy=<specification policy> [--filters=<syscall filters> ...]"
}

parse_args() {
  for i in "$@"
  do
  case $i in
      -h|--help)
      print_help
      exit
      shift
      ;;
      -t=*|--trace_dir=*)
      TRACE_DIR="${i#*=}"
      shift
      ;;
      -s=*|--spec_dir=*)
      SPEC_DIR="${i#*=}"
      shift
      ;;
      -p=*|--policy=*)
      POLICY="${i#*=}"
      shift
      ;;
      -f=*|--filters=*)
      FILTERS="$FILTERS ${i#*=}"
      shift
      ;;
      *)
      # unknown option
      echo "Unknown option ${i%=*}"
      exit
      ;;
  esac
  done
}

main() {
  SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
  mkdir -p $TRACE_DIR
  mkdir -p $SPEC_DIR
  chmod a+rw $TRACE_DIR
  chmod a+rw $SPEC_DIR

  echo "*** Writing config file..."

  sudo cp /etc/cvmfs/default.local $TRACE_DIR/config.backup
  
  sudo echo "CVMFS_TRACEFILE=$TRACE_DIR/@fqrn@.trace.log" >> /etc/cvmfs/default.local
  sudo echo "CVMFS_TRACEBUFFER=16384" >> /etc/cvmfs/default.local
  sudo echo "CVMFS_TRACEBUFFER_THRESHOLD=4092" >> /etc/cvmfs/default.local

  echo "*** Restarting autofs"

  sudo service autofs restart

  echo "*** Sleeping for a few seconds..."
  sleep 4

  echo "*** Starting bash - Please ONLY enter commands that should be traced now and enter `exit` once you are done..."

  bash

  echo "*** Bash is done..."
  REPOSITORIES=`sudo find /var/lib/cvmfs/shared -maxdepth 1 -name "cvmfs_io*"`
  for REPO_PATH in ${REPOSITORIES[@]}
  do
    REPO=${REPO_PATH#/var/lib/cvmfs/shared/cvmfs_io.}
    echo "*** Flushing buffer of $REPO"
    sudo cvmfs_talk -i $REPO tracebuffer flush
  done
  sudo cvmfs_talk tracebuffer flush

  echo "*** Writing config file..."
  sudo cp $TRACE_DIR/config.backup /etc/cvmfs/default.local
  rm $TRACE_DIR/config.backup
  echo "*** Restarting autofs"
  sudo service autofs restart

  sudo chmod a+r $TRACE_DIR/*.trace.log
  TRACE_FILES=`sudo find $TRACE_DIR -maxdepth 1 -name "*.trace.log"`
  for CUR_TRACE_FILE in ${TRACE_FILES[@]}
  do
    REPO=${CUR_TRACE_FILE#$TRACE_DIR"/"}
    REPO=${REPO%.trace.log}
    CMD="sudo python $SCRIPT_DIR/spec_builder.py"
    if [ ! -z "$POLICY" ]
    then
      CMD=$CMD" --policy $POLICY"
    fi
    if [  ! -z "$FILTERS" ]
    then
      CMD=$CMD" --filters $FILTERS"
    fi
    echo "*** Building spec for $CUR_TRACE_FILE..."
    eval $CMD" $CUR_TRACE_FILE $SPEC_DIR/$REPO.spec.txt"
  done

  echo "*** DONE: Tracefiles can be found in $TRACE_DIR, specs in $SPEC_DIR"
}

parse_args $@

main
