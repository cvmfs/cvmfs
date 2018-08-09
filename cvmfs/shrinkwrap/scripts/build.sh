#
# This file is part of the CernVM File System.
#

print_help() {
  echo "build.sh --spec_dir=<directory with spec files> --cvmfs_config=<cvmfs config> [--dest_sys=<destination file system>] [--dest_base=<destination file system base>] [--dest_data=<destination data directory>] [--gc]"
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
      --spec_dir=*)
      SPEC_DIR="${i#*=}"
      shift
      ;;
      --dest_sys=*)
      DEST_SYS="${i#*=}"
      shift
      ;;
      --dest_base=*)
      DEST_BASE="${i#*=}"
      shift
      ;;
      --dest_data=*)
      DEST_DATA="${i#*=}"
      shift
      ;;
      --cvmfs_config=*)
      CVMFS_CONFIG="${i#*=}"
      shift
      ;;
      --threads=*)
      THREADS="${i#*=}"
      shift
      ;;
      --gc)
      DO_GC="DO"
      shift
      ;;
      --fsck)
      DO_FSCK="DO"
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

export_spec() {
  REPO_PATH=$1
  REPO=${REPO_PATH#$SPEC_DIR"/"}
  REPO=${REPO%".spec.txt"}
  echo "*** Exporting $REPO"
  CMD="sudo env PATH=$PATH cvmfs_shrinkwrap -r $REPO -f $CVMFS_CONFIG -t $REPO_PATH"
  if [ ! -z "$DEST_SYS" ]
  then
  CMD=$CMD" -d $DEST_SYS"
  fi
  if [ ! -z "$DEST_BASE" ]
  then
  CMD=$CMD" -x $DEST_BASE"
  fi
  if [ ! -z "$DEST_DATA" ]
  then
  CMD=$CMD" -y $DEST_DATA"
  fi
  if [ ! -z "$THREADS" ]
  then
  CMD=$CMD" -j $THREADS"
  fi
  if [ ! -z "$DO_FSCK" ]
  then
  CMD=$CMD" -k"
  fi
  eval $CMD
}

main() {
  REPOSITORIES=`sudo find $SPEC_DIR -maxdepth 1 -name "*.spec.txt"`
  REPO_NUM=${REPOSITORIES[@]}
  for REPO_PATH in ${REPOSITORIES[@]}
  do
    export_spec $REPO_PATH
  done
  if [ ! -z "$DO_GC" ]
  then
    CMD=$CMD" -g"
    eval  $CMD
  fi
}

parse_args $@

main

