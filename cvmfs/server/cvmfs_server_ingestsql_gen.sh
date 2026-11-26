#
# This file is part of the CernVM File System
# This script takes care of creating, removing, and maintaining repositories
# on a Stratum 0/1 server
#
# Implementation of the "cvmfs_server ingestsql-gen" command
# Convenience wrapper around cvmfs_swissknife ingestsql_gen; needs no
# repository

# This file depends on functions implemented in the following files:
# - cvmfs_server_util.sh


cvmfs_server_ingestsql_gen() {
  local input=""
  local prefix=""
  local output=""
  local format=""
  local nested=""
  local owner=""
  local jobs=""
  local schema=0
  local no_hash=0

  while [ $# -gt 0 ]; do
    case "$1" in
      -p | --prefix | -o | --output | -f | --format | -N | --nested | \
      -u | --owner | -j | --jobs )
        [ $# -ge 2 ] || die "Option $1 requires an argument"
        case "$1" in
          -p | --prefix ) prefix="$2" ;;
          -o | --output ) output="$2" ;;
          -f | --format ) format="$2" ;;
          -N | --nested ) nested="$2" ;;
          -u | --owner )  owner="$2" ;;
          -j | --jobs )   jobs="$2" ;;
        esac
        shift
        ;;
      -S | --schema )
        schema=1
        ;;
      -H | --no-hash )
        no_hash=1
        ;;
      -* )
        die "Unknown option: $1"
        ;;
      * )
        [ x"$input" = x ] || die "Only one local path is supported"
        input="$1"
        ;;
    esac
    shift
  done

  if [ $schema -eq 1 ]; then
    $(__swissknife_cmd) ingestsql_gen -S
    return $?
  fi

  [ x"$input" != x ] || die "Please provide a local file or directory"

  # positional parameters double as an argument array that survives spaces
  set -- ingestsql_gen -i "$input"
  [ x"$prefix" = x ] || set -- "$@" -p "$prefix"
  [ x"$output" = x ] || set -- "$@" -o "$output"
  [ x"$format" = x ] || set -- "$@" -f "$format"
  [ x"$nested" = x ] || set -- "$@" -N "$nested"
  [ x"$owner" = x ]  || set -- "$@" -u "$owner"
  [ x"$jobs" = x ]   || set -- "$@" -j "$jobs"
  [ $no_hash -eq 0 ] || set -- "$@" -H

  $(__swissknife_cmd) "$@"
}
