set -e # ESSENTIAL! Don't remove this!
       # Stops the server script in case anything unexpected occurs, so that
       # malfunctions cause as less damage as possible.
       # For example a crashing `cvmfs_server publish` is prevented from wiping
       # the scratch area, giving us a chance to fix and retry the process.

die() {
  ( [ x"$(echo -n -e)" = x"-e" ] && echo $1 >&2 || echo -e $1 ) >&2
  exit 1
}

# set default locale
export LC_ALL=C
