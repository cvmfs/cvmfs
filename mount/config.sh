cvmfs_mkfqrn() {
   local repo; repo=$1

   if [ -z "$repo" ]; then
      echo
      return 0
   fi

   echo $repo | grep \\. || echo "${repo}.${CVMFS_DEFAULT_DOMAIN}"
   return 0
}

cvmfs_getorg() {
   local fqrn; fqrn=$1

   echo $fqrn | sed 's/^\([^\.]*\).*/\1/'
}

cvmfs_getdomain() {
   local fqrn; fqrn=$1

   echo $fqrn | sed 's/^[^\.]*\.\(.*\)/\1/'
}

cvmfs_readconfig() {
  local fqrn; fqrn=$1
  local org; org=`cvmfs_getorg $fqrn`
  local domain; domain=`cvmfs_getdomain $fqrn`

  CVMFS_PARMS=$(cvmfs2 -o parse "$fqrn" /)
  local ret=$?
  if [ $ret != 0 ]; then
    return $ret
  fi
  unset CVMFS_CACHE_DIR
  eval "$CVMFS_PARMS"

  if [ "x$fqrn" != "x" ]
  then
    if [ "x$CVMFS_CACHE_DIR" = "x" ]; then
      if [ x"$CVMFS_SHARED_CACHE" = xyes ]; then
        CVMFS_CACHE_DIR="$CVMFS_CACHE_BASE/shared"
      else
        CVMFS_CACHE_DIR="$CVMFS_CACHE_BASE/$fqrn"
      fi
    fi
  fi

  return 0
}

cvmfs_getorigin() {
   local fqrn; fqrn=$1
   local key; key=$2

   local domain; domain=`cvmfs_getdomain $fqrn`

   source=$(echo "$CVMFS_PARMS" | grep "^${key}=" | awk '{print $NF}')
   if [ "x$source" != "x" ]; then
      echo $source
      return 0
   fi

   return 1
}
