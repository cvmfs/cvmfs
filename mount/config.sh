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

  if [ -f /etc/cvmfs/default.conf ]
  then
    . /etc/cvmfs/default.conf
  else
    return 1
  fi

  local dist_default="/etc/cvmfs/default.d/*.conf"
  local file
  for file in /etc/cernvm/default.conf $dist_default \
              /etc/cernvm/default.conf \
              /etc/cvmfs/site.conf \
              /etc/cernvm/site.conf
  do
    if [ -f $file ]; then
      eval `cat $file | tr -d \" | sed 's/=\$(.*)//g' | sed -n -e  '/^[^+]/s/\([^=]*\)[=]\(.*\)/\1="\2"; /gp'`
    fi
  done

  if [ -f /etc/cvmfs/default.local ]
  then
    . /etc/cvmfs/default.local
  fi

  if [ "x$domain" != "x" ];
  then
    for file in  /etc/cvmfs/domain.d/$domain.conf \
                 /etc/cvmfs/domain.d/$domain.local
    do
      [ -f $file ] && . $file
    done
  fi

  if [ "x$fqrn" != "x" ]
  then
    for file in  /etc/cvmfs/config.d/$fqrn.conf \
                 /etc/cvmfs/config.d/$fqrn.local
    do
      [ -f $file ] && . $file
    done

    if [ x"$CVMFS_SHARED_CACHE" = xyes ]; then
      CVMFS_CACHE_DIR="$CVMFS_CACHE_BASE/shared"
    else
      CVMFS_CACHE_DIR="$CVMFS_CACHE_BASE/$fqrn"
    fi
  fi

  return 0
}

cvmfs_getorigin() {
   local fqrn; fqrn=$1
   local key; key=$2

   local domain; domain=`cvmfs_getdomain $fqrn`

   local dist_default="/etc/cvmfs/default.d/*.conf"
   local dist_default_reverse=
   for file in $dist_default; do
     dist_default_reverse="$file $dist_default_reverse"
   done
   source=`grep -H "^[ ]*\(readonly\)\{0,1\}[ ]*${key}=" \
      /etc/cvmfs/config.d/$fqrn.local \
      /etc/cvmfs/config.d/$fqrn.conf \
      /etc/cvmfs/domain.d/$domain.local \
      /etc/cvmfs/domain.d/$domain.conf \
      /etc/cvmfs/default.local \
      /etc/cernvm/site.conf \
      /etc/cvmfs/site.conf \
      /etc/cernvm/default.conf \
      $dist_default_reverse /etc/cvmfs/default.conf \
      2>/dev/null | head -n1 | cut -d":" -f1`
   if [ "x$source" != "x" ]; then
      echo $source
      return 0
   fi

   return 1
}
