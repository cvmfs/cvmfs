cvmfs_mkfqrn() {
   local repo; repo=$1
   
   if [ -z "$repo" ]; then
      echo
      return
   fi
   
   echo $repo | grep \\.
   if [ $? -ne 0 ]; then
      echo "${repo}.${CVMFS_DEFAULT_DOMAIN}"
   fi
}

cvmfs_getorg() {
   local fqrn; fqrn=$1
   
   echo $fqrn | sed 's/^\([^\.]*\).*/\1/' 
}

cvmfs_getdomain() {
   local fqrn; fqrn=$1
   
   echo $fqrn | sed 's/^[^\.]*\.\(.*\)/\1/' 
}


repository_start() {
  :
}

repository_stop() {
  :
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

  local file
  for file in /etc/cernvm/default.conf \
              /etc/cvmfs/site.conf \
              /etc/cernvm/site.conf
  do
    if [ -f $file ]; then
      eval `sed 's/=\$(.*)//g' $file |  sed -n -e  '/^[^+]/s/\([^=]*\)[=]\(.*\)/\1="\2"; /gp'` 
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
    local found_repo; found_repo=0
    for r in `echo $CVMFS_REPOSITORIES | sed 's/,/ /g'`
    do
      if [ `cvmfs_mkfqrn $r` == $fqrn ]; then
        found_repo=1
        break
      fi
    done
    if [ $found_repo -eq 0 ]; then
      CVMFS_CACHE_DIR="$CVMFS_CACHE_BASE/$fqrn"
      return 2
    fi 
    
    for file in  /etc/cvmfs/config.d/$fqrn.conf \
                 /etc/cvmfs/config.d/$fqrn.local
    do
      [ -f $file ] && . $file
    done

    CVMFS_CACHE_DIR="$CVMFS_CACHE_BASE/$fqrn"
  fi 
    
  
  return 0
}

cvmfs_getorigin() {
   local fqrn; fqrn=$1
   local key; key=$2
   
   local domain; domain=`cvmfs_getdomain $fqrn`
   for file in /etc/cvmfs/config.d/$fqrn.local \
               /etc/cvmfs/config.d/$fqrn.conf \
               /etc/cvmfs/domain.d/$domain.local \
               /etc/cvmfs/domain.d/$domain.conf \
               /etc/cvmfs/default.local \
               /etc/cernvm/site.conf \
               /etc/cvmfs/site.conf \
               /etc/cernvm/default.conf \
               /etc/cvmfs/default.conf
   do
      if [ -f $file ]; then
         grep -q "^${key}=" $file
         if [ $? -eq 0 ]; then
            echo $file
            return 0
         fi
      fi 
   done
   
   return 1
}
