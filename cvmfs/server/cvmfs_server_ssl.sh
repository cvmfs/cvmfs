#
# This file is part of the CernVM File System
# This script takes care of creating, removing, and maintaining repositories
# on a Stratum 0/1 server
#
# Functionality related to SSL

# This file depends on functions implemented in the following files:
# - cvmfs_server_sys.sh
# - cvmfs_server_util.sh
# - cvmfs_server_masterkeycard.sh

create_master_key() {
  local name=$1
  local user=$2

  local master_pub="/etc/cvmfs/keys/$name.pub"
  if masterkeycard_cert_available >/dev/null; then
    masterkeycard_read_pubkey >$master_pub
  else
    local master_key="/etc/cvmfs/keys/$name.masterkey"
    openssl genrsa -out $master_key 2048 > /dev/null 2>&1
    openssl rsa -in $master_key -pubout -out $master_pub > /dev/null 2>&1
    chmod 400 $master_key
    chown $user $master_key
  fi
  chmod 444 $master_pub
  chown $user $master_pub
}


create_cert() {
  local name=$1
  local user=$2

  local key; key="/etc/cvmfs/keys/$name.key"
  local csr; csr="/etc/cvmfs/keys/$name.csr"
  local crt; crt="/etc/cvmfs/keys/$name.crt"

  # Create self-signed certificate
  local cn="$name"
  if [ $(echo -n "$cn" | wc -c) -gt 30 ]; then
    cn="$(echo -n "$cn" | head -c 30)[...]"
  fi
  cn="$cn CernVM-FS Release Managers"
  openssl genrsa -out $key 2048 > /dev/null 2>&1
  openssl req -new -subj "/CN=$cn" \
    -key $key -out $csr > /dev/null 2>&1
  openssl x509 -req -days 365 -in $csr -signkey $key -out $crt > /dev/null 2>&1
  rm -f $csr
  chmod 444 $crt
  chmod 400 $key
  chown $user $crt $key
}


create_whitelist() {
  local name=$1
  local user=$2
  local spooler_definition=$3
  local temp_dir=$4
  local expire_days=$5
  local rewrite_path=$6
  local usemasterkeycard=0
  local hash_algorithm

  local whitelist
  whitelist=${temp_dir}/whitelist.$name

  local masterkey=/etc/cvmfs/keys/${name}.masterkey
  if cvmfs_sys_file_is_regular $masterkey; then
    if [ -z "$expire_days" ]; then
      expire_days=30
    fi
    echo -n "Signing $expire_days day whitelist with master key... "
  elif masterkeycard_cert_available >/dev/null; then
    usemasterkeycard=1
    if [ -z "$expire_days" ]; then
      expire_days=7
    fi
    echo -n "Signing $expire_days day whitelist with masterkeycard... "
  else
    die "Neither masterkey nor masterkeycard is available to sign whitelist!"
  fi
  echo `date -u "+%Y%m%d%H%M%S"` > ${whitelist}.unsigned
  echo "E`date -u --date="+$expire_days days" "+%Y%m%d%H%M%S"`" >> ${whitelist}.unsigned
  echo "N$name" >> ${whitelist}.unsigned
  if [ -n "$rewrite_path" ]; then
    local fingerprint
    fingerprint="`cat -v $rewrite_path | awk '/^N/{getline;print;exit}'`"
    echo "$fingerprint" >> ${whitelist}.unsigned
    hash_algorithm="`echo "$fingerprint"|sed -n 's/.*-//p'|tr '[A-Z]' '[a-z]'`"
    hash_algorithm="${hash_algorithm:-sha1}"
  else
    hash_algorithm="${CVMFS_HASH_ALGORITHM:-sha1}"
    openssl x509 -in /etc/cvmfs/keys/${name}.crt -outform der | \
      __publish hash -a $hash_algorithm -f >> ${whitelist}.unsigned
  fi

  local hash;
  hash="`cat ${whitelist}.unsigned | __publish hash -a $hash_algorithm`"
  echo "--" >> ${whitelist}.unsigned
  echo $hash >> ${whitelist}.unsigned
  echo -n $hash > ${whitelist}.hash
  if [ $usemasterkeycard -eq 1 ]; then
    masterkeycard_sign ${whitelist}.hash ${whitelist}.signature
    # verify the signature because it is not 100% reliable
    local pubkey=/etc/cvmfs/keys/${name}.pub
    if [ -f $pubkey ]; then
      cp $pubkey ${whitelist}.pub
    else
      masterkeycard_read_pubkey >${whitelist}.pub
    fi
    local checkhash="`openssl rsautl -verify -inkey ${whitelist}.pub -pubin -in ${whitelist}.signature 2>/dev/null`"
    rm -f ${whitelist}.pub
    [ "$hash" = "$checkhash" ] || die "invalid masterkeycard signature"
  else
    openssl rsautl -inkey $masterkey -sign -in ${whitelist}.hash -out ${whitelist}.signature
  fi
  cat ${whitelist}.unsigned ${whitelist}.signature > $whitelist
  chown $user $whitelist

  rm -f ${whitelist}.unsigned ${whitelist}.signature ${whitelist}.hash
  if [ -n "$rewrite_path" ]; then
    # copy first to a new name in case the filesystem is full
    cp -f $whitelist ${rewrite_path}.new
    chown $user ${rewrite_path}.new
    mv -f ${rewrite_path}.new ${rewrite_path}
  else
    __swissknife upload -i $whitelist -o .cvmfswhitelist -r $spooler_definition
  fi
  rm -f $whitelist
  syncfs
  echo "done"
}


import_keychain() {
  local name=$1
  local keys_location="$2"
  local cvmfs_user=$3
  local keys="$4"

  local global_key_dir="/etc/cvmfs/keys"
  mkdir -p $global_key_dir || return 1
  for keyfile in $keys; do
    echo -n "importing $keyfile ... "
    if [ ! -f "${global_key_dir}/${keyfile}" ]; then
      cp "${keys_location}/${keyfile}" $global_key_dir || return 2
    fi
    local key_mode=400
    if echo "$keyfile" | grep -vq '.*key$\|.gw$'; then
      key_mode=444
    fi
    chmod $key_mode "${global_key_dir}/${keyfile}"   || return 3
    chown $cvmfs_user "${global_key_dir}/${keyfile}" || return 4
    echo "done"
  done
}

