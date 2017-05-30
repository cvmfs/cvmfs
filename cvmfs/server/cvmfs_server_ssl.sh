#
# This file is part of the CernVM File System
# This script takes care of creating, removing, and maintaining repositories
# on a Stratum 0/1 server
#
# Functionality related to SSL

# This file depends on fuctions implemented in the following files:
# - cvmfs_server_util.sh

create_master_key() {
  local name=$1
  local user=$2

  master_key="/etc/cvmfs/keys/$name.masterkey"
  master_pub="/etc/cvmfs/keys/$name.pub"

  openssl genrsa -out $master_key 2048 > /dev/null 2>&1
  openssl rsa -in $master_key -pubout -out $master_pub > /dev/null 2>&1
  chmod 400 $master_key
  chmod 444 $master_pub
  chown $user $master_key $master_pub
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
  openssl req -new -subj "/C=/ST=/L=/O=/OU=/CN=$cn" \
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

  local whitelist
  whitelist=${temp_dir}/whitelist.$name
  local hash_algorithm="${CVMFS_HASH_ALGORITHM-sha1}"

  echo -n "Signing 30 day whitelist with master key... "
  echo `date -u "+%Y%m%d%H%M%S"` > ${whitelist}.unsigned
  echo "E`date -u --date='+30 days' "+%Y%m%d%H%M%S"`" >> ${whitelist}.unsigned
  echo "N$name" >> ${whitelist}.unsigned
  openssl x509 -in /etc/cvmfs/keys/${name}.crt -outform der | \
    __swissknife hash -a $hash_algorithm -f >> ${whitelist}.unsigned

  local hash;
  hash="`cat ${whitelist}.unsigned | __swissknife hash -a $hash_algorithm`"
  echo "--" >> ${whitelist}.unsigned
  echo $hash >> ${whitelist}.unsigned
  echo -n $hash > ${whitelist}.hash
  openssl rsautl -inkey /etc/cvmfs/keys/${name}.masterkey -sign -in ${whitelist}.hash -out ${whitelist}.signature
  cat ${whitelist}.unsigned ${whitelist}.signature > $whitelist
  chown $user $whitelist

  rm -f ${whitelist}.unsigned ${whitelist}.signature ${whitelist}.hash
  __swissknife upload -i $whitelist -o .cvmfswhitelist -r $spooler_definition
  rm -f $whitelist
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
    if echo "$keyfile" | grep -vq '.*key$'; then
      key_mode=444
    fi
    chmod $key_mode "${global_key_dir}/${keyfile}"   || return 3
    chown $cvmfs_user "${global_key_dir}/${keyfile}" || return 4
    echo "done"
  done
}

