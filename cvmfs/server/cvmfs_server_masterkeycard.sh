#
# This file is part of the CernVM File System
# This script takes care of creating, removing, and maintaining repositories
# on a Stratum 0/1 server
#
# Implementation of the "cvmfs_server masterkeycard" command

# This file depends on functions implemented in the following files:
# - cvmfs_server_sys.sh
# - cvmfs_server_util.sh
# - cvmfs_server_common.sh
# - cvmfs_server_resign.sh

# Check if a masterkeycard is available to be used
# If not, the reason is sent to stdout and false is returned,
# otherwise there's nothing to stdout and true is returned
masterkeycard_available() {
  local pattern="Yubikey.*CCID"
  local reason=""
  if ! lsusb | grep -q "$pattern"; then
    reason="USB device matching \"$pattern\" not present"
  elif ! which opensc-tool > /dev/null 2>&1; then
    reason="opensc-tool (from opensc package) not found"
  elif ! which systemctl > /dev/null 2>&1; then
    reason="masterkeycard only supported on systems with systemctl"
  elif ! systemctl is-active --quiet pcscd.socket; then
    reason="systemctl unit pcscd.socket is not active"
  elif ! systemctl is-enabled --quiet pcscd.socket; then
    reason="systemctl unit pcscd.socket is not enabled"
  elif ! opensc-tool -l | grep -q "$pattern"; then
    reason="opensc-tool -l has no device matching \"$pattern\""
  elif ! which yubico-piv-tool >/dev/null 2>&1; then
    reason="yubico-piv-tool (from yubico-piv-tool package) not found"
  elif yubico-piv-tool -a status 2>&1 | grep -q "Failed to connect"; then
    reason="yubico-piv-tool failed to connect to device"
  elif ! pkcs11-tool -I >/dev/null 2>&1; then
    reason="pkcs11-tool -I failed"
  fi
  if [ -n "$reason" ]; then
    echo "$reason"
    return 1
  fi
}

# same as above except also check if cert is present
masterkeycard_cert_available() {
  local reason
  reason="`masterkeycard_available`"
  if [ -z "$reason" ] && [ -z "`masterkeycard_read_cert`" ]; then
    reason="no certificate stored in device"
  fi
  if [ -n "$reason" ]; then
    echo "$reason"
    return 1
  fi
}

masterkeycard_read_cert() {
  yubico-piv-tool -s 9c -a read-certificate 2>/dev/null
}

masterkeycard_read_pubkey() {
  masterkeycard_read_cert | openssl x509 -pubkey -noout
}

masterkeycard_store() {
  local masterkey=$1
  # Note that these commands will fail if the management (mgm) key has been
  #   changed, but if a user is advanced enough to know that they can always
  #   run these commands by hand with the new mgm key for the rare case of
  #   storing a masterkey.
  #   Changing the management key is not required to keep a stored key from
  #   being used by an attacker who gains physical custody; changing the PIN
  #   and PUK can do that.
  yubico-piv-tool -s 9c -i $masterkey -a import-key
  openssl req -new -subj '/O=o/CN=cn' -x509 -days 36500 -key $masterkey | \
    yubico-piv-tool -s 9c -a import-cert
}

masterkeycard_delete() {
  yubico-piv-tool -s 9c -a delete-certificate
}

masterkeycard_sign() {
  local hashfile=$1
  local sigfile=$2

  local pkcsout
  # Capture output in a variable to bypass annoying "Using" messages that
  #  normally go to stderr, while still checking the exit code from pkcs11-tool.
  if ! pkcsout="`pkcs11-tool -p ${CVMFS_MASTERKEYCARD_PIN:-123456} -s -m RSA-PKCS -i $hashfile -o $sigfile 2>&1`"; then
    echo "$pkcsout" >&2
    return 1
  fi
}

cvmfs_server_masterkeycard() {
  local names
  local name
  local goodnames
  local retcode=0
  local action=""
  local force=0
  local reason
  local masterkey
  local pubkey

  # optional parameter handling
  OPTIND=1
  while getopts "aksdrcf" option
  do
    case $option in
      a|k|s|d|r|c)
        [ -z "$action" ] || die "Only one masterkeycard action option allowed"
        action=$option
      ;;
      f)
        # force skipping the prompts for dangerous actions
        force=1
      ;;
      ?)
        shift $(($OPTIND-2))
        usage "Command masterkeycard: Unrecognized option: $1"
      ;;
    esac
  done
  shift $(($OPTIND-1))

  [ -n "$action" ] || usage "Command masterkeycard: no action option given"

  if [ $action = s ]; then
    is_root || die "Only root may store to the masterkeycard"
    check_parameter_count 1 $#
    name="$1"
  elif [ $action = c ]; then
    is_root || die "Only root may convert repositories to use the masterkeycard"
    check_parameter_count_for_multiple_repositories $#
    names=$(get_or_guess_multiple_repository_names "$@")
  elif [ $action = d ]; then
    is_root || die "Only root may delete from the masterkeycard"
    check_parameter_count 0 $#
  else
    check_parameter_count 0 $#
  fi

  case $action in
    a)
      # check whether a smartcard is available
      reason="`masterkeycard_available`" || die "$reason"
      echo masterkeycard is available
    ;;
    k)
      # check whether a cert (and presumably key) is stored in the card
      reason="`masterkeycard_cert_available`" || die "$reason"
      echo masterkeycard key is available
    ;;
    s)
      # Store the masterkey from the given repository into the card.
      # Does not need to be a fully created repo, the masterkey just has
      #  to exist.
      masterkey="/etc/cvmfs/keys/${name}.masterkey"

      cvmfs_sys_file_is_regular $masterkey || die "$masterkey not found"

      if [ $force -ne 1 ] && masterkeycard_read_cert >/dev/null; then
        local reply
        read -p "You are about to overwrite a stored key!  Are you sure (y/N)? " reply
        if [ "$reply" != "y" ] && [ "$reply" != "Y" ]; then
          return 1
        fi
      fi

      masterkeycard_store $masterkey

      if check_repository_existence "$name"; then
        echo
        echo "Now back up $masterkey"
        echo "to flash drives stored in safe places and use"
        echo "  cvmfs_server masterkeycard -c $name"
        echo "to remove the masterkey and convert to use the key in the card."
      fi
    ;;
    d)
      # delete a certificate from the card
      reason="`masterkeycard_available`" || die "$reason"
      masterkeycard_read_cert >/dev/null || { echo "No certificate in card to delete"; exit; }

      if [ $force -ne 1 ]; then
        local reply
        read -p "You are about to delete a card's stored certificate!  Are you sure (y/N)? " reply
        if [ "$reply" != "y" ] && [ "$reply" != "Y" ]; then
          return 1
        fi
      fi

      masterkeycard_delete
      echo
      echo "IMPORTANT NOTE: this did not delete the masterkey, only the certificate,"
      echo "  so the card still needs to be kept physically secure.  The masterkey can"
      echo "  still be made usable by storing a certificate made from the corresponding"
      echo "  pub key. To destroy the masterkey, store an unimportant key in its place."
    ;;
    r)
      # read the pub key from the card to stdout
      reason="`masterkeycard_cert_available`" || die "$reason"
      masterkeycard_read_pubkey
    ;;
    c)
      # convert given repositories to use the key in the card

      for name in $names; do

        # sanity checks
        check_repository_existence "$name" || { echo "Repository $name does not exist"; retcode=1; continue; }
        is_stratum0 $name  || { echo "Repository $name is not a stratum 0 repository"; retcode=1; continue; }
        health_check $name || { echo "Repository $name is not healthy"; retcode=1; continue; }

        # get repository information
        load_repo_config $name

        # check if repository is compatible to the installed CernVM-FS version
        check_repository_compatibility $name

        goodnames="$goodnames $name"
      done

      if [ $force -eq 1 ] && [ $retcode -ne 0 ]; then
        # If any repo had a problem, and -f is requested, do nothing.
        # If -f is not requested, the user will have a chance to decide
        # whether or not to proceed with the other repos.
        return $retcode
      fi

      if [ -n "$goodnames" ] && [ $force -ne 1 ]; then
        echo "The following repository(ies) will be converted to use the masterkeycard:"
        echo " $goodnames"
        echo "This will remove the masterkey and update the pub key and cannot be undone!"
        local reply
        read -p "Are you sure you want to proceed (y/N)? " reply
        if [ "$reply" != "y" ] && [ "$reply" != "Y" ]; then
          return 1
        fi
      fi

      pubkey="`masterkeycard_read_pubkey`" || die "Failure reading pub key from mastercard"
      for name in $goodnames; do
        masterkey="/etc/cvmfs/keys/${name}.masterkey"
        if cvmfs_sys_file_is_regular $masterkey; then
          echo "Removing $masterkey and updating pub key"
          shred -uf $masterkey
        else
          echo "$masterkey already missing, but updating pub key"
        fi
        echo "$pubkey" >/etc/cvmfs/keys/$name.pub
        cvmfs_server_resign $name
      done
    ;;
  esac


  return $retcode
}


