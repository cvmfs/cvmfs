#
# This file is part of the CernVM File System
# This script takes care of creating, removing, and maintaining repositories
# on a Stratum 0/1 server
#
# Implementation of the "cvmfs_server refresh-lease" command
#
# Renews the gateway lease of an active transaction by sending a PATCH request
# to the gateway's /leases/<token> endpoint. This allows short leases to be
# kept alive indefinitely by a publishing tool, while still expiring quickly if
# that tool crashes. The HMAC authorization header is computed here so callers
# do not have to.

# This file depends on functions implemented in the following files:
# - cvmfs_server_util.sh
# - cvmfs_server_common.sh

cvmfs_server_refresh_lease() {
  local name
  local expires_in_sec=""

  # parameter handling
  OPTIND=1
  while getopts "t:" option; do
    case $option in
      t)
        expires_in_sec=$OPTARG
      ;;
      ?)
        shift $(($OPTIND-2))
        usage "Command refresh-lease: Unrecognized option: $1"
      ;;
    esac
  done

  # get repository name
  shift $(($OPTIND-1))
  check_parameter_count 1 $#
  name=$(get_repository_name $1)

  is_valid_repo_name "$name" || die "invalid repository name: $name"

  # validate -t if given (must be a positive integer)
  if [ x"$expires_in_sec" != x"" ]; then
    echo "$expires_in_sec" | grep -qE '^[0-9]+$' || \
      die "invalid value for -t: '$expires_in_sec' (expected a number of seconds)"
  fi

  load_repo_config $name
  is_owner_or_root $name || die "Permission denied: Repository $name is owned by $CVMFS_USER"

  # only gateway repositories have leases to refresh
  local upstream=$CVMFS_UPSTREAM_STORAGE
  [ x"$(get_upstream_type $upstream)" = xgw ] || \
    die "$name is not connected to a gateway; nothing to refresh"
  local gateway_api_url
  gateway_api_url=$(get_upstream_config "$upstream")

  # the session token of the active transaction
  local session_token_file="${CVMFS_SPOOL_DIR}/session_token"
  cvmfs_sys_file_is_regular "$session_token_file" || \
    die "No active gateway session for $name (is a transaction open?)"
  local token
  token=$(cat "$session_token_file")
  [ x"$token" != x"" ] || die "Empty session token in $session_token_file"

  # read the .gw key to compute the HMAC for authentication
  local gw_key_file="/etc/cvmfs/keys/${name}.gw"
  cvmfs_sys_file_is_regular "$gw_key_file" || die "Gateway key not found: $gw_key_file"
  local gw_key_id
  local gw_key_secret
  gw_key_id=$(cat "$gw_key_file" | awk '{print $2}')
  gw_key_secret=$(cat "$gw_key_file" | awk '{print $3}')
  [ x"$gw_key_id" != x"" ] && [ x"$gw_key_secret" != x"" ] || \
    die "Could not parse gateway key file: $gw_key_file"

  # for lease operations on /leases/<token> the HMAC is computed over the token
  local hmac_hash
  hmac_hash=$(echo -n "$token" | openssl dgst -sha1 -hmac "$gw_key_secret" | awk '{print $NF}')
  local hmac_b64
  hmac_b64=$(echo -n "$hmac_hash" | base64)

  # Send the PATCH. When -t is given, carry the requested extension in the
  # request body; otherwise send no body so the gateway applies its configured
  # default refresh extension. The HMAC covers the token, not the body.
  local lease_url="${gateway_api_url}/leases/${token}"
  local response
  if [ x"$expires_in_sec" != x"" ]; then
    response=$(curl -sf -X PATCH \
      -H "Authorization: ${gw_key_id} ${hmac_b64}" \
      -H "Content-Type: application/json" \
      --data "{\"expires_in_sec\":${expires_in_sec}}" \
      "$lease_url" 2>&1) || \
      die "Could not reach gateway at ${gateway_api_url}/leases"
  else
    response=$(curl -sf -X PATCH \
      -H "Authorization: ${gw_key_id} ${hmac_b64}" \
      "$lease_url" 2>&1) || \
      die "Could not reach gateway at ${gateway_api_url}/leases"
  fi

  has_jq || die "refresh-lease requires 'jq' to parse the gateway response"

  local status
  status=$(echo "$response" | jq -r '.status // empty' 2>/dev/null) || \
    die "Could not parse gateway response: $response"

  if [ x"$status" != x"ok" ]; then
    local reason
    reason=$(echo "$response" | jq -r '.reason // "unknown"' 2>/dev/null)
    die "Gateway refused to refresh the lease: $reason"
  fi

  local expires
  expires=$(echo "$response" | jq -r '.data.expires // empty' 2>/dev/null)
  echo "Lease for $name refreshed; new expiration: $expires"
}
