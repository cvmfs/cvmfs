#
# This file is part of the CernVM File System
# This script takes care of connecting a publisher to a gateway
#
# Implementation of the "cvmfs_server connect-gw" command

# This file depends on functions implemented in the following files:
# - cvmfs_server_util.sh
# - cvmfs_server_common.sh
# - cvmfs_server_ssl.sh
# - cvmfs_server_apache.sh
# - cvmfs_server_json.sh
# - cvmfs_server_mkfs.sh


cvmfs_server_connect_gw() {
  local name
  local gateway_url
  local owner
  local keys_import_location
  local stratum0
  local fetch_keys=0
  local no_publisher=0

  # parameter handling
  OPTIND=1
  while getopts "u:o:k:w:KP" option; do
    case $option in
      u)
        gateway_url=$OPTARG
      ;;
      o)
        owner=$OPTARG
      ;;
      k)
        keys_import_location=$OPTARG
      ;;
      w)
        stratum0=$OPTARG
      ;;
      K)
        fetch_keys=1
      ;;
      P)
        no_publisher=1
      ;;
      ?)
        shift $(($OPTIND-2))
        usage "Command connect-gw: Unrecognized option: $1"
      ;;
    esac
  done

  # get repository name
  shift $(($OPTIND-1))
  check_parameter_count 1 $#
  name=$(get_repository_name $1)

  is_valid_repo_name "$name" || die "invalid repository name: $name"
  is_root                    || die "Only root can connect to a gateway"

  # gateway URL is required
  [ x"$gateway_url" != x"" ] || die "Please specify the gateway with -u (e.g. -u gateway.cern.ch)"

  # If the user passed just a hostname (no scheme, no port, no path), expand it
  # to the full gateway API URL: http://<host>:4929/api/v1
  if ! echo "$gateway_url" | grep -q '://'; then
    gateway_url="http://${gateway_url}:4929/api/v1"
  elif ! echo "$gateway_url" | grep -q '/api/'; then
    # Has scheme but no API path — append default port and path if missing
    # Strip trailing slash
    gateway_url=$(echo "$gateway_url" | sed 's|/$||')
    # Add port if missing
    if ! echo "$gateway_url" | grep -qE ':[0-9]+$'; then
      gateway_url="${gateway_url}:4929"
    fi
    gateway_url="${gateway_url}/api/v1"
  fi

  # Health check: verify the gateway is reachable
  echo -n "Checking gateway connectivity at ${gateway_url}... "
  local gw_host gw_port
  gw_host=$(echo "$gateway_url" | sed -E 's|https?://([^:/]+).*|\1|')
  gw_port=$(echo "$gateway_url" | sed -E 's|https?://[^:]+:([0-9]+).*|\1|')
  [ x"$gw_port" = x"$gateway_url" ] && gw_port=80  # no port matched, default to 80

  # First check if the host/port is reachable at the TCP level
  if ! timeout 5 bash -c "echo >/dev/tcp/${gw_host}/${gw_port}" 2>/dev/null; then
    die "fail!

  Cannot connect to ${gw_host}:${gw_port}.
  Possible causes:
    - The gateway service is not running (check with: systemctl status cvmfs-gateway)
    - A firewall is blocking port ${gw_port}
    - The hostname '${gw_host}' does not resolve or is unreachable
  "
  fi

  # Then check if the gateway API responds
  local health_response
  health_response=$(curl -sf --max-time 10 "${gateway_url}" 2>&1)
  if [ $? -ne 0 ]; then
    die "fail!

  Host ${gw_host}:${gw_port} is reachable but the gateway API is not responding at:
    ${gateway_url}
  Possible causes:
    - The gateway is listening on a different port (default: 4929)
    - The API path is wrong (expected: /api/v1)
    - The gateway service is starting up or in a bad state
  Check the gateway with: curl ${gateway_url}
  "
  fi
  echo "ok"

  # default stratum0 URL: derive from gateway URL
  if [ x"$stratum0" = x"" ]; then
    # Strip the port and API path from the gateway URL to build the stratum0 URL
    # e.g. http://gateway.cern.ch:4929/api/v1 -> http://gateway.cern.ch/cvmfs/<name>
    local gw_scheme_host
    gw_scheme_host=$(echo "$gateway_url" | sed -E 's|(https?://[^:/]+).*|\1|')
    stratum0="${gw_scheme_host}/cvmfs/${name}"
  fi

  # default key location
  if [ x"$keys_import_location" = x"" ]; then
    keys_import_location="/etc/cvmfs/keys"
  fi

  # Check that the .gw key exists (always required — it's the secret)
  local gw_key="${keys_import_location}/${name}.gw"
  local pub_key="${keys_import_location}/${name}.pub"
  local crt_key="${keys_import_location}/${name}.crt"

  cvmfs_sys_file_is_regular "$gw_key" || \
    die "Gateway key not found: $gw_key
  Copy the .gw file from the gateway machine, or place it in /etc/cvmfs/keys/"

  # Auto-fetch .pub and .crt from the gateway if they are missing
  # (also triggered explicitly with -K)
  if [ $fetch_keys -eq 1 ] || \
     ! cvmfs_sys_file_is_regular "$pub_key" || \
     ! cvmfs_sys_file_is_regular "$crt_key"; then
    echo -n "Fetching repository keys from gateway... "

    # Read the .gw key to compute the HMAC for authentication
    local gw_key_id
    local gw_key_secret
    gw_key_id=$(cat "$gw_key" | awk '{print $2}')
    gw_key_secret=$(cat "$gw_key" | awk '{print $3}')
    [ x"$gw_key_id" != x"" ] && [ x"$gw_key_secret" != x"" ] || \
      die "fail! Could not parse gateway key file: $gw_key"

    # Compute HMAC of the URL path using the gateway secret
    local api_path="/api/v1/repos/${name}/keys"
    local hmac_hash
    hmac_hash=$(echo -n "$api_path" | openssl dgst -sha1 -hmac "$gw_key_secret" | awk '{print $NF}')
    local hmac_b64
    hmac_b64=$(echo -n "$hmac_hash" | base64)

    local keys_response
    keys_response=$(curl -sf -H "Authorization: ${gw_key_id} ${hmac_b64}" \
      "${gateway_url}/repos/${name}/keys" 2>&1) || \
      die "fail! Could not reach gateway key endpoint at ${gateway_url}/repos/${name}/keys
  Make sure the gateway has 'enable_key_endpoint: true' in its configuration."

    has_jq || die "fail! connect-gw requires 'jq' to parse the gateway response"

    local status
    status=$(echo "$keys_response" | jq -r '.status // empty' 2>/dev/null) || \
      die "fail! Could not parse gateway response"

    if [ x"$status" != x"ok" ]; then
      local reason
      reason=$(echo "$keys_response" | jq -r '.reason // "unknown"' 2>/dev/null)
      die "fail! Gateway returned error: $reason"
    fi

    # Decode and write the keys (the gateway returns them base64-encoded)
    mkdir -p "$keys_import_location"
    local ext
    local encoded
    for ext in pub crt; do
      encoded=$(echo "$keys_response" | jq -r ".data.${ext} // empty" 2>/dev/null)
      if [ x"$encoded" != x"" ]; then
        echo "$encoded" | base64 -d > "${keys_import_location}/${name}.${ext}" || \
          die "fail! Could not write key files"
      fi
    done
    echo "done"
    echo "  -> ${keys_import_location}/${name}.pub"
    echo "  -> ${keys_import_location}/${name}.crt"
  fi

  # Final check that all key files are present
  cvmfs_sys_file_is_regular "$pub_key" || \
    die "Public key not found: $pub_key
  Could not fetch from gateway. Copy the .pub file from the gateway machine manually."
  cvmfs_sys_file_is_regular "$crt_key" || \
    die "Certificate not found: $crt_key
  Could not fetch from gateway. Copy the .crt file from the gateway machine manually."

  # Build the upstream string for gateway mode
  local upstream="gw,/srv/cvmfs/${name}/data/txn,${gateway_url}"

  # Default owner
  if [ x"$owner" = x"" ]; then
    owner=$(whoami)
  fi

  echo "Connecting to gateway as publisher for ${name}"
  echo "  Gateway URL:  $gateway_url"
  echo "  Stratum 0:    $stratum0"
  echo "  Upstream:     $upstream"
  echo "  Keys:         $keys_import_location"
  echo "  Owner:        $owner"
  if [ $no_publisher -eq 1 ]; then
    echo "  Mountless:    yes (no FUSE mount, for use with mountless ingest)"
  fi
  echo

  # Delegate to mkfs with the right parameters
  local mkfs_flags=""
  if [ $no_publisher -eq 1 ]; then
    mkfs_flags="-P"
  fi
  cvmfs_server_mkfs $mkfs_flags \
                    -w "$stratum0" \
                    -u "$upstream" \
                    -k "$keys_import_location" \
                    -o "$owner" \
                    "$name"
}
