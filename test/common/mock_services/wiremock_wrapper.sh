#!/bin/sh

CVMFS_WIREMOCK_JAR_LOCATION="https://ecsft.cern.ch/dist/cvmfs/wiremock/wiremock-1.53-standalone.jar"

init_wiremock() {
  if ! which java > /dev/null 2>&1; then
    echo "Java is not installed! Wiremock cannot run."
    return 1
  fi

  which wiremock > /dev/null 2>&1 || install_wiremock
}

install_wiremock() {
  local jar_name="$(basename $CVMFS_WIREMOCK_JAR_LOCATION)"
  local jar_install_path="/usr/bin/${jar_name}"
  local wiremock_launcher="wiremock_launcher.sh"
  local wiremock_launcher_install_path="/usr/bin/wiremock"

  echo -n "Downloading wiremock... "
  wget --quiet                \
       --no-check-certificate \
       $CVMFS_WIREMOCK_JAR_LOCATION || { echo "fail"; return 1; }
  echo "done"

  echo -n "Installing wiremock... "
  sudo cp $jar_name $jar_install_path || { echo "fail"; return 2; }
  cat > $wiremock_launcher << EOF
# added by the CVMFS testing framework to conveniently start wiremock
java -jar $jar_install_path \$@
EOF
  sudo cp $wiremock_launcher $wiremock_launcher_install_path || { echo "fail"; return 4; }
  sudo chmod a+x $wiremock_launcher_install_path || { echo "fail"; return 5; }
  echo "done"
}

wait_for_local_port() {
  local port="$1"
  local initial_timeout=60
  local timeout=$initial_timeout

  echo -n "waiting for port ${port}... "
  while ! netstat -plnt 2>&1 | grep -q ":$port" && [ $timeout -gt 0 ]; do
    timeout=$(( $timeout - 1 ))
    sleep 1
  done

  if [ $timeout -eq 0 ]; then
    echo "fail (waited $initial_timeout seconds)"
    return 1
  else
    echo "okay"
    return 0
  fi
}

start_wiremock() {
  local document_root="$1"
  local port="$2"
  local logfile="$3"
  local is_proxy=""
  [ -z $4 ] || is_proxy="--enable-browser-proxying"
  run_background_service $logfile "wiremock              \
                                   --root $document_root \
                                   --port $port          \
                                   $is_proxy             \
                                   --verbose " > /dev/null
  wait_for_local_port $port
}

send_wiremock_admin_command() {
  local url="$1"
  local admin_command="$2"
  cat | curl -X POST "${url}/__admin/${admin_command}" -d @- \
            --header "Content-Type: application/json" > /dev/null 2>&1 || exit 1
}

shutdown_wiremock() {
  local url="$1"
  echo "" | send_wiremock_admin_command "$url" "shutdown"
}

alter_wiremock_settings() {
  local url="$1"
  cat | send_wiremock_admin_command "$url" "settings"
}

set_wiremock_response_delay() {
  local url="$1"
  local delay="$2"
  echo "{\"milliseconds\": $delay}" | send_wiremock_admin_command "$url" "socket-delay"
  # Workaround: the delay seems to take effect only after one request was posted
  curl $url > /dev/null 2>&1
}

set_wiremock_response_fault() {
  local url="$1"
  local url_pattern="$2"
  local fault_code="$3"

  echo "{                                      \
          \"request\": {                       \
            \"method\": \"GET\",               \
            \"urlPattern\": \"${url_pattern}\" \
          },                                   \
                                               \
          \"response\": {                      \
            \"fault\": \"${fault_code}\"       \
          }                                    \
        }" | send_wiremock_admin_command "$url" "mappings/new"
}

set_wiremock_empty_response_fault() {
  local url="$1"
  local url_pattern=".*"
  [ -z "$2" ] || url_pattern="$2"
  set_wiremock_response_fault "$url" "$url_pattern" "EMPTY_RESPONSE"
}

set_wiremock_garbage_response_fault() {
  local url="$1"
  local url_pattern=".*"
  [ -z "$2" ] || url_pattern="$2"
  set_wiremock_response_fault "$url" "$url_pattern" "MALFORMED_RESPONSE_CHUNK"
}

reset_wiremock() {
  local url="$1"
  echo "" | send_wiremock_admin_command "$url" "reset"
}
