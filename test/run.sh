#!/bin/bash

usage() {
  echo "$0 [logfile] [-o xUnit XML output] [-s suite labels] [-p profile]"
  echo "          [-x <exclusion list> --] [test list]"
  echo "  -- or --"
  echo "$0 [logfile] -s3 <S3 storage path>"
  echo ""
  echo "Profiles are shell fragments loaded from $(dirname "$0")/profiles/<name>.sh;"
  echo "they may set PROFILE_EXCLUSIONS, PROFILE_TESTSUITE, PROFILE_LABELS,"
  echo "PROFILE_CLASS_NAME and may define a profile_setup() hook."
}

git_source_revision() {
  local source_tree="$1"
  local git_root
  local git_revision

  if ! command -v git >/dev/null 2>&1; then
    return 0
  fi

  git_root="$(git -C "$source_tree" rev-parse --show-toplevel 2>/dev/null)" || return 0
  git_revision="$(git -C "$git_root" rev-parse --short=12 HEAD 2>/dev/null)" || return 0

  if [ -n "$(git -C "$git_root" status --porcelain 2>/dev/null)" ]; then
    git_revision="${git_revision}-dirty"
  fi

  printf '%s' "$git_revision"
}

export LC_ALL=C

SCRIPT_DIR=$(cd "$(dirname "$0")"; pwd)
CURRENT_DIR=$(pwd)
SOURCE_GIT_REVISION="$(git_source_revision "$SCRIPT_DIR")"

if [ "$CURRENT_DIR" != "$SCRIPT_DIR" ]; then
  echo "Warning: run.sh must be started from its parent directory: $SCRIPT_DIR"
  echo "Fatal: please cd $SCRIPT_DIR and run ./run.sh from there"
  exit 1
fi

# set up a log file
logfile_arg_consumed=0
default_logfile_used=0
case "${1:-}" in
  ''|-*)
    logfile="/tmp/cvmfs-test-logs/cvmfs-test-log-$(date +'%Y%m%d-%H%M%S').log"
    default_logfile_used=1
    if ! mkdir -p "$(dirname "$logfile")"; then
      echo "failed to create log directory $(dirname "$logfile")"
      exit 1
    fi
    ;;
  *)
    logfile=$1
    logfile_arg_consumed=1
    ;;
esac

if ! echo "$logfile" | grep -q ^/; then
  logfile=$(pwd)/$(basename $logfile)
fi

if [ "$logfile_arg_consumed" -eq 1 ]; then
  shift
fi

if [ "x$1" = "x-s3" ]; then
  s3_storage=$2
  if [ "x$s3_storage" = "x" ]; then
    usage
    exit 1
  fi
  if [ -d $s3_storage ]; then
    echo "$s3_storage already exists, remove before starting S3 server"
    exit 1
  fi
  echo "Starting S3 test server" > $logfile
  if [ -n "$SOURCE_GIT_REVISION" ]; then
    echo "Source tree git revision: $SOURCE_GIT_REVISION" >> $logfile
  fi
  mkdir -p $s3_storage/{config,mc_config,data}

  s3_config=$s3_storage/test_s3.conf
  tee $s3_config >> $logfile << EOF
CVMFS_S3_HOST=localhost
CVMFS_S3_PORT=13337
CVMFS_S3_ACCESS_KEY=not
CVMFS_S3_SECRET_KEY=important
CVMFS_S3_BUCKETS_PER_ACCOUNT=1
CVMFS_S3_DNS_BUCKETS=false
CVMFS_S3_MAX_NUMBER_OF_PARALLEL_CONNECTIONS=10
CVMFS_S3_BUCKET=testbucket
EOF
  tee $s3_storage/config/config.json >> $logfile << EOF
{
	"version": "23",
	"credential": {
		"accessKey": "not",
		"secretKey": "important"
	}
}
EOF
  tee $s3_storage/mc_config/config.json >> $logfile << EOF
{
	"version": "9",
	"hosts": {
		"cvmfs": {
			"url": "http://localhost:13337",
			"accessKey": "not",
			"secretKey": "important",
			"api": "S3v4",
			"lookup": "auto"
		}
	}
}
EOF
  echo "********** Environment for starting S3 integration tests **************"
  echo "export CVMFS_TEST_S3_CONFIG=$s3_config"
  echo "export CVMFS_TEST_HTTP_BASE=http://localhost:13337/cvmfstest"
  echo "export CVMFS_TEST_S3_STORAGE=$s3_storage/data/cvmfstest"
  echo "***********************************************************************"
  /usr/local/bin/minio server --address :13337 --config-dir $s3_storage/config $s3_storage/data
  exit 0
fi

echo "Start test suite for cvmfs $(cvmfs2 --version)" > $logfile
date >> $logfile
if [ -n "$SOURCE_GIT_REVISION" ]; then
  echo "Source tree git revision: $SOURCE_GIT_REVISION" >> $logfile
fi

# read command line parameters
test_exclusions=0
xml_output=""
debug=""
labels="$CVMFS_TEST_SUITES"
suite_option_provided=0
default_suite_used=0
default_testsuite_used=0
profile_name=""
while getopts "xo:ds:p:" option; do
  case $option in
    x)
      test_exclusions=1
    ;;
    o)
      xml_output="$OPTARG"
    ;;
    d)
      debug="-x"
    ;;
    s)
      labels="$OPTARG"
      suite_option_provided=1
    ;;
    p)
      profile_name="$OPTARG"
    ;;
    ?)
      usage
      exit 1
    ;;
  esac
done
shift $(( $OPTIND - 1 ))

# load an optional test configuration profile
PROFILE_EXCLUSIONS=""
PROFILE_TESTSUITE=""
PROFILE_LABELS=""
PROFILE_CLASS_NAME=""
if [ -n "$profile_name" ]; then
  profile_file="$SCRIPT_DIR/profiles/${profile_name}.sh"
  if [ ! -f "$profile_file" ]; then
    echo "Error: profile '$profile_name' not found at $profile_file"
    exit 1
  fi
  # shellcheck disable=SC1090
  . "$profile_file"
  echo "Using test profile: $profile_name"
  echo "Using test profile: $profile_name" >> $logfile
  if command -v profile_setup >/dev/null 2>&1; then
    if ! profile_setup >> $logfile 2>&1; then
      echo "profile_setup for '$profile_name' failed; see $logfile"
      exit 1
    fi
  fi
  if [ -n "$PROFILE_CLASS_NAME" ] && [ -z "$CVMFS_TEST_CLASS_NAME" ]; then
    export CVMFS_TEST_CLASS_NAME="$PROFILE_CLASS_NAME"
  fi
  if [ -n "$PROFILE_LABELS" ] && [ -z "$labels" ] && [ "$suite_option_provided" -eq 0 ]; then
    labels="$PROFILE_LABELS"
  fi
fi

# configure the exclusion list of tests
exclusions="$CVMFS_TEST_EXCLUDE $PROFILE_EXCLUSIONS"
if [ $test_exclusions -ne 0 ]; then
  while [ $# -ne 0 ] && [ x"$1" != x"--" ]; do
    exclusions="$exclusions $1"
    shift
  done
  shift # get rid of '--'
fi

# Optionally shuffle the order of the explicitly listed test cases. This is off
# by default so that foundational tests (e.g. 000-dummy, 001-chksetup) run first;
# set CVMFS_TEST_SHUFFLE=1 to stress-test that tests have no ordering dependency.
if [ -n "$CVMFS_TEST_SHUFFLE" ] && [ "$CVMFS_TEST_SHUFFLE" != "0" ]; then
  testsuite=$(shuf -e $@)
else
  testsuite="$@"
fi
if [ -z "$testsuite" ]; then
  if [ "$suite_option_provided" -eq 0 ] && [ -z "$labels" ]; then
    labels="quick"
    default_suite_used=1
  fi

  if [ -n "$PROFILE_TESTSUITE" ]; then
    testsuite="$PROFILE_TESTSUITE"
    default_testsuite_used=1
  elif [ "$suite_option_provided" -eq 0 ]; then
    testsuite="src/0* src/1* src/5* src/6*"
    default_testsuite_used=1
  else
    testsuite=$(find src -mindepth 1 -maxdepth 1 -type d | sort)
  fi
fi

default_cli_parameters=""
if [ "$default_logfile_used" -eq 1 ]; then
  default_cli_parameters="logfile=$logfile"
fi
if [ "$default_suite_used" -eq 1 ]; then
  if [ -n "$default_cli_parameters" ]; then
    default_cli_parameters="$default_cli_parameters; "
  fi
  default_cli_parameters="${default_cli_parameters}-s quick"
fi
if [ "$default_testsuite_used" -eq 1 ]; then
  if [ -n "$default_cli_parameters" ]; then
    default_cli_parameters="$default_cli_parameters; "
  fi
  default_cli_parameters="${default_cli_parameters}-- src/0* src/1* src/5* src/6*"
fi

if [ "x$labels" != "x" ]; then
  echo "Limiting test cases to suite(s): $labels"
fi

echo "Starting CernVM-FS integration test run at $(date)"
echo "Logging test output to $logfile"
if [ -n "$SOURCE_GIT_REVISION" ]; then
  echo "Running from git revision $SOURCE_GIT_REVISION"
fi
if [ -n "$default_cli_parameters" ]; then
  echo "Defaulted CLI parameters for this run: $default_cli_parameters"
fi

# start running the tests
TEST_ROOT="$SCRIPT_DIR"
export TEST_ROOT

num_tests=0
num_skipped=0
num_passed=0
num_failures=0
num_warnings=0

TEST_ID_WIDTH=24
TEST_STATUS_WIDTH=6
TEST_TIMESTAMP_WIDTH=6
STATUS_LAYOUT_MIN_COLUMNS=100
STATUS_LAYOUT="aligned"
STATUS_COLOR_ENABLED=0

COLOR_RESET=""
COLOR_PASS=""
COLOR_WARN=""
COLOR_FAIL=""
COLOR_SKIP=""
COLOR_HEADING=""

# test failure handling
# this functions should EXCLUSIVELY be called in the run loop (global state)
report_failure() {
  local message="$1"
  local workdir=$2
  echo $message
  if [ x$workdir != x ]; then
    if [ x$PARROT_ENABLED = "xTRUE" ]; then
      cp $CVMFS_TEST_SYSLOG_TARGET $workdir
    else
      sudo cp $CVMFS_TEST_SYSLOG_TARGET $workdir
    fi
  fi
  num_failures=$(($num_failures+1))
}
report_warning() {
  local message="$1"
  echo $message
  num_warnings=$(($num_warnings+1))
}
report_passed() {
  local message="$1"
  echo $message
  num_passed=$(($num_passed+1))
}
report_skipped() {
  local message="$1"
  echo $message
  num_skipped=$(($num_skipped+1))
}

test_display_id() {
  local test_path="$1"
  local display_id="${test_path#./}"

  display_id="${display_id#test/}"
  display_id="${display_id#src/}"

  echo "$display_id"
}

test_elapsed_display() {
  local elapsed_ms="$1"
  local elapsed_tenths=$(( (elapsed_ms + 50) / 100 ))
  local elapsed_seconds=$(( elapsed_tenths / 10 ))
  local elapsed_fraction=$(( elapsed_tenths % 10 ))

  printf '%3d.%1ds' "$elapsed_seconds" "$elapsed_fraction"
}

is_positive_integer() {
  case "$1" in
    ''|*[!0-9]*) return 1 ;;
    0) return 1 ;;
    *) return 0 ;;
  esac
}

terminal_columns() {
  local columns

  if is_positive_integer "$COLUMNS"; then
    echo "$COLUMNS"
    return
  fi

  if [ -t 1 ] && command -v tput >/dev/null 2>&1; then
    columns="$(tput cols 2>/dev/null)"
    if is_positive_integer "$columns"; then
      echo "$columns"
      return
    fi
  fi

  echo 120
}

configure_status_layout() {
  local columns

  columns="$(terminal_columns)"
  if [ "$columns" -lt "$STATUS_LAYOUT_MIN_COLUMNS" ]; then
    STATUS_LAYOUT="compact"
  else
    STATUS_LAYOUT="aligned"
  fi
}

configure_status_colors() {
  if [ ! -t 1 ]; then
    return
  fi

  if [ -n "$NO_COLOR" ]; then
    return
  fi

  case "${TERM:-}" in
    ''|dumb)
      return
      ;;
  esac

  STATUS_COLOR_ENABLED=1
  COLOR_RESET=$'\033[0m'
  COLOR_PASS=$'\033[32m'
  COLOR_WARN=$'\033[33m'
  COLOR_FAIL=$'\033[31m'
  COLOR_SKIP=$'\033[36m'
  COLOR_HEADING=$'\033[1m'
}

status_color() {
  case "$1" in
    PASS)
      printf '%s' "$COLOR_PASS"
      ;;
    WARN)
      printf '%s' "$COLOR_WARN"
      ;;
    FAIL)
      printf '%s' "$COLOR_FAIL"
      ;;
    SKIP)
      printf '%s' "$COLOR_SKIP"
      ;;
  esac
}

format_status_label() {
  local status="$1"
  local color

  if [ "$STATUS_COLOR_ENABLED" -eq 0 ]; then
    printf '%s' "$status"
    return
  fi

  color="$(status_color "$status")"
  if [ -z "$color" ]; then
    printf '%s' "$status"
  else
    printf '%s%s%s' "$color" "$status" "$COLOR_RESET"
  fi
}

format_section_heading() {
  local title="$1"

  if [ "$STATUS_COLOR_ENABLED" -eq 0 ] || [ -z "$COLOR_HEADING" ]; then
    printf '%s' "$title"
  else
    printf '%s%s%s' "$COLOR_HEADING" "$title" "$COLOR_RESET"
  fi
}

format_status_entry_prefix() {
  local test_id="$1"
  local test_name="$2"
  local test_padding=""
  local test_padding_width=0

  : "$test_name"

  if [ "$STATUS_LAYOUT" = "compact" ]; then
    printf "%s" "$test_id"
  else
    test_padding_width=$((TEST_ID_WIDTH - ${#test_id} - 1))
    if [ "$test_padding_width" -gt 0 ]; then
      printf -v test_padding '%*s' "$test_padding_width" ''
      test_padding="${test_padding// /.}"
    fi

    printf "%s %s " "$test_id" "$test_padding"
  fi
}

format_status_entry_suffix() {
  local status="$1"
  local timestamp="$2"
  local detail="$3"
  local status_label

  status_label="$(format_status_label "$status")"

  if [ "$STATUS_LAYOUT" = "compact" ]; then
    printf " | %s" "$status_label"
  else
    printf "%s" "$status_label"
  fi

  if [ -n "$timestamp" ] || [ -n "$detail" ]; then
    printf " | %${TEST_TIMESTAMP_WIDTH}s" "$timestamp"
  fi

  if [ -n "$detail" ]; then
    printf " | %s" "$detail"
  fi
}

format_status_entry() {
  local test_id="$1"
  local test_name="$2"
  local status="$3"
  local timestamp="$4"
  local detail="$5"

  format_status_entry_prefix "$test_id" "$test_name"
  format_status_entry_suffix "$status" "$timestamp" "$detail"
}

append_preview_skip_entry() {
  local preview_var="$1"
  local test_id="$2"
  local preview_entry

  preview_entry="$(format_status_entry "$test_id" "" "SKIP" "" "")"
  printf -v "$preview_var" '%s%s\n' "${!preview_var}" "$preview_entry"
}

print_test_status() {
  local test_id="$1"
  local test_name="$2"
  local status="$3"
  local timestamp="$4"
  local detail="$5"

  format_status_entry "$test_id" "$test_name" "$status" "$timestamp" "$detail"
  printf "\n"
}

print_test_status_prefix() {
  local test_id="$1"
  local test_name="$2"

  format_status_entry_prefix "$test_id" "$test_name"
}

print_test_status_suffix() {
  local status="$1"
  local timestamp="$2"
  local detail="$3"

  format_status_entry_suffix "$status" "$timestamp" "$detail"
  printf "\n"
}

append_status_digest() {
  local digest_var="$1"
  local test_id="$2"
  local test_name="$3"
  local status="$4"
  local timestamp="$5"
  local detail="$6"
  local digest_entry

  digest_entry="$(format_status_entry "$test_id" "$test_name" "$status" "$timestamp" "$detail")"

  printf -v "$digest_var" '%s%s\n' "${!digest_var}" "$digest_entry"
}

print_digest_section() {
  local title="$1"
  local digest_content="$2"
  local empty_message="$3"

  echo ""
  printf '%s\n' "$(format_section_heading "$title")"
  if [ -n "$digest_content" ]; then
    printf "%s" "$digest_content"
  elif [ -n "$empty_message" ]; then
    echo "$empty_message"
  fi
}

run_test_with_timeout() {
  local test_path="$1"
  local test_workdir="$2"
  local test_logfile="$3"
  local test_timeout="$4"
  local test_root="$5"

  timeout --foreground -s HUP "$test_timeout" \
    bash $debug -c '
      set -m
      test_tree_pid=""

      stop_test_tree() {
        local signal="$1"

        if [ -z "$test_tree_pid" ]; then
          return 0
        fi

        kill -s "$signal" -- "-$test_tree_pid" 2>/dev/null || true
        sleep 1
        kill -KILL -- "-$test_tree_pid" 2>/dev/null || true
      }

      trap '\''stop_test_tree HUP; wait "$test_tree_pid" 2>/dev/null || true; exit 124'\'' HUP
      trap '\''stop_test_tree INT; wait "$test_tree_pid" 2>/dev/null || true; exit 130'\'' INT
      trap '\''stop_test_tree TERM; wait "$test_tree_pid" 2>/dev/null || true; exit 143'\'' TERM

      bash -c '\''
        set -e &&
        set -o pipefail &&
        set -x &&
        set -u &&
        cd "$4" &&
        . ./test_functions &&
        . "$1/main" &&
        cd "$2" &&
        cvmfs_run_test "$3" "$4/$1"
        retval=$?
        mangle_test_retval "$retval"
        exit $?
      '\'' run_test_child "$1" "$2" "$3" "$4" &
      test_tree_pid=$!
      wait "$test_tree_pid"
      exit $?
    ' run_test_wrapper "$test_path" "$test_workdir" "$test_logfile" "$test_root" >> "$test_logfile" 2>&1
}

clean_workdir() {
  if [ x$PARROT_ENABLED = "xTRUE" ]; then
    rm -rf "$workdir" >> $logfile
  else
    sudo rm -rf "$workdir" >> $logfile
  fi
}

# makes sure the test environment for the test case is sane
setup_environment() {
  local autofs_demand=$1
  local workdir=$2

  # make sure the environment is clean
  local clean_retval=0
  cvmfs_clean || clean_retval=$?
  if [ $clean_retval -ne 0 ]; then
    echo "failed to clean environment (retval: $clean_retval)"
    return 101
  fi

  # create a workspace for the test case
  rm -rf "$workdir" && mkdir -p "$workdir"
  if [ $? -ne 0 ]; then
    echo "failed to create test working directory"
    return 102
  fi

  # if we are not inside a docker
  if [ x"$CVMFS_TEST_DOCKER" = xno ] && ! running_on_osx; then
    # configure autofs to the test's needs
    if $autofs_demand; then
      service_switch autofs restart || true
      local timeout=10 # wait until autofs restarts (possible race >.<)
      while [ $timeout -gt 0 ] && ! autofs_check; do
        timeout=$(( $timeout - 1))
        sleep 1
      done
      if [ $timeout -eq 0 ]; then
        echo "failed to restart autofs"
        return 103
      fi
    else
      if ! autofs_switch off; then
        echo "failed to switch off autofs"
        return 104
      fi
    fi
  fi

  # if the test is a benchmark we have to configure the environment
  if [ x"$cvmfs_benchmark" = x"yes" ]; then
    setup_benchmark_environment $workdir
  fi

  # Avoid issues with modern Apache reload logic
  echo "# Created by CVMFS integration tests" | sudo tee /etc/cvmfs/cvmfs_server_hooks.sh \
    || return  105
  echo "CVMFS_SERVER_APACHE_RELOAD_IS_RESTART=$CVMFS_SERVER_APACHE_RELOAD_IS_RESTART" | \
    sudo tee -a /etc/cvmfs/cvmfs_server_hooks.sh || return 105

  # reset the test warning flags
  reset_test_warning_flags

  return 0
}

# source common functions used in the test cases
. ./test_functions

configure_status_layout
configure_status_colors

testsuite_start="$(get_millisecond_epoch)"

workdir_basedir="${CVMFS_TEST_SCRATCH}/workdir"
scratch_basedir="${CVMFS_TEST_SCRATCH}/scratch"
[ ! -d $scratch_basedir ] || rm -fR $scratch_basedir
mkdir -p $scratch_basedir

preview_run_count=0
preview_suite_skip_count=0
preview_excluded_count=0
preview_suite_skip_entries=""
preview_excluded_entries=""
issue_digest_entries=""

get_iso8601_timestamp > ${scratch_basedir}/starttime

to_syslog "Test Suite started"

for t in $testsuite
do
  TEST_DISPLAY_ID="$(test_display_id "$t")"
  if contains "$exclusions" $t; then
    preview_excluded_count=$((preview_excluded_count+1))
    append_preview_skip_entry preview_excluded_entries "$TEST_DISPLAY_ID"
    continue
  fi

  if ! is_in_suite $t $labels; then
    preview_suite_skip_count=$((preview_suite_skip_count+1))
    append_preview_skip_entry preview_suite_skip_entries "$TEST_DISPLAY_ID"
    continue
  fi

  preview_run_count=$((preview_run_count+1))
done

if [ "$preview_run_count" -gt 0 ] || [ "$preview_suite_skip_count" -gt 0 ] || [ "$preview_excluded_count" -gt 0 ]; then
  echo ""
  printf '%s\n' "$(format_section_heading "Selection preview:")"
  echo "Will run $preview_run_count tests."
  if [ "$preview_suite_skip_count" -gt 0 ]; then
    printf '%s\n' "$(format_section_heading "Excluded by suite (-s) ($preview_suite_skip_count):")"
    printf "%s" "$preview_suite_skip_entries"
  fi
  if [ "$preview_excluded_count" -gt 0 ]; then
    printf '%s\n' "$(format_section_heading "Excluded by -x ($preview_excluded_count):")"
    printf "%s" "$preview_excluded_entries"
    printf "\n"
  fi
fi

# run the tests
for t in $testsuite
do
  # count the tests
  num_tests=$(($num_tests+1))

  # add some whitespace between tests in the log
  i=0
  while [ $i -lt 10 ]; do
    echo "" >> $logfile
    i=$(($i+1))
  done

  # source the test code
  TEST_DISPLAY_ID="$(test_display_id "$t")"
  workdir="${workdir_basedir}/$(basename $t)"
  scratchdir="${scratch_basedir}/$(basename $t)"

  if ! mkdir -p $scratchdir; then
    report_failure "failed to create $scratchdir" >> $logfile
    print_test_status "$TEST_DISPLAY_ID" "$(basename "$t")" "FAIL" "" "failed to create scratchdir"
    append_status_digest issue_digest_entries "$TEST_DISPLAY_ID" "$(basename "$t")" "FAIL" "" "failed to create scratchdir"
    continue
  fi

  # Allow non-regular files e.g. /dev/stdout to be used as logfile.
  # Don't expect the possibility to read it back.
  if [[ -f "$logfile" ]]; then
    wc -l < "$logfile" > ${scratchdir}/log_begin
  fi

  cvmfs_test_autofs_on_startup=true # might be overwritten by some tests
  if ! . $t/main; then
    report_failure "failed to source $t/main" >> $logfile
    print_test_status "$TEST_DISPLAY_ID" "$(basename "$t")" "FAIL" "" "failed to source main"
    echo "101" > ${scratchdir}/retval
    append_status_digest issue_digest_entries "$TEST_DISPLAY_ID" "$(basename "$t")" "FAIL" "" "retval 101, failed to source main"
    continue
  fi

  # write some status info to the screen
  TEST_NR="$(basename $t | head -c3)"
  to_syslog "Test $TEST_NR (${cvmfs_test_name}) started"
  echo "-- Testing ${cvmfs_test_name} ($(date) / test number $TEST_NR)" >> $logfile
  echo "$cvmfs_test_name"          > ${scratchdir}/name
  echo "$(basename $t | head -c3)" > ${scratchdir}/number

  # check if test should be skipped
  if contains "$exclusions" $t; then
    report_skipped "test case was marked to be skipped" >> $logfile
    touch ${scratchdir}/skipped
    continue
  fi

  if ! is_in_suite $t $labels; then
    report_skipped "test case not part of selected suites" >> $logfile
    touch ${scratchdir}/skipped
    continue
  fi

  cvmfs_test_timeout=
  cvmfs_test_timeout=$(. $t/main && echo $cvmfs_test_timeout)
  if [ x"$cvmfs_test_timeout" = x ]; then
    if is_in_suite $t quick; then
      # Default 5min timeout for quick tests
      cvmfs_test_timeout=300
    else
      cvmfs_test_timeout=0
    fi
  fi

  # configure the environment for the test
  setup_retval=0
  setup_environment $cvmfs_test_autofs_on_startup $workdir >> $logfile 2>&1 || setup_retval=$?
  if [ $setup_retval -ne 0 ]; then
    to_syslog "Test $TEST_NR (${cvmfs_test_name}) failed (setup)"
    report_failure "failed to setup environment (retval: $setup_retval)" >> $logfile
    print_test_status "$TEST_DISPLAY_ID" "$cvmfs_test_name" "FAIL" "" "setup retval $setup_retval"
    echo "0"         > ${scratchdir}/elapsed
    echo "102"       > ${scratchdir}/retval
    # Allow non-regular files e.g. /dev/stdout to be used as logfile.
    # Don't expect the possibility to read it back.
    if [[ -f "$logfile" ]]; then
      wc -l < "$logfile" > ${scratchdir}/log_end
    fi
    touch              ${scratchdir}/failure
    append_status_digest issue_digest_entries "$TEST_DISPLAY_ID" "$cvmfs_test_name" "FAIL" "" "retval 102, setup retval $setup_retval"
    continue
  fi

  # run the test
  print_test_status_prefix "$TEST_DISPLAY_ID" "$cvmfs_test_name"
  test_start=$(get_millisecond_epoch)
  run_test_with_timeout "$t" "$workdir" "$logfile" "$cvmfs_test_timeout" "$TEST_ROOT"
  RETVAL=$?
  if [ $RETVAL -eq 130 ] || [ $RETVAL -eq 143 ]; then
    printf "\n"
    to_syslog "Test Suite interrupted"
    exit $RETVAL
  fi
  if [ $RETVAL -eq 124 ]; then
    RETVAL=$CVMFS_TEST_RETVAL_TIMEOUT
  fi
  test_end=$(get_millisecond_epoch)
  test_time_elapsed=$(( ( $test_end - $test_start ) ))
  TEST_ELAPSED_DISPLAY="$(test_elapsed_display "$test_time_elapsed")"
  echo "execution took $(milliseconds_to_seconds $test_time_elapsed) seconds" >> $logfile
  echo "$test_time_elapsed" > ${scratchdir}/elapsed
  echo "$RETVAL"            > ${scratchdir}/retval

  # if the test is a benchmark we have to collect the results before removing the folder
  if [ x"$cvmfs_benchmark" = x"yes" ]; then
    collect_benchmark_results
    cvmfs_umount $FQRN > /dev/null 2>&1
  fi

  # check the final test result
  case $RETVAL in
    0)
      clean_workdir
      to_syslog "Test $TEST_NR (${cvmfs_test_name}) finished successfully"
      report_passed "Test passed" >> $logfile
      touch ${scratchdir}/success
      print_test_status_suffix "PASS" "$TEST_ELAPSED_DISPLAY" ""
      ;;
    $CVMFS_MEMORY_WARNING)
      clean_workdir
      to_syslog "Test $TEST_NR (${cvmfs_test_name}) finished with memory warning"
      report_warning "Memory limit exceeded!" >> $logfile
      touch ${scratchdir}/memorywarning
      print_test_status_suffix "WARN" "$TEST_ELAPSED_DISPLAY" "memory limit exceeded"
      append_status_digest issue_digest_entries "$TEST_DISPLAY_ID" "$cvmfs_test_name" "WARN" "$TEST_ELAPSED_DISPLAY" "memory limit exceeded"
      ;;
    $CVMFS_TIME_WARNING)
      clean_workdir
      to_syslog "Test $TEST_NR (${cvmfs_test_name}) finished with time warning"
      report_warning "Time limit exceeded!" >> $logfile
      tail -n 50 /var/log/messages /var/log.syslog >> $logfile 2>/dev/null
      touch ${scratchdir}/timewarning
      print_test_status_suffix "WARN" "$TEST_ELAPSED_DISPLAY" "time limit exceeded"
      append_status_digest issue_digest_entries "$TEST_DISPLAY_ID" "$cvmfs_test_name" "WARN" "$TEST_ELAPSED_DISPLAY" "time limit exceeded"
      ;;
    $CVMFS_GENERAL_WARNING)
      clean_workdir
      to_syslog "Test $TEST_NR (${cvmfs_test_name}) finished with warning"
      report_warning "Test case finished with warnings!" >> $logfile
      touch ${scratchdir}/generalwarning
      print_test_status_suffix "WARN" "$TEST_ELAPSED_DISPLAY" "warning"
      append_status_digest issue_digest_entries "$TEST_DISPLAY_ID" "$cvmfs_test_name" "WARN" "$TEST_ELAPSED_DISPLAY" "warning"
      ;;
    $CVMFS_TEST_RETVAL_TIMEOUT)
      to_syslog "Test $TEST_NR (${cvmfs_test_name}) timed out"
      report_failure "Testcase timed out" $workdir >> $logfile
      touch ${scratchdir}/failure
      print_test_status_suffix "FAIL" "$TEST_ELAPSED_DISPLAY" "timeout"
      append_status_digest issue_digest_entries "$TEST_DISPLAY_ID" "$cvmfs_test_name" "FAIL" "$TEST_ELAPSED_DISPLAY" "retval $CVMFS_TEST_RETVAL_TIMEOUT (timeout)"
      ;;
    *)
      to_syslog "Test $TEST_NR (${cvmfs_test_name}) failed"
      report_failure "Testcase failed with RETVAL $RETVAL" $workdir >> $logfile
      tail -n 50 /var/log/messages /var/log.syslog >> $logfile 2>/dev/null
      touch ${scratchdir}/failure
      print_test_status_suffix "FAIL" "$TEST_ELAPSED_DISPLAY" "retval $RETVAL"
      append_status_digest issue_digest_entries "$TEST_DISPLAY_ID" "$cvmfs_test_name" "FAIL" "$TEST_ELAPSED_DISPLAY" "retval $RETVAL"
      ;;
  esac

  # Allow non-regular files e.g. /dev/stdout to be used as logfile.
  # Don't expect the possibility to read it back.
  if [[ -f "$logfile" ]]; then
    wc -l < "$logfile" > ${scratchdir}/log_end
  fi
done

testsuite_end="$(get_millisecond_epoch)"
testsuite_time_elapsed=$(( $testsuite_end - $testsuite_start ))

echo "$testsuite_time_elapsed" > ${scratch_basedir}/elapsed
echo "$num_tests"              > ${scratch_basedir}/num_tests
echo "$num_skipped"            > ${scratch_basedir}/num_skipped
echo "$num_passed"             > ${scratch_basedir}/num_passed
echo "$num_warnings"           > ${scratch_basedir}/num_warnings
echo "$num_failures"           > ${scratch_basedir}/num_failures

# export xunit XML
if [ ! -z "$xml_output" ]; then
  export_xunit_xml "$xml_output" $scratch_basedir $logfile
fi

# remove runtime information
# rm -rf $scratch_basedir

# print final status information
date >> $logfile
echo "Finished test suite in $(milliseconds_to_human_readable $testsuite_time_elapsed)" >> $logfile

echo ""
echo "Tests:    $num_tests"
echo "Skipped:  $num_skipped"
echo "Passed:   $num_passed"
echo "Warnings: $num_warnings"
echo "Failures: $num_failures"
echo ""
echo "took $(milliseconds_to_human_readable $testsuite_time_elapsed)"

if [ -n "$issue_digest_entries" ]; then
  print_digest_section "Warnings / failures digest:" "$issue_digest_entries" ""
elif [ "$num_warnings" -eq 0 ] && [ "$num_failures" -eq 0 ]; then
  echo ""
  printf '%s\n' "$(format_section_heading "All executed tests ran successfully.")"
fi

to_syslog "Test Suite finished"

exit $num_failures

