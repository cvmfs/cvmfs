#!/bin/bash -u

HTTP_HEADER='FALSE'

TMPFILE=$(mktemp)

cleanup_tmpfile() {
    if [ -n "${TMPFILE}" ] && [ -f "${TMPFILE}" ]; then
        rm -f "${TMPFILE}"
    fi
}
trap cleanup_tmpfile EXIT

# CVMFS Extended Attributes and their descriptions
declare -A CVMFS_EXTENDED_ATTRIBUTE_GAGUES=(
    ['hitrate']='CVMFS cache hit rate (%)'
    ['inode_max']='Shows the highest possible inode with the current set of loaded catalogs.'
    ['maxfd']='Shows the maximum number of file descriptors available to file system clients.'
    ['ncleanup24']='Shows the number of cache cleanups in the last 24 hours.'
    ['nclg']='Shows the number of currently loaded nested catalogs.'
    ['ndiropen']='Shows the overall number of opened directories.'
    ['pid']='Shows the process id of the CernVM-FS Fuse process.'
    ['speed']='Shows the average download speed.'
    ['useddirp']='Shows the number of file descriptors currently issued to file system clients.'
    ['usedfd']='Shows the number of open directories currently used by file system clients.'
)

#############################################################
usage() {
    echo "Usage: $0 [--help] [--http]" >&2
    echo '' >&2
    echo '  --http: add the HTTP protocol header to the output' >&2
    echo '' >&2
    echo 'NOTE: The user running this script must have read access' >&2
    echo '      to the CVMFS cache' >&2
    exit 1
}

generate_metric() {
    local metric_name="$1"
    local metric_type="$2"
    local help_text="$3"
    local metric_labels="$4"
    local metric_value="$5"

    cat >>"${TMPFILE}" <<EOF
# HELP $metric_name $help_text
# TYPE $metric_name $metric_type
${metric_name}{${metric_labels}} ${metric_value}
EOF
}

list_mounted_cvmfs_repos() {
    cvmfs_config status | tr -s '[:space:]' | cut -d ' ' -f 1 | sort -u
}

mountpoint_for_cvmfs_repo() {
    local reponame
    reponame="$1"

    cvmfs_talk -i "${reponame}" mountpoint
}

fqrn_for_cvmfs_repo() {
    local reponame
    reponame="$1"

    local repopath
    repopath=$(mountpoint_for_cvmfs_repo "${reponame}")

    attr -g fqrn "${repopath}" | tail -n +2
}

get_cvmfs_repo_extended_attribute_gague_metrics() {
    local reponame
    reponame="$1"

    local repomountpoint
    repomountpoint=$(mountpoint_for_cvmfs_repo "${reponame}")

    local fqrn
    fqrn=$(fqrn_for_cvmfs_repo "${reponame}")

    local attribute
    for attribute in "${!CVMFS_EXTENDED_ATTRIBUTE_GAGUES[@]}"; do
        local result
        result=$(attr -g "${attribute}" "${repomountpoint}" | tail -n +2)
        generate_metric "cvmfs_${attribute}" 'gague' "${CVMFS_EXTENDED_ATTRIBUTE_GAGUES[${attribute}]}" "repo=${fqrn}" "${result}"
    done
}

get_cvmfs_repo_proxy_metrics() {
    local reponame
    reponame="$1"

    local repomountpoint
    repomountpoint=$(mountpoint_for_cvmfs_repo "${reponame}")

    local fqrn
    fqrn=$(fqrn_for_cvmfs_repo "${reponame}")

    local proxy_list
    mapfile -t proxy_list < <(attr -g proxy_list "${repomountpoint}" | tail -n +2 | grep -v '^$')

    local proxy_filter_by_group
    mapfile -t proxy_filter_by_group < <(cvmfs_talk -i "${reponame}" proxy info | tail -n +2 | grep '^\[' | grep ']' | tr -s '[:space:]')

    local proxy
    local my_proxy_group
    for proxy in "${proxy_list[@]}"; do
        local line
        local result
        for line in "${proxy_filter_by_group[@]}"; do
            result=$(echo "${line}" | grep "${proxy}" | cut -d' ' -f 1 | tr -d '][')
            if [[ "x${result}" != 'x' ]]; then
                my_proxy_group=${result}
                break
            fi
        done
        generate_metric "cvmfs_proxy" "gague" "Shows all registered proxies for this repository." "repo=${fqrn},group=${my_proxy_group},url=${proxy}" 1
    done
}

get_cvmfs_repo_metrics() {
    local reponame
    reponame="$1"

    local repomountpoint
    repomountpoint=$(mountpoint_for_cvmfs_repo "${reponame}")

    local fqrn
    fqrn=$(fqrn_for_cvmfs_repo "${reponame}")

    local repo_pid
    repo_pid=$(cvmfs_talk -i "${reponame}" pid)

    local cache_volume
    cache_volume=$(cvmfs_talk -i "${reponame}" parameters | grep CVMFS_CACHE_BASE | tr '=' ' ' | tr -s '[:space:]' | cut -d ' ' -f 2)

    local cached_bytes
    cached_bytes=$(cvmfs_talk -i "${reponame}" cache size | tr -d ')(' | tr -s '[:space:]' | cut -d ' ' -f 6)
    generate_metric 'cvmfs_cached_bytes' 'gauge' 'CVMFS currently cached bytes.' "repo=${fqrn}" "${cached_bytes}"

    local pinned_bytes
    pinned_bytes=$(cvmfs_talk -i "${reponame}" cache size | tr -d ')(' | tr -s '[:space:]' | cut -d ' ' -f 10)
    generate_metric 'cvmfs_pinned_bytes' 'gauge' 'CVMFS currently pinned bytes.' "repo=${fqrn}" "${pinned_bytes}"

    local total_cache_size_mb
    total_cache_size_mb=$(cvmfs_talk -i "${reponame}" parameters | grep CVMFS_QUOTA_LIMIT | tr '=' ' ' | tr -s '[:space:]' | cut -d ' ' -f 2)
    local total_cache_size
    total_cache_size=$((total_cache_size_mb * 1024 * 1024))
    generate_metric 'cvmfs_total_cache_size_bytes' 'gague' 'CVMFS configured cache size via CVMFS_QUOTA_LIMIT.' "repo=${fqrn}" "${total_cache_size}"

    local cache_volume_max
    cache_volume_max=$(df -B1 "${cache_volume}" | tail -n 1 | tr -s '[:space:]' | cut -d ' ' -f 2)
    generate_metric 'cvmfs_physical_cache_size_bytes' 'gague' 'CVMFS cache volume physical size.' "repo=${fqrn}" "${cache_volume_max}"

    local cache_volume_free
    cache_volume_free=$(df -B1 "${cache_volume}" | tail -n 1 | tr -s '[:space:]' | cut -d ' ' -f 4)
    generate_metric 'cvmfs_physical_cache_avail_bytes' 'gague' 'CVMFS cache volume physical free space available.' "repo=${fqrn}" "${cache_volume_free}"

    local cvmfs_mount_version
    cvmfs_mount_version=$(attr -g version "${repomountpoint}" | tail -n +2)
    local cvmfs_mount_revision
    cvmfs_mount_revision=$(attr -g revision "${repomountpoint}" | tail -n +2)
    generate_metric 'cvmfs_repo' 'guage' 'Shows the version of CVMFS used by this repository.' "repo=${fqrn},mountpoint=${repomountpoint},version=${cvmfs_mount_version},revision=${cvmfs_mount_revision}" 1

    local cvmfs_mount_rx_kb
    cvmfs_mount_rx_kb=$(attr -g rx "${repomountpoint}" | tail -n +2)
    local cvmfs_mount_rx
    cvmfs_mount_rx=$((cvmfs_mount_rx_kb * 1024))
    generate_metric 'cvmfs_rx_total' 'counter' 'Shows the overall amount of downloaded bytes since mounting.' "repo=${fqrn}" "${cvmfs_mount_rx}"

    local cvmfs_mount_uptime_minutes
    cvmfs_mount_uptime_minutes=$(attr -g uptime "${repomountpoint}" | tail -n +2)
    local now
    local rounded_now_to_minute
    local cvmfs_mount_uptime
    local cvmfs_mount_epoch_time
    now=$(date +%s)
    rounded_now_to_minute=$((now - (now % 60)))
    cvmfs_mount_uptime=$((cvmfs_mount_uptime_minutes * 60))
    cvmfs_mount_epoch_time=$((rounded_now_to_minute - cvmfs_mount_uptime))
    generate_metric 'cvmfs_uptime_seconds' 'counter' 'Shows the time since the repo was mounted.' "repo=${fqrn}" "${cvmfs_mount_uptime}"
    generate_metric 'cvmfs_mount_epoch_timestamp' 'counter' 'Shows the epoch time the repo was mounted.' "repo=${fqrn}" "${cvmfs_mount_epoch_time}"

    local cvmfs_repo_expires_min
    cvmfs_repo_expires_min=$(attr -g expires "${repomountpoint}" | tail -n +2)
    local cvmfs_repo_expires
    cvmfs_repo_expires=$((cvmfs_repo_expires_min * 60))
    generate_metric 'cvmfs_repo_expires_seconds' 'gague' 'Shows the remaining life time of the mounted root file catalog in seconds.' "repo=${fqrn}" "${cvmfs_repo_expires}"

    local cvmfs_mount_ndownload
    cvmfs_mount_ndownload=$(attr -g ndownload "${repomountpoint}" | tail -n +2)
    generate_metric 'cvmfs_ndownload_total' 'counter' 'Shows the overall number of downloaded files since mounting.' "repo=${fqrn}" "${cvmfs_mount_ndownload}"

    local cvmfs_mount_nioerr
    cvmfs_mount_nioerr=$(attr -g nioerr "${repomountpoint}" | tail -n +2)
    generate_metric 'cvmfs_nioerr_total' 'counter' 'Shows the total number of I/O errors encountered since mounting.' "repo=${fqrn}" "${cvmfs_mount_nioerr}"

    local cvmfs_mount_timeout
    cvmfs_mount_timeout=$(attr -g timeout "${repomountpoint}" | tail -n +2)
    generate_metric 'cvmfs_timeout' 'guage' 'Shows the timeout for proxied connections in seconds.' "repo=${fqrn}" "${cvmfs_mount_timeout}"

    local cvmfs_mount_timeout_direct
    cvmfs_mount_timeout_direct=$(attr -g timeout_direct "${repomountpoint}" | tail -n +2)
    generate_metric 'cvmfs_timeout_direct' 'guage' 'Shows the timeout for direct connections in seconds.' "repo=${fqrn}" "${cvmfs_mount_timeout_direct}"

    local cvmfs_mount_timestamp_last_ioerr
    cvmfs_mount_timestamp_last_ioerr=$(attr -g timestamp_last_ioerr "${repomountpoint}" | tail -n +2)
    generate_metric 'cvmfs_timestamp_last_ioerr' 'counter' 'Shows the timestamp of the last ioerror.' "repo=${fqrn}" "${cvmfs_mount_timestamp_last_ioerr}"

    local cvmfs_repo_pid_statline
    cvmfs_repo_pid_statline=$(</proc/"${repo_pid}"/stat)
    local cvmfs_repo_stats
    read -ra cvmfs_repo_stats <<<"${cvmfs_repo_pid_statline}"
    local cvmfs_utime
    local cvmfs_stime
    cvmfs_utime=${cvmfs_repo_stats[13]}
    cvmfs_stime=${cvmfs_repo_stats[14]}
    local cvmfs_user_seconds
    local cvmfs_system_seconds
    cvmfs_user_seconds=$(printf "%.2f" "$(echo "scale=4; $cvmfs_utime / $CLOCK_TICK" | bc)")
    cvmfs_system_seconds=$(printf "%.2f" "$(echo "scale=4; $cvmfs_stime / $CLOCK_TICK" | bc)")
    generate_metric 'cvmfs_cpu_user_total' 'counter' 'CPU time used in userspace by CVMFS mount in seconds.' "repo=${fqrn}" "${cvmfs_user_seconds}"
    generate_metric 'cvmfs_cpu_system_total' 'counter' 'CPU time used in the kernel system calls by CVMFS mount in seconds.' "repo=${fqrn}" "${cvmfs_system_seconds}"

    local cvmfs_mount_active_proxy
    cvmfs_mount_active_proxy=$(attr -g proxy "${repomountpoint}" | tail -n +2)
    generate_metric 'cvmfs_active_proxy' 'gauge' 'Shows the active proxy in use for this mount.' "repo=${fqrn},proxy=${cvmfs_mount_active_proxy}" 1

    # Pull in xattr based metrics with simple labels
    get_cvmfs_repo_extended_attribute_gague_metrics "${reponame}"
    get_cvmfs_repo_proxy_metrics "${reponame}"
}

#############################################################
# List "uncommon" commands we expect
for cmd in attr bc cvmfs_config cvmfs_talk grep; do
    if ! command -v "$cmd" &>/dev/null; then
        echo "ERROR: Required command '$cmd' not found" >&2
        exit 1
    fi
done

#############################################################
# setup args in the right order for making getopt evaluation
# nice and easy.  You'll need to read the manpages for more info
args=$(getopt --options 'h' --longoptions 'help,http' -- "$@")
eval set -- "$args"

for arg in $@; do
    case $1 in
    --)
        # end of getopt args, shift off the -- and get out of the loop
        shift
        break 2
        ;;
    --http)
        # Add the http header to the output
        HTTP_HEADER='TRUE'
        shift
        ;;
    -h | --help)
        # get help
        shift
        usage
        ;;
    esac
done

CLOCK_TICK=$(getconf CLK_TCK)

for REPO in $(cvmfs_config status | cut -d ' ' -f 1); do
    get_cvmfs_repo_metrics "${REPO}"
done

if [[ "${HTTP_HEADER}" == 'TRUE' ]]; then
    content_length=$(stat --printf="%s" "${TMPFILE}")
    echo -ne "HTTP/1.1 200 OK\r\n"
    echo -ne "Content-Type: text/plain; version=0.0.4; charset=utf-8; escaping=underscores\r\n"
    echo -ne "Content-Length: ${content_length}\r\n"
    echo -ne "Connection: close\r\n"
    echo -ne "\r\n"
fi

cat "${TMPFILE}"
