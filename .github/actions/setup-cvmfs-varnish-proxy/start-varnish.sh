#!/usr/bin/env bash

set -euo pipefail

port="${INPUT_PORT:-}"
config_path="${INPUT_CONFIG_PATH:-}"
system_cache_dir="${INPUT_SYSTEM_CACHE_DIR:-}"
system_log_dir="${INPUT_SYSTEM_LOG_DIR:-}"
restore_cache_dir="${INPUT_RESTORE_CACHE_DIR:-}"
restore_log_dir="${INPUT_RESTORE_LOG_DIR:-}"
cache_size="${INPUT_CACHE_SIZE:-}"
action_path="${INPUT_ACTION_PATH:?INPUT_ACTION_PATH is required}"

port="${port:-3128}"
config_path="${config_path:-/etc/varnish/cvmfs.vcl}"
system_cache_dir="${system_cache_dir:-/var/cache/varnish}"
system_log_dir="${system_log_dir:-/var/log/varnish}"
restore_cache_dir="${restore_cache_dir:-$HOME/.cache/varnishcache/varnish-cache}"
restore_log_dir="${restore_log_dir:-$HOME/.cache/varnishcache/varnish-logs}"
cache_size="${cache_size:-2G}"

runtime_dir="${system_cache_dir}/runtime"
pid_file="${runtime_dir}/varnishd.pid"
storage_file="${system_cache_dir}/storage.bin"
vcl_path="${config_path}"

sudo apt-get update
sudo apt-get install -y --no-install-recommends varnish varnish-modules
sudo systemctl stop varnish >/dev/null 2>&1 || true

if ! find /usr/lib -path '*varnish*' -name 'libvmod_dynamic.so' -print -quit | grep -q .; then
  echo "libvmod_dynamic.so was not installed; the upstream cvmfs.vcl requires the dynamic VMOD" >&2
  exit 1
fi

sudo mkdir -p "${system_cache_dir}" "${system_log_dir}" "$(dirname "${config_path}")"
sudo install -m 0644 "${action_path}/cvmfs.vcl" "${config_path}"

if [ -d "${restore_cache_dir}" ] && [ "$(ls -A "${restore_cache_dir}")" ]; then
  sudo cp -a "${restore_cache_dir}"/. "${system_cache_dir}/"
fi

if [ -d "${restore_log_dir}" ] && [ "$(ls -A "${restore_log_dir}")" ]; then
  sudo cp -a "${restore_log_dir}"/. "${system_log_dir}/"
fi

sudo chown -R "$(id -u):$(id -g)" "${system_cache_dir}" "${system_log_dir}"
sudo chmod -R 755 "${system_cache_dir}" "${system_log_dir}"

rm -rf "${runtime_dir}"
mkdir -p "${runtime_dir}" "${system_log_dir}"
: > "${system_log_dir}/varnish.log"
: > "${system_log_dir}/access.log"

varnishd \
  -a "127.0.0.1:${port}" \
  -f "${vcl_path}" \
  -n "${runtime_dir}" \
  -s "file,${storage_file},${cache_size}" \
  -P "${pid_file}"

for _ in $(seq 1 20); do
  if [[ -s "${pid_file}" ]] && kill -0 "$(cat "${pid_file}")" 2>/dev/null; then
    break
  fi
  sleep 1
done

if [[ ! -s "${pid_file}" ]] || ! kill -0 "$(cat "${pid_file}" 2>/dev/null)" 2>/dev/null; then
  echo "Varnish failed to stay running; inspect ${system_log_dir}/varnish.log for details" >&2
  exit 1
fi

varnishlog \
  -D \
  -n "${runtime_dir}" \
  -P "${runtime_dir}/varnishlog.pid" \
  -w "${system_log_dir}/varnish.log"

varnishncsa \
  -D \
  -n "${runtime_dir}" \
  -P "${runtime_dir}/varnishncsa.pid" \
  -w "${system_log_dir}/access.log"

{
  echo "proxy-url=http://localhost:${port}"
} >> "${GITHUB_OUTPUT}"