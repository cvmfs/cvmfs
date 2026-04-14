#!/bin/bash
# entrypoint-apache.sh – start-up script for the cvmfs-apache container.
#
# Waits until the gateway container has written at least one CVMFS Apache
# config snippet to the shared /apache-conf volume, then symlinks them into
# httpd's conf.d directory and starts Apache in the foreground.

set -e

APACHE_CONF_DIR="${APACHE_CONF_DIR:-/apache-conf}"
HTTPD_CONF_D="/etc/httpd/conf.d"

# ---------------------------------------------------------------------------
# Enable required Apache modules (mod_proxy, mod_headers, mod_expires)
# httpd on AlmaLinux/RHEL loads modules from /etc/httpd/conf.modules.d/;
# the proxy modules are already listed but may be commented out.
# ---------------------------------------------------------------------------
enable_module() {
    local mod_file="$1"
    local directive="$2"
    if [ -f "${mod_file}" ]; then
        sed -i "s|^#\(${directive}\)|\1|" "${mod_file}"
    fi
}
enable_module /etc/httpd/conf.modules.d/00-proxy.conf    "LoadModule proxy_module"
enable_module /etc/httpd/conf.modules.d/00-proxy.conf    "LoadModule proxy_http_module"
enable_module /etc/httpd/conf.modules.d/00-base.conf     "LoadModule headers_module"
enable_module /etc/httpd/conf.modules.d/00-base.conf     "LoadModule expires_module"

# ---------------------------------------------------------------------------
# Wait until at least one conf file has been placed on the shared volume
# ---------------------------------------------------------------------------
echo "[entrypoint-apache] Waiting for CVMFS Apache configs in ${APACHE_CONF_DIR} ..."
while [ -z "$(ls -A "${APACHE_CONF_DIR}"/*.conf 2>/dev/null)" ]; do
    sleep 2
done
echo "[entrypoint-apache] Config files found:"
ls "${APACHE_CONF_DIR}"/*.conf

# ---------------------------------------------------------------------------
# Symlink (or copy) all conf files into httpd's conf.d
# ---------------------------------------------------------------------------
for conf_file in "${APACHE_CONF_DIR}"/*.conf; do
    dest="${HTTPD_CONF_D}/$(basename "${conf_file}")"
    if [ ! -e "${dest}" ]; then
        ln -s "${conf_file}" "${dest}"
        echo "[entrypoint-apache] Linked ${conf_file} → ${dest}"
    fi
done

# ---------------------------------------------------------------------------
# Validate the configuration before starting
# ---------------------------------------------------------------------------
echo "[entrypoint-apache] Checking Apache configuration ..."
httpd -t

# ---------------------------------------------------------------------------
# Start Apache in the foreground
# ---------------------------------------------------------------------------
echo "[entrypoint-apache] Starting Apache ..."
exec httpd -D FOREGROUND

