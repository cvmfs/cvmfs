#!/bin/bash
# -----------------------------------------------------------------------------
# Description:
# This script automates the process of downloading, building, and installing
# the specified version of the libfuse library. This is particularly useful 
# when testing CVMFS on various linux kernel versions which uses different versions 
# which may require different versions of libfuse due to compatibility differences.
#
# Usage:
# ./build_libfuse.sh <libfuse_version>
#
# Example:
# ./build_libfuse.sh 3.10.0
#
# Dependencies:
#   - curl
#   - meson
#   - ninja
# -----------------------------------------------------------------------------

set -eo pipefail

SCRIPT_LOCATION=$(
	cd "$(dirname "$0")"
	pwd
)
. ${SCRIPT_LOCATION}/common.sh

command -v curl >/dev/null 2>&1 || die "curl is not installed"
command -v meson >/dev/null 2>&1 || die "meson is not installed"

LIBFUSE_VERSION="$1"
if [[ -z "$LIBFUSE_VERSION" ]]; then
	echo "ERROR: No LIBFUSE_VERSION specified"
	echo "  Usage: build_libfuse.sh <libfuse_version>"
	exit 1
fi

REPO_ROOT="$(get_repository_root)"

DISTRO=""
DISTRO_ID=""
DISTRO_VERSION=""
PLATFORM_ID=""

# Check for Linux
if [[ "$(uname)" == "Linux" ]]; then
	# Check for /etc/os-release for further distro information
	if [[ -f /etc/os-release ]]; then
		# Read lines from /etc/os-release
		while IFS= read -r line; do
			# Extract the ID and VERSION_ID
			if [[ "$line" =~ ^ID=(.*) ]]; then
				DISTRO_ID="${BASH_REMATCH[1]}"
			fi
			if [[ "$line" =~ ^VERSION_ID=(.*) ]]; then
				DISTRO_VERSION="${BASH_REMATCH[1]}"
			fi
			if [[ "$line" =~ ^PLATFORM_ID=(.*) ]]; then
				PLATFORM_ID="${BASH_REMATCH[1]}"
			fi
		done </etc/os-release

		# Trim quotes from DISTRO_ID, DISTRO_VERSION, and PLATFORM_ID
		DISTRO_ID="${DISTRO_ID//\"/}"
		DISTRO_VERSION="${DISTRO_VERSION//\"/}"
		PLATFORM_ID="${PLATFORM_ID//\"/}"

		# Use PLATFORM_ID if available
		if [[ -n "$PLATFORM_ID" ]]; then
			# Extract the part after the colon from PLATFORM_ID (e.g., "platform:el9" -> "el9")
			if [[ "$PLATFORM_ID" =~ ^([^:]+):(.*) ]]; then
				DISTRO="${BASH_REMATCH[2]}"
			else
				DISTRO="$PLATFORM_ID"
			fi
		else
			DISTRO="${DISTRO_ID}${DISTRO_VERSION}"
		fi
	fi
fi

ARCH=$(uname -m)

# Logic to define build and install locations for externals
if [ -d "${REPO_ROOT}/externals_build" ] && [ -d "${REPO_ROOT}/externals_install" ]; then
	EXTERNALS_BUILD_LOCATION="${REPO_ROOT}/externals_build"
	EXTERNALS_INSTALL_LOCATION="${REPO_ROOT}/externals_install"
else
	EXTERNALS_BUILD_LOCATION="${REPO_ROOT}/externals_build.${ARCH}"
	EXTERNALS_INSTALL_LOCATION="${REPO_ROOT}/externals_install.${ARCH}"
	if [ -n "$DISTRO" ]; then
		EXTERNALS_BUILD_LOCATION="${EXTERNALS_BUILD_LOCATION}.${DISTRO}"
		EXTERNALS_INSTALL_LOCATION="${EXTERNALS_INSTALL_LOCATION}.${DISTRO}"
	fi
fi

# Create the directories for the build and install locations
echo "Using build and install directories at:"
echo "  Build location: ${EXTERNALS_BUILD_LOCATION}"
echo "  Install location: ${EXTERNALS_INSTALL_LOCATION}"

mkdir -p "${EXTERNALS_BUILD_LOCATION}" "${EXTERNALS_INSTALL_LOCATION}"

if [[ -z "$LIBFUSE_VERSION" ]]; then
	echo "ERROR: No LIBFUSE_VERSION specified."
	echo "Usage: build_libfuse.sh <libfuse_version>"
	exit 1
fi

LIBFUSE_URL="https://github.com/libfuse/libfuse/releases/download/fuse-${LIBFUSE_VERSION}/fuse-${LIBFUSE_VERSION}.tar.gz"
LIBFUSE_TARBALL="fuse-${LIBFUSE_VERSION}.tar.gz"

LIBFUSE_BUILD_DIR="${EXTERNALS_BUILD_LOCATION}/build_libfuse-${LIBFUSE_VERSION}"
LIBFUSE_INSTALL_DIR="${EXTERNALS_INSTALL_LOCATION}"

mkdir -p "$LIBFUSE_BUILD_DIR" "$LIBFUSE_INSTALL_DIR"

# Download and extract libfuse
if [[ ! -f "${LIBFUSE_BUILD_DIR}/${LIBFUSE_TARBALL}" ]]; then
	echo "Downloading libfuse ${LIBFUSE_VERSION}..."
	curl -L "$LIBFUSE_URL" -o "${LIBFUSE_BUILD_DIR}/${LIBFUSE_TARBALL}" || die "Failed to download libfuse ${LIBFUSE_VERSION}"
fi

if [[ ! -d "${LIBFUSE_BUILD_DIR}/fuse-${LIBFUSE_VERSION}" ]]; then
	echo "Extracting libfuse..."
	tar -xf "${LIBFUSE_BUILD_DIR}/${LIBFUSE_TARBALL}" -C "$LIBFUSE_BUILD_DIR"
fi

MESON_SOURCE_DIR="${LIBFUSE_BUILD_DIR}/fuse-${LIBFUSE_VERSION}"

# Configure libfuse
echo "Configuring libfuse..."
meson setup \
	--prefix="$LIBFUSE_INSTALL_DIR" \
	-Dudevrulesdir=lib/udev/rules.d \
	-Dinitscriptdir=etc/init.d \
	-Duseroot=false \
	-Dexamples=false \
	-Dtests=false \
	"$LIBFUSE_BUILD_DIR" \
	"$MESON_SOURCE_DIR"

# Build libfuse
echo "Building libfuse..."
ninja -C "$LIBFUSE_BUILD_DIR"

# Install libfuse
echo "Installing libfuse..."
ninja -C "$LIBFUSE_BUILD_DIR" install

echo ""
echo "libfuse ${LIBFUSE_VERSION} built and installed at ${LIBFUSE_INSTALL_DIR}"
