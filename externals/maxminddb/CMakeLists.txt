# CMake module for vendoring the MaxMind DB reader.
#
# Unlike the other externals this is NOT a C/C++ library: it is the pure-Python
# "maxminddb" package imported at runtime by the geo-api webapi
# (cvmfs/webapi/cvmfs_geo.py). Upstream also ships an optional C extension that
# links the system libmaxminddb, but cvmfs does not vendor that C library, so
# the extension was never built and only the pure-Python reader is shipped
# (see packaging/debian/.../cvmfs-server.install.in). We therefore only need to
# download the tarball and install the maxminddb/ package tree -- there is no
# build step, no compiled artifact and no CMake target to consume.
include(FetchContent)

set(MAXMINDDB_VERSION "1.5.4")
set(MAXMINDDB_URL "https://github.com/maxmind/MaxMind-DB-Reader-python/archive/refs/tags/v${MAXMINDDB_VERSION}.tar.gz")
set(MAXMINDDB_URL_MIRROR "https://ecsft.cern.ch/dist/cvmfs/build_externals/MaxMind-DB-Reader-python-${MAXMINDDB_VERSION}.tar.gz")
set(MAXMINDDB_HASH "MD5=f304242e2d3cfff938d4f1f6ce1a434c")

FetchContent_Declare(
  MaxMindDB
  URL "${MAXMINDDB_URL}" "${MAXMINDDB_URL_MIRROR}"
  URL_HASH "${MAXMINDDB_HASH}"
  DOWNLOAD_DIR ${EXTERNALS_DOWNLOAD_LOCATION}
  DOWNLOAD_EXTRACT_TIMESTAMP TRUE
  EXCLUDE_FROM_ALL
)

# The tarball has no CMakeLists.txt, so MakeAvailable only downloads and
# extracts the sources (it skips the add_subdirectory step).
FetchContent_MakeAvailable(MaxMindDB)

message(STATUS "Vendoring maxminddb python package from ${maxminddb_SOURCE_DIR}")

# Copy the pure-Python package into the externals prefix, reproducing the old
# makeHook.sh layout (${EXTERNALS_INSTALL_LOCATION}/maxminddb/...), which
# cvmfs/CMakeLists.txt in turn ships to /usr/share/cvmfs-server/webapi/maxminddb.
#
# This is done at configure time with file(COPY) rather than install(), on
# purpose: like the old makeHook.sh (which ran during bootstrap.sh), the
# package must physically exist under EXTERNALS_INSTALL_LOCATION by the time the
# main `make install` runs, because cvmfs/CMakeLists.txt reads it straight from
# that directory. An install(... COMPONENT external_deps) would only fire from
# the `install_externals` target, which nothing in the build/package flow
# invokes, so the directory would never be populated and `make install` would
# fail with "file INSTALL cannot find .../maxminddb". FetchContent extracts at
# configure time, so maxminddb_SOURCE_DIR is already available here.
file(COPY ${maxminddb_SOURCE_DIR}/maxminddb
     DESTINATION ${EXTERNALS_INSTALL_LOCATION})
