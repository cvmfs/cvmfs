# CMake module for vendoring sqlite3
include(FetchContent)

# SQLite is distributed as a preprocessed "amalgamation" (a single sqlite3.c).
# The download-number encoding is X.YY.ZZ.PP -> XYYZZPP, e.g. 3.38.2 -> 3380200,
# hosted under the release year directory on sqlite.org.
set(SQLITE3_VERSION "3.38.2")
set(SQLITE3_AMALGAMATION "sqlite-amalgamation-3380200")
set(SQLITE3_URL "https://www.sqlite.org/2022/${SQLITE3_AMALGAMATION}.zip")
set(SQLITE3_URL_MIRROR "https://ecsft.cern.ch/dist/cvmfs/build_externals/${SQLITE3_AMALGAMATION}.zip")
set(SQLITE3_HASH "MD5=a905f1aac38ecd5b8d215eabd09b0432")

# -> sqlite3 <-
# NOTE: sqlite ships only the amalgamation (no CMake support), so we fetch the
# sources and build a small static library ourselves, compiled with the
# cvmfs-specific tuning flags below. Doing it as a normal CMake target (instead
# of the old bootstrap.sh Makefile) means the build honours CMAKE_C_COMPILER,
# toolchain files and the sysroot, so cross-compilation works out of the box.

setup_external_build_mode(sqlite3)

FetchContent_Declare(
  sqlite3
  URL "${SQLITE3_URL}" "${SQLITE3_URL_MIRROR}"
  URL_HASH "${SQLITE3_HASH}"
  DOWNLOAD_EXTRACT_TIMESTAMP TRUE
  DOWNLOAD_DIR ${EXTERNALS_DOWNLOAD_LOCATION}
  INSTALL_DIR ${EXTERNALS_INSTALL_LOCATION}
  EXCLUDE_FROM_ALL
  FIND_PACKAGE_ARGS
)

# sqlite has no CMakeLists.txt: FetchContent_MakeAvailable only downloads and
# extracts the sources (it skips the add_subdirectory step); we define the build
# target against ${sqlite3_SOURCE_DIR} below.
FetchContent_MakeAvailable(sqlite3)
unset(FETCHCONTENT_TRY_FIND_PACKAGE_MODE)

if(sqlite3_FOUND)
  message(STATUS "Found system sqlite3: ${SQLITE3_LIBRARY}")
  # Findsqlite3.cmake exposes the uniform SQLite::SQLite3 imported target.
else()
  message(STATUS "Building sqlite3 from vendored sources.")

  # EXCLUDE_FROM_ALL: only build when a consumer links it. Matches the other
  # externals.
  add_library(sqlite3 STATIC EXCLUDE_FROM_ALL ${sqlite3_SOURCE_DIR}/sqlite3.c)
  target_include_directories(sqlite3
    PUBLIC $<BUILD_INTERFACE:${sqlite3_SOURCE_DIR}>
           $<INSTALL_INTERFACE:include>
  )
  # cvmfs-specific tuning, carried over verbatim from the old externals Makefile.
  # These only affect how sqlite3.c is compiled (no cvmfs source references them),
  # so they stay PRIVATE.
  target_compile_definitions(sqlite3 PRIVATE
    _REENTRANT __EXTENSIONS__ _LARGEFILE64_SOURCE __LARGE64_FILES
    SQLITE_DQS=0
    SQLITE_THREADSAFE=2
    HAVE_MALLOC_USABLE_SIZE
    SQLITE_ENABLE_MEMORY_MANAGEMENT
    SQLITE_ENABLE_MEMSYS5
    SQLITE_DEFAULT_PAGE_SIZE=1024
    SQLITE_DEFAULT_CACHE_SIZE=2000
    SQLITE_OMIT_DESERIALIZE
    SQLITE_OMIT_JSON
  )
  set_target_properties(sqlite3 PROPERTIES
    POSITION_INDEPENDENT_CODE ON
    C_STANDARD 11
    C_STANDARD_REQUIRED ON
  )

  add_library(SQLite::SQLite3 ALIAS sqlite3)

  # Back-compat: the tree still exposes sqlite headers to every target in cvmfs/
  # via the global include_directories() list (like the other externals), so keep
  # the legacy include variable pointing at the fetched sources. CACHE INTERNAL so
  # it is visible in the parent scope (this runs from the externals/ subdirectory).
  set(SQLITE3_INCLUDE_DIR "${sqlite3_SOURCE_DIR}" CACHE INTERNAL "sqlite3 include dir")

  install(TARGETS sqlite3 DESTINATION ${EXTERNALS_INSTALL_LOCATION} COMPONENT external_deps EXCLUDE_FROM_ALL)
  install(FILES  ${sqlite3_SOURCE_DIR}/sqlite3.h ${sqlite3_SOURCE_DIR}/sqlite3ext.h DESTINATION ${EXTERNALS_INSTALL_LOCATION}/include COMPONENT external_deps EXCLUDE_FROM_ALL)
  set(EXTERNALS_INSTALL_TARGETS ${EXTERNALS_INSTALL_TARGETS} "sqlite3" PARENT_SCOPE)
endif()
