# CMake module for vendoring pacparser
include(FetchContent)

set(PACPARSER_VERSION "1.5.1")
set(PACPARSER_URL "https://github.com/manugarg/pacparser/archive/refs/tags/v${PACPARSER_VERSION}.tar.gz")
set(PACPARSER_URL_MIRROR "https://ecsft.cern.ch/dist/cvmfs/build_externals/pacparser-${PACPARSER_VERSION}.tar.gz")
set(PACPARSER_HASH "MD5=df887062fbe8f6d60397fadd12d2ebb1")

# -> pacparser <-
# NOTE: pacparser ships only a Makefile (no CMake support), so we fetch the
# sources and build a small static library ourselves. 
#
# Since 1.5 pacparser uses quickjs which improves the build a lot.

setup_external_build_mode(pacparser)

FetchContent_Declare(
  pacparser
  URL "${PACPARSER_URL}" "${PACPARSER_URL_MIRROR}"
  URL_HASH "${PACPARSER_HASH}"
  DOWNLOAD_EXTRACT_TIMESTAMP TRUE
  DOWNLOAD_DIR ${EXTERNALS_LIB_LOCATION}/download
  INSTALL_DIR ${EXTERNALS_INSTALL_LOCATION}
  EXCLUDE_FROM_ALL
  FIND_PACKAGE_ARGS
)

# pacparser has no CMakeLists.txt: FetchContent_MakeAvailable only downloads and
# extracts the sources (it skips the add_subdirectory step); we define the build
# target against ${pacparser_SOURCE_DIR} below.
FetchContent_MakeAvailable(pacparser)
unset(FETCHCONTENT_TRY_FIND_PACKAGE_MODE)

if(pacparser_FOUND)
  message(STATUS "Found system pacparser: ${PACPARSER_LIBRARIES}")
  # Findpacparser.cmake exposes the uniform pacparser::pacparser imported target.
else()
  message(STATUS "Building pacparser from vendored sources.")

  # EXCLUDE_FROM_ALL: only build when a consumer links it (e.g. the client),
  # so server-only builds don't compile QuickJS. Matches the other externals.
  add_library(pacparser STATIC EXCLUDE_FROM_ALL
    ${pacparser_SOURCE_DIR}/src/pacparser.c
    ${pacparser_SOURCE_DIR}/src/quickjs/quickjs.c
  )
  # public: pacparser.h ; private: the bundled QuickJS headers
  target_include_directories(pacparser
    PUBLIC  $<BUILD_INTERFACE:${pacparser_SOURCE_DIR}/src>
            $<INSTALL_INTERFACE:include>
    PRIVATE ${pacparser_SOURCE_DIR}/src/quickjs
  )
  target_compile_definitions(pacparser PRIVATE VERSION="${PACPARSER_VERSION}")
  set_target_properties(pacparser PROPERTIES
    POSITION_INDEPENDENT_CODE ON
    C_STANDARD 11
    C_STANDARD_REQUIRED ON
  )
  # QuickJS-ng needs libm, pthreads and (for dlopen on some platforms) libdl.
  find_package(Threads REQUIRED)
  target_link_libraries(pacparser PUBLIC m Threads::Threads ${CMAKE_DL_LIBS})

  add_library(pacparser::pacparser ALIAS pacparser)

  install(TARGETS pacparser DESTINATION ${EXTERNALS_INSTALL_LOCATION} COMPONENT external_deps EXCLUDE_FROM_ALL)
  install(FILES  ${pacparser_SOURCE_DIR}/src/pacparser.h DESTINATION ${EXTERNALS_INSTALL_LOCATION}/include COMPONENT external_deps EXCLUDE_FROM_ALL)
  set(EXTERNALS_INSTALL_TARGETS ${EXTERNALS_INSTALL_TARGETS} "pacparser" PARENT_SCOPE)
endif()
