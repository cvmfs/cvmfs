# CMake module for vendoring json
include(FetchContent)

set(JSON_VERSION "3.12.0")
set(JSON_URL "https://github.com/nlohmann/json/archive/refs/tags/v${JSON_VERSION}.tar.gz")
set(JSON_URL_MIRROR "https://ecsft.cern.ch/dist/cvmfs/build_externals/json-${JSON_VERSION}.tar.gz")
set(JSON_HASH "MD5=c2528c3e04faccaaee44f1f8f3d30d99")

setup_external_build_mode(json)

FetchContent_Declare(
  nlohmann_json
  URL "${JSON_URL}" "${JSON_URL_MIRROR}"
  URL_HASH "${JSON_HASH}"
  DOWNLOAD_DIR ${EXTERNALS_DOWNLOAD_LOCATION}
  INSTALL_DIR ${EXTERNALS_INSTALL_LOCATION}
  EXCLUDE_FROM_ALL
  FIND_PACKAGE_ARGS
)

FetchContent_MakeAvailable(nlohmann_json)
unset(FETCHCONTENT_TRY_FIND_PACKAGE_MODE)

if(nlohmann_json_FOUND)
  message(STATUS "Found json target")
  set_target_properties(nlohmann_json::nlohmann_json PROPERTIES IMPORTED_GLOBAL TRUE)
else()
  message(STATUS "Building json from vendored sources.")
  install(TARGETS nlohmann_json DESTINATION ${EXTERNALS_INSTALL_LOCATION} COMPONENT external_deps EXCLUDE_FROM_ALL)
  install(FILES  ${json_SOURCE_DIR}/include/nlohmann/json.hpp  DESTINATION ${EXTERNALS_INSTALL_LOCATION}/include COMPONENT external_deps EXCLUDE_FROM_ALL)
  set(EXTERNALS_INSTALL_TARGETS ${EXTERNALS_INSTALL_TARGETS} "json" PARENT_SCOPE)
endif()
