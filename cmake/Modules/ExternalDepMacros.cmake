
function(setup_external_build_mode PACKAGE_NAME)
  # convert package name and lists to lowercase for robust comparison
  string(TOLOWER "${PACKAGE_NAME}" _pkg_lower)
  string(TOLOWER "${BUILTIN_EXTERNALS_LIST}" _list_lower)
  string(TOLOWER "${BUILTIN_EXTERNALS_EXCLUDE}" _exclude_lower)

  if("${_pkg_lower}" IN_LIST _exclude_lower)
    message(STATUS "External [${PACKAGE_NAME}]: Mode ALWAYS (System)")
    set(FETCHCONTENT_TRY_FIND_PACKAGE_MODE ALWAYS PARENT_SCOPE)
  elseif("${_pkg_lower}" IN_LIST _list_lower)
    message(STATUS "External [${PACKAGE_NAME}]: Mode NEVER (Source)")
    set(FETCHCONTENT_TRY_FIND_PACKAGE_MODE NEVER PARENT_SCOPE)
  else()
    message(STATUS "External [${PACKAGE_NAME}]: Mode OPTIONAL (Try System First)")
    set(FETCHCONTENT_TRY_FIND_PACKAGE_MODE OPT_IN PARENT_SCOPE)
  endif()
endfunction()

# Verify that a zlib we did not build ourselves compresses byte for byte like
# the vendored one. Content hashes are taken over the compressed stream, so a
# deviating implementation renames every object: publishing re-uploads content
# that is already there, and cvmfs_server check fails on existing repositories.
function(check_zlib_deflate_fingerprint ZLIB_LIBS ZLIB_INCLUDES)
  # Deflate output of the vendored zlib 1.2.8 for the reference buffer in
  # externals/zlib/check_deflate_fingerprint.c, as crc32(compressed):length.
  # Identical for every upstream zlib from 1.2.8 to 1.3.1; zlib-ng in compat
  # mode differs.
  set(_expected "e415c33f:65562")

  if(CVMFS_SKIP_ZLIB_FINGERPRINT_CHECK)
    return()
  endif()
  if(CMAKE_CROSSCOMPILING)
    message(STATUS "zlib deflate fingerprint: skipped (cross-compiling)")
    return()
  endif()

  try_run(_zlib_fp_run _zlib_fp_compile
    ${CMAKE_CURRENT_BINARY_DIR}/zlib_fingerprint
    SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/check_deflate_fingerprint.c
    LINK_LIBRARIES ${ZLIB_LIBS}
    CMAKE_FLAGS "-DINCLUDE_DIRECTORIES=${ZLIB_INCLUDES}"
    RUN_OUTPUT_VARIABLE _zlib_fp_out
  )
  string(STRIP "${_zlib_fp_out}" _zlib_fp_out)

  if(NOT _zlib_fp_compile OR NOT _zlib_fp_run EQUAL 0)
    message(WARNING "Could not verify the deflate output of ${ZLIB_LIBS}. "
                    "Continuing, but the unit tests are the next line of "
                    "defence.")
    return()
  endif()

  if(_zlib_fp_out STREQUAL "${_expected}")
    message(STATUS "zlib deflate fingerprint: ${_zlib_fp_out} (ok)")
    return()
  endif()

  string(CONCAT _msg
    "${ZLIB_LIBS} does not produce the deflate output cvmfs expects "
    "(${_zlib_fp_out} instead of ${_expected}). This is typically zlib-ng in "
    "zlib-compat mode. Since content hashes are computed over the compressed "
    "stream, the same file ends up under a different object name: "
    "deduplication against published content breaks and cvmfs_server check "
    "reports corruption. Drop zlib from BUILTIN_EXTERNALS_EXCLUDE to use the "
    "vendored zlib, or set -DCVMFS_SKIP_ZLIB_FINGERPRINT_CHECK=ON if you know "
    "this build never writes to a repository.")

  if(BUILD_SERVER OR BUILD_SERVER_DEBUG OR BUILD_UNITTESTS
     OR BUILD_UNITTESTS_DEBUG)
    message(FATAL_ERROR "${_msg}")
  else()
    message(WARNING "${_msg}")
  endif()
endfunction()
