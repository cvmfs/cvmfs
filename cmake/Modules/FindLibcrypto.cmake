# - Try to find builtin LibreSSL libcrypto
#
# Once done this will define
#
#  Libcrypto_FOUND - system has SHA3
#  Libcrypto_INCLUDE_DIRS - the SHA3 include directory
#  Libcrypto_LIBRARIES - Link these to use SHA3
#

if(CVMFS_SYSTEM_LIBCRYPTO)
  # The built-in LibreSSL is disabled: use the system OpenSSL located by the
  # mandatory find_package(OpenSSL) instead.
  set(Libcrypto_LIBRARIES OpenSSL::Crypto)
  set(Libcrypto_INCLUDE_DIRS ${OPENSSL_INCLUDE_DIR})
else()
  find_path(
      Libcrypto_INCLUDE_DIRS
      NAMES tls.h
      PATHS ${EXTERNALS_INSTALL_LOCATION}/crypto/include
      NO_DEFAULT_PATH
  )

  find_library(
      Libcrypto_LIBRARIES
      NAMES crypto
      PATHS ${EXTERNALS_INSTALL_LOCATION}/crypto/lib
      NO_DEFAULT_PATH
  )
endif()

include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(
    Libcrypto
    DEFAULT_MSG
    Libcrypto_LIBRARIES
    Libcrypto_INCLUDE_DIRS
)

if(Libcrypto_FOUND)
    mark_as_advanced(Libcrypto_LIBRARIES Libcrypto_INCLUDE_DIRS)
endif()

