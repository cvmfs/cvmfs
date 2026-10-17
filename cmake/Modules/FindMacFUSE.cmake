# Find macFUSE (libfuse3 + the macfuse.fs bundle)
#
#  MacFUSE_FOUND      - True if the libfuse3 headers and library are found
#  MACFUSE_VERSION    - Version of the installed macfuse.fs bundle
#  MACFUSE_HAS_FSKIT  - True if macFUSE supports the FSKit backend
#
# Also sets the FUSE3_* variables of FindFUSE3.

set (MACFUSE_MIN_VERSION_FSKIT "5.4.0")
set (MACFUSE_PLIST "/Library/Filesystems/macfuse.fs/Contents/Info.plist")

find_package (FUSE3 QUIET)

# The fuse3 headers carry no macFUSE version, so read it from the bundle
set (MACFUSE_VERSION "")
if (EXISTS ${MACFUSE_PLIST})
  file (READ ${MACFUSE_PLIST} _macfuse_plist)
  string (REGEX MATCH
          "<key>CFBundleShortVersionString</key>[ \t\r\n]*<string>([^<]+)</string>"
          _ "${_macfuse_plist}")
  set (MACFUSE_VERSION "${CMAKE_MATCH_1}")
endif ()

set (MACFUSE_HAS_FSKIT FALSE)
if (MACFUSE_VERSION AND NOT MACFUSE_VERSION VERSION_LESS MACFUSE_MIN_VERSION_FSKIT)
  set (MACFUSE_HAS_FSKIT TRUE)
endif ()

include (FindPackageHandleStandardArgs)
find_package_handle_standard_args (MacFUSE
  REQUIRED_VARS FUSE3_LIBRARY FUSE3_INCLUDE_DIR
  VERSION_VAR MACFUSE_VERSION
)
