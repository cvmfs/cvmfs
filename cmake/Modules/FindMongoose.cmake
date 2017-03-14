# - Try to find Mongoose
#
# Once done this will define
#
#  MONGOOSE_FOUND - system has mongoose
#  MONGOOSE_INCLUDE_DIRS - the mongoose include directory
#  MONGOOSE_LIBRARIES - Link these to use mongoose
#

find_path(
    MONGOOSE_INCLUDE_DIRS
    NAMES mongoose.h
    HINTS ${MONGOOSE_INCLUDE_DIRS}
)

find_library(
    MONGOOSE_LIBRARIES
    NAMES mongoose
    HINTS ${MONGOOSE_LIBRARY_DIRS}
)

include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(
    MONGOOSE
    DEFAULT_MSG
    MONGOOSE_LIBRARIES
    MONGOOSE_INCLUDE_DIRS
)

if(MONGOOSE_FOUND)
    mark_as_advanced(MONGOOSE_LIBRARIES MONGOOSE_INCLUDE_DIRS)
endif()

