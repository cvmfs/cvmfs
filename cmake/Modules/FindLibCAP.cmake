# Looking for libcap
# Found here:
# https://github.com/passy/webscan/blob/master/cmake/FindCAP.cmake

FIND_PATH(CAP_INCLUDE_DIR 
          NAMES
          sys/capability.h
          capability.h
)
  
FIND_LIBRARY(CAP_LIBRARY
             NAMES 
             cap
)

SET(CAP_INCLUDE_DIRS ${CAP_INCLUDE_DIR})
SET(CAP_LIBRARIES ${CAP_LIBRARY})

IF(CAP_INCLUDE_DIRS)
  MESSAGE(STATUS "Cap include dirs set to ${CAP_INCLUDE_DIRS}")
ELSE(CAP_INCLUDE_DIRS)
  MESSAGE(FATAL " Cap include dirs cannot be found")
ENDIF(CAP_INCLUDE_DIRS)

IF(CAP_LIBRARIES)
  MESSAGE(STATUS "Pcap library set to ${CAP_LIBRARIES}")
ELSE(CAP_LIBRARIES)
  MESSAGE(FATAL "Pcap library cannot be found")
ENDIF(CAP_LIBRARIES)

#Functions
INCLUDE(CheckFunctionExists)
SET(CMAKE_REQUIRED_INCLUDES ${CAP_INCLUDE_DIRS})
SET(CMAKE_REQUIRED_LIBRARIES ${CAP_LIBRARIES})


#Is pcap found ?
IF(CAP_INCLUDE_DIRS AND CAP_LIBRARIES)
  SET(CAP_FOUND "YES" )
ENDIF(CAP_INCLUDE_DIRS AND CAP_LIBRARIES)

find_package_handle_standard_args(LibCAP DEFAULT_MSG
    CAP_LIBRARIES
    CAP_INCLUDE_DIRS
)

MARK_AS_ADVANCED(
  CAP_LIBRARIES
  CAP_INCLUDE_DIRS
)