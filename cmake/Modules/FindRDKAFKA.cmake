if(APPLE)
  set(RDKAFKA_LIBRARY_VAR System)
else()
  # Linux type:
  set(RDKAFKA_LIBRARY_VAR rdkafka)
endif()

find_library(RDKAFKA_LIBRARY
  NAMES ${RDKAFKA_LIBRARY_VAR}
  PATHS /lib /usr/lib /usr/local/lib
  )

# Must be *after* the lib itself
set(CMAKE_FIND_FRAMEWORK_SAVE ${CMAKE_FIND_FRAMEWORK})
set(CMAKE_FIND_FRAMEWORK NEVER)

find_path(RDKAFKA_INCLUDE_DIR rdkafka.h
/usr/include/librdkafka
/usr/local/include
/usr/include
)

if (RDKAFKA_LIBRARY AND RDKAFKA_INCLUDE_DIR)
  set(RDKAFKA_LIBRARIES ${RDKAFKA_LIBRARY})
  set(RDKAFKA_FOUND "YES")
else ()
  set(RDKAFKA_FOUND "NO")
endif ()


if (RDKAFKA_FOUND)
   if (NOT RDKAFKA_FIND_QUIETLY)
      message(STATUS "Found RDKAFKA: ${RDKAFKA_LIBRARIES}")
      set (RDKAFKA_FIND_QUIETLY 1 CACHE BOOL "Don't spam about RDKAFKA findings")
   endif ()
else ()
   if (RDKAFKA_FIND_REQUIRED)
      message( "library: ${RDKAFKA_LIBRARY}" )
      message( "include: ${RDKAFKA_INCLUDE_DIR}" )
      message(FATAL_ERROR "Could not find RDKAFKA library")
   endif ()
endif ()

# Deprecated declarations.
#set (NATIVE_RDKAFKA_INCLUDE_PATH ${RDKAFKA_INCLUDE_DIR} )
#get_filename_component (NATIVE_RDKAFKA_LIB_PATH ${RDKAFKA_LIBRARY} PATH)

mark_as_advanced(
  RDKAFKA_LIBRARY
  RDKAFKA_INCLUDE_DIR
  )
set(CMAKE_FIND_FRAMEWORK ${CMAKE_FIND_FRAMEWORK_SAVE})
