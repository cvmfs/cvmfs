project(SHA3)

add_library(cvmfs_sha3 STATIC
  src/64opt/KeccakF-1600-opt64.c
  src/KeccakHash.c
  src/KeccakSponge.c
  src/SnP-FBWL-default.c
  )

set_property(TARGET cvmfs_sha3 PROPERTY POSITION_INDEPENDENT_CODE TRUE)

target_include_directories(cvmfs_sha3 PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/src
  ${CMAKE_CURRENT_SOURCE_DIR}/src/64opt
  )

set(SHA3_INCLUDE_DIRS ${CMAKE_CURRENT_SOURCE_DIR}/src ${CMAKE_CURRENT_SOURCE_DIR}/src/64opt PARENT_SCOPE)
set(SHA3_LIBRARY $<TARGET_FILE:cvmfs_sha3> PARENT_SCOPE)
set(SHA3_LIBRARIES cvmfs_sha3 PARENT_SCOPE)
