project(VJSON)

add_library(cvmfs_vjson STATIC src/block_allocator.cpp src/json.cpp)
set_property(TARGET cvmfs_vjson PROPERTY POSITION_INDEPENDENT_CODE TRUE)
target_include_directories(cvmfs_vjson PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
set(VJSON_INCLUDE_DIRS ${CMAKE_CURRENT_SOURCE_DIR}/src PARENT_SCOPE)
set(VJSON_LIBRARY $<TARGET_FILE:cvmfs_vjson> PARENT_SCOPE)
set(VJSON_LIBRARIES cvmfs_vjson PARENT_SCOPE)
