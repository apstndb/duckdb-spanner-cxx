cmake_minimum_required(VERSION 3.5)

set(TARGET_NAME spanner)

find_package(google_cloud_cpp_spanner REQUIRED)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

set(EXTENSION_SOURCES
    src/spanner_extension.cpp
    src/spanner_client.cpp
    src/spanner_query.cpp
    src/spanner_scan.cpp
    src/spanner_copy.cpp
    src/spanner_types.cpp
    src/spanner_utils.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

target_link_libraries(${EXTENSION_NAME} google-cloud-cpp::spanner)
target_link_libraries(${LOADABLE_EXTENSION_NAME} google-cloud-cpp::spanner)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
