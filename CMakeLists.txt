cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME ion)

# DuckDB's extension distribution supports vcpkg. As such, dependencies can be added in ./vcpkg.json and then
# used in cmake with find_package. Feel free to remove or replace with other dependencies.
# Note that it should also be removed from vcpkg.json to prevent needlessly installing it..
find_package(OpenSSL REQUIRED)
if(DEFINED VCPKG_TARGET_TRIPLET)
  set(IONC_VCPKG_DIR "${CMAKE_CURRENT_LIST_DIR}/vcpkg_installed/${VCPKG_TARGET_TRIPLET}/share/ion-c")
endif()
if(IONC_VCPKG_DIR AND EXISTS "${IONC_VCPKG_DIR}/IonCConfig.cmake")
  find_package(IonC CONFIG REQUIRED PATHS "${IONC_VCPKG_DIR}" NO_DEFAULT_PATH)
else()
  find_package(IonC CONFIG REQUIRED)
endif()

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

set(EXTENSION_SOURCES
  src/ion_extension.cpp
  src/ion_copy.cpp
  src/ion_serialize.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link OpenSSL in both the static library as the loadable extension
target_link_libraries(${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto)
target_link_libraries(${LOADABLE_EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto)
target_link_libraries(${EXTENSION_NAME} IonC::ionc)
target_link_libraries(${LOADABLE_EXTENSION_NAME} IonC::ionc)
target_compile_definitions(${EXTENSION_NAME} PRIVATE DUCKDB_IONC DECNUMDIGITS=34)
target_compile_definitions(${LOADABLE_EXTENSION_NAME} PRIVATE DUCKDB_IONC DECNUMDIGITS=34)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
