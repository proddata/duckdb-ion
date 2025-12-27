cmake_minimum_required(VERSION 3.11)

# Set extension name here
set(TARGET_NAME ion)

# DuckDB's extension distribution supports vcpkg. As such, dependencies can be added in ./vcpkg.json and then
# used in cmake with find_package. Feel free to remove or replace with other dependencies.
# Note that it should also be removed from vcpkg.json to prevent needlessly installing it..
if(DEFINED VCPKG_TARGET_TRIPLET)
  set(IONC_VCPKG_DIR "${CMAKE_CURRENT_LIST_DIR}/vcpkg_installed/${VCPKG_TARGET_TRIPLET}/share/ion-c")
endif()
if(IONC_VCPKG_DIR AND EXISTS "${IONC_VCPKG_DIR}/IonCConfig.cmake")
  find_package(IonC CONFIG REQUIRED PATHS "${IONC_VCPKG_DIR}" NO_DEFAULT_PATH)
else()
  find_package(IonC CONFIG QUIET)
endif()

if(NOT IonC_FOUND)
  include(FetchContent)
  set(IONC_BUILD_TESTS OFF CACHE BOOL "" FORCE)
  set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
  FetchContent_Declare(
    ionc
    GIT_REPOSITORY https://github.com/amzn/ion-c.git
    GIT_TAG v1.1.4
  )
  FetchContent_MakeAvailable(ionc)
endif()

set(IONC_INCLUDE_DIRS "")
set(IONC_LINK_LIB "")
set(IONC_INTERFACE_LIBS "")
if(TARGET IonC::ionc)
  get_target_property(IONC_INCLUDE_DIRS IonC::ionc INTERFACE_INCLUDE_DIRECTORIES)
  get_target_property(IONC_LINK_LIB IonC::ionc IMPORTED_LOCATION)
  get_target_property(IONC_INTERFACE_LIBS IonC::ionc INTERFACE_LINK_LIBRARIES)
  if(NOT IONC_LINK_LIB)
    get_target_property(IONC_LINK_LIB IonC::ionc IMPORTED_LOCATION_RELEASE)
  endif()
endif()
if(NOT IONC_LINK_LIB AND TARGET ionc)
  set(IONC_LINK_LIB $<TARGET_FILE:ionc>)
  get_target_property(IONC_INTERFACE_LIBS ionc INTERFACE_LINK_LIBRARIES)
  if(NOT IONC_INCLUDE_DIRS)
    set(IONC_INCLUDE_DIRS "${ionc_SOURCE_DIR}/include")
  endif()
endif()
if(NOT IONC_LINK_LIB)
  find_package(IonC CONFIG REQUIRED)
  get_target_property(IONC_INCLUDE_DIRS IonC::ionc INTERFACE_INCLUDE_DIRECTORIES)
  get_target_property(IONC_LINK_LIB IonC::ionc IMPORTED_LOCATION)
  get_target_property(IONC_INTERFACE_LIBS IonC::ionc INTERFACE_LINK_LIBRARIES)
  if(NOT IONC_LINK_LIB)
    get_target_property(IONC_LINK_LIB IonC::ionc IMPORTED_LOCATION_RELEASE)
  endif()
endif()
if(NOT IONC_LINK_LIB)
  message(FATAL_ERROR "IonC library location not found")
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

if(IONC_INCLUDE_DIRS)
  target_include_directories(${EXTENSION_NAME} PRIVATE ${IONC_INCLUDE_DIRS})
  target_include_directories(${LOADABLE_EXTENSION_NAME} PRIVATE ${IONC_INCLUDE_DIRS})
endif()
target_link_libraries(${EXTENSION_NAME} ${IONC_LINK_LIB})
target_link_libraries(${LOADABLE_EXTENSION_NAME} ${IONC_LINK_LIB})
if(IONC_INTERFACE_LIBS)
  target_link_libraries(${EXTENSION_NAME} ${IONC_INTERFACE_LIBS})
  target_link_libraries(${LOADABLE_EXTENSION_NAME} ${IONC_INTERFACE_LIBS})
endif()
target_compile_definitions(${EXTENSION_NAME} PRIVATE DUCKDB_IONC DECNUMDIGITS=34)
target_compile_definitions(${LOADABLE_EXTENSION_NAME} PRIVATE DUCKDB_IONC DECNUMDIGITS=34)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
