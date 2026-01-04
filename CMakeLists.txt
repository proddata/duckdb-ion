cmake_minimum_required(VERSION 3.11)

# Set extension name here
set(TARGET_NAME ion)

include(${CMAKE_CURRENT_LIST_DIR}/cmake/ionc.cmake)
duckdb_ion_resolve_ionc(IONC_TARGET DECNUMBER_TARGET)
set(IONC_LINK_TARGETS ${IONC_TARGET})
if(DECNUMBER_TARGET)
  list(APPEND IONC_LINK_TARGETS ${DECNUMBER_TARGET})
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

target_link_libraries(${EXTENSION_NAME} ${IONC_LINK_TARGETS})
target_link_libraries(${LOADABLE_EXTENSION_NAME} ${IONC_LINK_TARGETS})
target_compile_definitions(${EXTENSION_NAME} PRIVATE DUCKDB_IONC DECNUMDIGITS=34)
target_compile_definitions(${LOADABLE_EXTENSION_NAME} PRIVATE DUCKDB_IONC DECNUMDIGITS=34)

if(DUCKDB_ION_IONC_FROM_FETCHCONTENT AND DEFINED DUCKDB_EXPORT_SET)
  foreach(ionc_dep IN LISTS IONC_LINK_TARGETS)
    if(TARGET ${ionc_dep})
      get_target_property(_ionc_dep_imported ${ionc_dep} IMPORTED)
      if(NOT _ionc_dep_imported)
        install(
          TARGETS ${ionc_dep}
          EXPORT "${DUCKDB_EXPORT_SET}"
          LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
          ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
      endif()
    endif()
  endforeach()
endif()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
