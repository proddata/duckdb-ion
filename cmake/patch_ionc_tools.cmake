if(NOT DEFINED IONC_SOURCE_DIR)
  message(FATAL_ERROR "IONC_SOURCE_DIR is required")
endif()

set(ionc_cmake "${IONC_SOURCE_DIR}/CMakeLists.txt")
if(NOT EXISTS "${ionc_cmake}")
  message(FATAL_ERROR "IonC CMakeLists.txt not found: ${ionc_cmake}")
endif()

file(READ "${ionc_cmake}" ionc_cmake_contents)
if(ionc_cmake_contents MATCHES "add_subdirectory\\(tools\\)")
  string(REPLACE "add_subdirectory(tools)" "# add_subdirectory(tools)" ionc_cmake_contents "${ionc_cmake_contents}")
  file(WRITE "${ionc_cmake}" "${ionc_cmake_contents}")
endif()
