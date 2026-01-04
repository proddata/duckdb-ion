include(FetchContent)

set(DUCKDB_ION_CMAKE_DIR "${CMAKE_CURRENT_LIST_DIR}")

function(duckdb_ion_resolve_ionc out_ionc_target out_decnumber_target)
	# Prefer an installed IonC package (vcpkg or system), fall back to FetchContent.
	set(_ionc_target "")
	set(_decnumber_target "")
	set(DUCKDB_ION_IONC_FROM_FETCHCONTENT FALSE PARENT_SCOPE)

	if(DEFINED VCPKG_TARGET_TRIPLET)
		set(_ionc_candidate_share_dirs "")
		list(APPEND _ionc_candidate_share_dirs
		     "${DUCKDB_ION_CMAKE_DIR}/../vcpkg_installed/${VCPKG_TARGET_TRIPLET}/share/ion-c")

		if(DEFINED VCPKG_INSTALLED_DIR)
			list(APPEND _ionc_candidate_share_dirs "${VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET}/share/ion-c")
		endif()

		if(DEFINED CMAKE_TOOLCHAIN_FILE AND CMAKE_TOOLCHAIN_FILE MATCHES ".*/scripts/buildsystems/vcpkg\\.cmake$")
			get_filename_component(_vcpkg_buildsystems_dir "${CMAKE_TOOLCHAIN_FILE}" DIRECTORY)
			get_filename_component(_vcpkg_scripts_dir "${_vcpkg_buildsystems_dir}" DIRECTORY)
			get_filename_component(_vcpkg_root_dir "${_vcpkg_scripts_dir}" DIRECTORY)
			list(APPEND _ionc_candidate_share_dirs "${_vcpkg_root_dir}/installed/${VCPKG_TARGET_TRIPLET}/share/ion-c")
		endif()

		if(DEFINED VCPKG_ROOT)
			list(APPEND _ionc_candidate_share_dirs "${VCPKG_ROOT}/installed/${VCPKG_TARGET_TRIPLET}/share/ion-c")
		endif()
		if(DEFINED ENV{VCPKG_ROOT})
			list(APPEND _ionc_candidate_share_dirs "$ENV{VCPKG_ROOT}/installed/${VCPKG_TARGET_TRIPLET}/share/ion-c")
		endif()

		set(_found_ionc_share_dir "")
		foreach(_candidate_dir IN LISTS _ionc_candidate_share_dirs)
			if(EXISTS "${_candidate_dir}/IonCConfig.cmake")
				set(_found_ionc_share_dir "${_candidate_dir}")
				break()
			endif()
		endforeach()

		if(_found_ionc_share_dir)
			find_package(IonC CONFIG REQUIRED PATHS "${_found_ionc_share_dir}" NO_DEFAULT_PATH)
		else()
			find_package(IonC CONFIG QUIET)
		endif()
	else()
		find_package(IonC CONFIG QUIET)
	endif()

	if(IonC_FOUND)
		if(TARGET IonC::ionc_static)
			set(_ionc_target IonC::ionc_static)
		elseif(TARGET IonC::ionc)
			set(_ionc_target IonC::ionc)
		endif()
		if(TARGET IonC::decNumber_static)
			set(_decnumber_target IonC::decNumber_static)
		elseif(TARGET IonC::decNumber)
			set(_decnumber_target IonC::decNumber)
		endif()
	endif()

	if(NOT _ionc_target)
		if(POLICY CMP0169)
			cmake_policy(SET CMP0169 OLD)
		endif()

		# FetchContent fallback for environments without vcpkg/system packages.
		#
		# Important: ion-c includes git submodules (bench deps like yyjson). We do not
		# build those components for the extension, and CI networking/DNS can be flaky,
		# so we disable submodule initialization entirely.
		FetchContent_Declare(
		    ionc
		    GIT_REPOSITORY https://github.com/amzn/ion-c.git
		    GIT_TAG v1.1.4
		    GIT_SHALLOW TRUE
		    GIT_SUBMODULES ""
		    GIT_SUBMODULES_RECURSE FALSE)

		FetchContent_GetProperties(ionc)
		if(NOT ionc_POPULATED)
			# Keep these settings scoped to ion-c by restoring them afterwards.
			set(_saved_build_shared_libs "${BUILD_SHARED_LIBS}")
			set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
			set(IONC_BUILD_TESTS OFF CACHE BOOL "" FORCE)

			FetchContent_Populate(ionc)
			execute_process(
			    COMMAND ${CMAKE_COMMAND} -DIONC_SOURCE_DIR=${ionc_SOURCE_DIR}
			            -P ${DUCKDB_ION_CMAKE_DIR}/patch_ionc_tools.cmake)
			add_subdirectory(${ionc_SOURCE_DIR} ${ionc_BINARY_DIR})
			set(DUCKDB_ION_IONC_FROM_FETCHCONTENT TRUE PARENT_SCOPE)

			set(BUILD_SHARED_LIBS "${_saved_build_shared_libs}" CACHE BOOL "" FORCE)
		endif()

		if(TARGET ionc_static)
			set(_ionc_target ionc_static)
		elseif(TARGET ionc)
			set(_ionc_target ionc)
		endif()
		if(TARGET decNumber_static)
			set(_decnumber_target decNumber_static)
		elseif(TARGET decNumber)
			set(_decnumber_target decNumber)
		endif()
	endif()

	if(NOT _ionc_target)
		message(FATAL_ERROR "IonC target not found (expected IonC::ionc[_static] or ionc[_static])")
	endif()

	set(${out_ionc_target} "${_ionc_target}" PARENT_SCOPE)
	set(${out_decnumber_target} "${_decnumber_target}" PARENT_SCOPE)
endfunction()
