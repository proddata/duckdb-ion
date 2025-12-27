vcpkg_buildpath_length_warning(37)
if(VCPKG_TARGET_IS_WINDOWS)
    vcpkg_check_linkage(ONLY_STATIC_LIBRARY)
endif()

vcpkg_from_github(
    OUT_SOURCE_PATH SOURCE_PATH
    REPO amzn/ion-c
    REF v1.1.4
    SHA512 2d7e0deff4ab83cc3a153ad8249f30025ca6cc44541c52cba56876ea355c830984aae78a88081f55c9e8c42d86e70e67d573ed2a652d2c965d286a0e8fe86ec4
    PATCHES
        disable-tests-tools.patch
)

vcpkg_configure_cmake(
    SOURCE_PATH "${SOURCE_PATH}"
    OPTIONS
        -DBUILD_SHARED_LIBS=OFF
        -DIONC_BUILD_TESTS=OFF
)

vcpkg_install_cmake()
vcpkg_copy_pdbs()

vcpkg_cmake_config_fixup(CONFIG_PATH lib/cmake/IonC)

file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/include")

file(INSTALL "${SOURCE_PATH}/LICENSE" DESTINATION "${CURRENT_PACKAGES_DIR}/share/${PORT}" RENAME copyright)
