vcpkg_buildpath_length_warning(37)
if(VCPKG_TARGET_IS_WINDOWS)
    vcpkg_check_linkage(ONLY_STATIC_LIBRARY)
endif()

vcpkg_from_github(
    OUT_SOURCE_PATH SOURCE_PATH
    REPO amzn/ion-c
    REF v1.4.0
    SHA512 9c51679f1396a77504bf7f478fdac2a28c505525419750778eab83fd0926f1be5abdcbe5fca8c1ed94551934ebb75b6ef3ce34c530a9e7ec15f2c1ed60283524
    PATCHES
        disable-tests-tools.patch
)

vcpkg_configure_cmake(
    SOURCE_PATH "${SOURCE_PATH}"
    OPTIONS
        -DBUILD_SHARED_LIBS=OFF
)

vcpkg_install_cmake()
vcpkg_copy_pdbs()

vcpkg_cmake_config_fixup(CONFIG_PATH lib/cmake/IonC)

file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/include")

file(INSTALL "${SOURCE_PATH}/LICENSE" DESTINATION "${CURRENT_PACKAGES_DIR}/share/${PORT}" RENAME copyright)
