# It is necessary to pass this flag to AWS C++ SDK to enable compatibility with VS2019 C++ stdlib
set(VCPKG_CXX_FLAGS "/D_DISABLE_CONSTEXPR_MUTEX_CONSTRUCTOR")
# If VCPKG_CXX_FLAGS is set, VCPKG_C_FLAGS must be set
set(VCPKG_C_FLAGS "")

# The following is copied from x64-windows-static-md-release.cmake
set(VCPKG_TARGET_ARCHITECTURE x64)
set(VCPKG_CRT_LINKAGE dynamic)
set(VCPKG_LIBRARY_LINKAGE static)

set(VCPKG_BUILD_TYPE release)
