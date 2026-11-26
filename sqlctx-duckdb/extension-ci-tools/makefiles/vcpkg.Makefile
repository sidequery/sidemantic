#### Setup VCPKG to correct version 2025.12.12 tag is 84bab45d415d22042bd0b9081aea57f362da3f35
vcpkg/scripts/buildsystems/vcpkg.cmake:
	git -C vcpkg fetch || git clone --depth 1 --branch 2025.12.12 https://github.com/microsoft/vcpkg
	cd vcpkg && ./bootstrap-vcpkg.sh

setup-vcpkg: vcpkg/scripts/buildsystems/vcpkg.cmake
	@echo 'Consider exporting VCPKG_TOOLCHAIN_PATH=$(PWD)/vcpkg/scripts/buildsystems/vcpkg.cmake'

cleanup-vcpkg:
	rm -rf vcpkg
