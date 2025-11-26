FROM alpine:3

###
# Base image setup
###

# Setup the basic necessities
RUN apk update --y -qq
RUN apk add -qq ccache cmake git ninja ninja-build clang19 gcc libssl3 wget bash zip gettext unzip build-base curl make libffi-dev zlib openssh autoconf linux-headers libunwind-dev jq libtool
RUN wget https://dl-cdn.alpinelinux.org/alpine/v3.21/community/x86_64/aws-cli-2.22.10-r0.apk
RUN apk add --allow-untrusted aws-cli-2.22.10-r0.apk

# Setup VCPKG n a mounted volume TODO: figure out how to cache this
ARG vcpkg_url
ARG vcpkg_commit
RUN mkdir /vcpkg && \
    cd /vcpkg && \
    git init && \
    git remote add origin $vcpkg_url && \
    git fetch origin $vcpkg_commit && \
    git checkout $vcpkg_commit && \
    VCPKG_FORCE_SYSTEM_BINARIES=1 ./bootstrap-vcpkg.sh
ENV VCPKG_ROOT=/vcpkg
ENV VCPKG_TOOLCHAIN_PATH=/vcpkg/scripts/buildsystems/vcpkg.cmake
ENV PATH="${VCPKG_ROOT}:${PATH}"

# Add ninja to path
ENV PATH="/usr/lib/ninja-build/bin:${PATH}"

# Common environment variables
ENV GEN=ninja
ENV DUCKDB_PLATFORM=linux_amd64_musl
ENV VCPKG_FORCE_SYSTEM_BINARIES=1

# Specify where we expect the extension to be mounted and use that as working dir
VOLUME /duckdb_build_dir
WORKDIR /duckdb_build_dir

# Mount for ccache to allow restoring ccache in GH actions
VOLUME /ccache_dir
ENV CCACHE_DIR=/ccache_dir
ENV CCACHE_COMPRESS=TRUE
ENV CCACHE_COMPRESSLEVEL=6
ENV CCACHE_MAXSIZE=400M

# Allow git access to the mounted volume
RUN git config --global --add safe.directory "*"

###
# Conditionally configure some extra dependencies
###
# a `;` separated list of extra toolchains to install (passed in like this to makes things easier through GitHub Actions)
# Note that it should start and end with a `;` e.g. `;rust;parser_tools;`
ARG extra_toolchains

# NOTE: the weird case conditionals are because of bash limitations in the ubuntu image used (see https://stackoverflow.com/questions/229551/how-to-check-if-a-string-contains-a-substring-in-bash)

# Install Parser tools
RUN case "$extra_toolchains" in \
  *\;parser_tools\;*) \
    apk add -qq bison flex \
  ;; \
esac

# Install Fortran
RUN case "$extra_toolchains" in \
  *\;fortran\;*) \
    apk add -qq gfortran \
  ;; \
esac

# Configure Rust
RUN case "$extra_toolchains" in \
  *\;rust\;*) \
    curl https://sh.rustup.rs -sSf | bash -s -- -y \
  ;; \
esac
ENV PATH="/root/.cargo/bin:${PATH}"

# Configure go
RUN case "$extra_toolchains" in \
  *\;go\;*) \
    apk add -qq go \
  ;; \
esac
ENV PATH="/usr/local/go/bin:${PATH}"

# Install Python3
RUN case "$extra_toolchains" in \
  *\;python3\;*) \
    apk add -qq python3 \
  ;; \
esac
