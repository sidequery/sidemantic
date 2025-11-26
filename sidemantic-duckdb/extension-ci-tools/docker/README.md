# DuckDB docker images
DuckDB uses Docker images to build linux binaries in a flexible and reproducible way. These images can be used 
to compile a DuckDB binary (both extensions and the duckdb shell). For more details on how to use these Dockerfiles,
check out the `.github/workflows/_extension_distribution.yml` workflow, which invokes the dockerfiles as part of the 
build process.