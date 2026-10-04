# Updating DuckDB compatibility

The extension builds against DuckDB's internal C++ API. A loadable extension must
match its host version and platform; source compatibility is not binary compatibility.

The Makefile selects stable DuckDB 1.5.6 or a pinned Cyanoptera development commit.
CI builds both and runs the SQL and SemanticInput host suites. The development job
sets `SIDEMANTIC_NATIVE_PEG=1` to require tests that disable parser overrides and use
Sidemantic's registered native grammar. Release packaging intentionally accepts only
the stable version and currently produces unsigned Linux amd64 artifacts.

When updating:

1. Change the stable version or `DUCKDB_NEXT_COMMIT` in the Makefile and the CI matrix
   as appropriate. Keep the release workflow's stable allowlist and input tests aligned.
2. Use fresh build directories for each host. `src/include/sidemantic_compat.hpp.in`
   is generated from the API probes in CMake; adapt those probes and frontend code
   when upstream interfaces change.
3. Build and run the extension SQL suites, including transactions, restart, legacy
   migration, routing and native PEG tests. Run `test/test_semantic_input_host.py`
   against the freshly built shell and extension.
4. Update the README with the verified versions and distribution scope. Do not
   infer additional platform support or signed/community availability from compilation.

The vendored extension-ci-tools supplies the build harness. Update it only when
changes to that harness are required by the selected DuckDB versions.
