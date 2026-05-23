# Runtime Feature Matrix

This matrix documents current product support for native Sidemantic projects. It should be updated whenever a fixture category, Rust runtime path, DuckDB extension path, or WASM surface changes support level.

| Capability | Python | Rust native runtime | DuckDB extension | WASM |
|---|---:|---:|---:|---:|
| Native YAML load | Yes | Yes | Via load APIs | Yes |
| Native SQL definitions | Yes | Yes, fixture-covered compile | Partial | Partial |
| Native format `version: 1` | Yes | Yes | Inherited from Rust | Inherited from Rust |
| Native SQL-backed model source | Yes | Yes, fixture-covered compile | Partial | Partial |
| SQL frontmatter definitions | Yes | Yes, fixture-covered compile | Partial | Partial |
| YAML embedded SQL blocks | Yes | Yes, fixture-covered compile | No dedicated fixture yet | No dedicated fixture yet |
| `source_uri` model loading | Yes | Yes, validation fixture-covered; query generation rejects URI-only sources | No dedicated fixture yet | No dedicated fixture yet |
| Default time dimension | Yes | Yes, fixture-covered compile | No dedicated fixture yet | No dedicated fixture yet |
| Segments | Yes | Yes, fixture-covered compile | Partial | Partial |
| Metric filters | Yes | Yes, fixture-covered compile | No dedicated fixture yet | No dedicated fixture yet |
| Derived metrics | Yes | Yes, fixture-covered compile | Partial | Partial |
| Ratio metrics | Yes | Yes, fixture-covered compile | Partial | Partial |
| Relationships | Yes | Yes, fixture-covered compile | Partial | Partial |
| Composite joins | Yes | Yes, fixture-covered compile | No dedicated fixture yet | No dedicated fixture yet |
| Multi-hop joins | Yes | Yes, fixture-covered compile | No dedicated fixture yet | No dedicated fixture yet |
| Many-to-many joins | Yes | Yes, fixture-covered compile | No dedicated fixture yet | No dedicated fixture yet |
| Parameters in filters | Yes | Yes, fixture-covered compile | No dedicated fixture yet | No dedicated fixture yet |
| Symmetric aggregation | Yes | Yes, shared fixture result parity | No dedicated fixture yet | No dedicated fixture yet |
| Cumulative metrics | Yes | Yes, fixture-covered compile | No dedicated fixture yet | No dedicated fixture yet |
| Time comparison metrics | Yes | Yes, fixture-covered compile | No dedicated fixture yet | No dedicated fixture yet |
| Conversion metrics | Yes | Yes, fixture-covered compile | No dedicated fixture yet | No dedicated fixture yet |
| Retention metrics | Yes | Yes, fixture-covered compile | No dedicated fixture yet | No dedicated fixture yet |
| Cohort metrics | Yes | Yes, fixture-covered compile | No dedicated fixture yet | No dedicated fixture yet |
| Table calculations | Post-query processing | Yes, Rust fixture-covered compile and Rust-only result coverage | No dedicated fixture yet | No dedicated fixture yet |
| Pre-aggregation routing | Yes | Yes, fixture-covered compile | No dedicated fixture yet | No dedicated fixture yet |
| Semantic SQL rewrite | Yes | Native subset, fixture-covered | Native subset target | Narrow subset |
| DuckDB execution | Yes | Via ADBC, fixture result parity in CI | Native DuckDB process | No |
| SQLite execution | No named adapter | Via ADBC, feature-gated | No | No |
| PostgreSQL execution | Yes | Via ADBC, gated | No | No |
| External format import | Yes | No | No | No |
| Python semantic definition files | Yes | No | No | No |
| Notebook/widget UX | Yes | No | No | No |
| PostgreSQL wire server | Yes | No | No | No |

Support terms:

- `Yes`: shipped in the product path and covered by normal tests.
- `Partial`: implemented in some runtime paths but not yet complete enough to claim full product support.
- `Via ADBC, feature-gated`: implemented behind Rust feature flags and driver availability.
- `No dedicated fixture yet`: code may exist, but the native fixture suite does not yet enforce behavior.

The Rust runtime is scoped to native YAML and SQL projects. Python remains the external adapter and migration layer.
