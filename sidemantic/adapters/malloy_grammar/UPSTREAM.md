# Malloy grammar provenance

The bundled grammar is pinned to Malloy [`v0.0.407`](https://github.com/malloydata/malloy/releases/tag/v0.0.407), commit [`cf1f6a6449562bd9b74fc14936f47af294c3a325`](https://github.com/malloydata/malloy/commit/cf1f6a6449562bd9b74fc14936f47af294c3a325). `UPSTREAM.json` records the immutable upstream paths and checksums, the vendored checksums, and the ANTLR generator version.

`MalloyParser.g4` is an exact upstream copy. `MalloyLexer.g4` preserves the same token grammar but adapts upstream TypeScript-target members and embedded lexer actions to the Python target. The generated Python lexer, parser, listener, visitor, token, and interpreter files were produced with ANTLR 4.13.2.

Run the offline bundle drift check after any grammar or generated-file change:

```bash
uv run --frozen scripts/check_malloy_grammar_drift.py
```

To also verify the recorded upstream checksums against the pinned immutable commit:

```bash
uv run --frozen scripts/check_malloy_grammar_drift.py --verify-upstream
```

An intentional update must pin a specific upstream release and commit, review and preserve the Python-target lexer adaptations, regenerate every ANTLR artifact with the recorded generator version, and update `UPSTREAM.json` in the same change.
