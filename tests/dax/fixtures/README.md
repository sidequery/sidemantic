# DAX Fixture Sources

This directory contains DAX examples and lexer keyword lists sourced from permissively licensed open-source projects.
Each subdirectory includes the upstream LICENSE file.

Sources
- pbi_parsers (MIT license)
  - Repo: https://github.com/douglassimonsen/pbi_parsers
  - Commit: 3b6aba9ff4f3a1a523ae79da7c8cb19d57e6f831
  - Files used: `docs/docs/index.md` examples (DAX expressions).

- PyDAXLexer (MIT license)
  - Repo: https://github.com/jurgenfolz/PyDAXLexer
  - Commit: 3fec0fbe80777fa98b652efb62d677d2930fd997
  - Files used: `resources/sample_dax_expressions/*`, `tests/*.py`, `main.py` (DAX expressions).

- TabularEditor (MIT license)
  - Repo: https://github.com/TabularEditor/TabularEditor
  - Commit: 9d3456cfdf05aac16bb73131cc4c34f3dcd62d93
  - Files used: `AntlrGrammars/DAXLexer.g4` (keyword list).

- query-docs (CC BY 4.0 for docs, MIT for code)
  - Repo: https://github.com/MicrosoftDocs/query-docs
  - Commit: b1008faf12c519f7b649cd492ec83d98914c07fc
  - Files used: `query-languages/dax/dax-queries.md` (DAX query examples).

Notes
- Expressions and queries are stored as blocks separated by `---`.
- Only ASCII expressions were included to keep fixtures portable.
- Duplicates were removed.
- Expressions containing standalone `=>` function definitions were excluded for now.
