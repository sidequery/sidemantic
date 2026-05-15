# External Power BI Fixture Sources

This directory contains small, permissively licensed Power BI/TMDL/DAX fixtures used to exercise real exported syntax.

Only source text needed by tests is copied: `.tmdl` files, one DAX text file, and upstream license files. PBIX binaries, report JSON, images, generated app code, and data files are intentionally excluded.

Trailing whitespace was stripped from copied text files to keep patches clean; no semantic TMDL or DAX content was changed.

| Fixture | Upstream | License | Commit | Copied paths |
| --- | --- | --- | --- | --- |
| `microsoft-analysis-services-sales` | <https://github.com/microsoft/Analysis-Services> | MIT | `61ee41607dfb0fa50378165fdb0fc03042c0ef17` | `pbidevmode/fabricps-pbip/SamplePBIP/Sales.SemanticModel/**/*.tmdl` |
| `microsoft-fabric-samples-bank-customer-churn` | <https://github.com/microsoft/fabric-samples> | MIT | `6107067b0152392f87e10704b5c645d2c1123818` | `docs-samples/data-science/enrich-powerbi-report-with-machine-learning/Bank Customer Churn Analysis/Bank Customer Churn Analysis/Bank Customer Churn Analysis.SemanticModel/**/*.tmdl` |
| `pbi-tools-adventureworks-dw2020` | <https://github.com/pbi-tools/adventureworksdw2020-pbix> | MIT | `c47fefe4dc48df6461fc45b0442910b5b95f193d` | `pbix/Model/**/*.tmdl` |
| `pbip-lineage-explorer-sample` | <https://github.com/JonathanJihwanKim/pbip-lineage-explorer> | MIT | `ccced0cdaa58822eff76e4b0f17a5b4bc0678080` | `public/sample-pbip/**/*.tmdl` |
| `ruiromano-pbip-demo-agentic-model01` | <https://github.com/RuiRomano/pbip-demo-agentic> | MIT | `2c573dfeb90a4d9983ebcbc340642a8126597605` | `.resources/pbip-sample/Model01.SemanticModel/**/*.tmdl` |
| `marfolger-powerbi-dax` | <https://github.com/MarFolger25/PowerBI_DAX> | MIT | `2773ab5713a800e7c6243f97995440112a93bda6` | `business_logic_DAX.txt` |

Each fixture subdirectory contains `LICENSE.upstream` copied from the source repository.
