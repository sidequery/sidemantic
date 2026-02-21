# GoodData Fixture Sources

## gooddata/gooddata-python-sdk (MIT License)

Source: https://github.com/gooddata/gooddata-python-sdk

- `sdk_declarative_ldm.json` -- `packages/gooddata-sdk/tests/catalog/expected/declarative_ldm.json`
  6 datasets, star schema, GEO labels, aggregatedFacts, 1 SQL dataset (dict-style sql field)

- `sdk_declarative_ldm_with_sql_dataset.json` -- `packages/gooddata-sdk/tests/catalog/expected/declarative_ldm_with_sql_dataset.json`
  7 datasets, newer reference format, isNullable facts, 2 SQL datasets (dict-style sql field)

- `sdk_declarative_analytics_model.json` -- `packages/gooddata-sdk/tests/catalog/expected/declarative_analytics_model.json`
  24 MAQL metrics, analytics dashboards, visualization objects (not LDM, for future analytics support)

## gooddata/gooddata-public-demos (BSD-3-Clause License)

Source: https://github.com/gooddata/gooddata-public-demos

- `ecommerce_demo_ldm.json` -- `ecommerce-demo/workspaces/demo/ldm.json`
  6 datasets, 5 date dimensions, GEO_LATITUDE/GEO_LONGITUDE labels, returns + inventory models

- `ecommerce_demo_analytics.json` -- `ecommerce-demo/workspaces/demo/workspaceAnalytics.json`
  60 MAQL metrics (RSQ, CORREL, rank, PreviousPeriod), dashboards (not LDM, for future analytics support)

## Existing hand-crafted fixtures

- `cloud_ldm.json` -- minimal cloud LDM (customers, orders, date)
- `cloud_kitchen_sink.json` -- cloud LDM with fields, tablePath, multi-grain, many_to_many
- `legacy_project_model.json` -- minimal legacy projectModel format
- `legacy_kitchen_sink.json` -- legacy with multiple labels, custom fields
