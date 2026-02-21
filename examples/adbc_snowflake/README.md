# ADBC + Snowflake Example

Uses ADBC with key-pair auth to connect to Snowflake.

## Prerequisites

```bash
pip install adbc-driver-manager
dbc install snowflake
```

## Config

Copy `sidemantic.yaml.example` to `sidemantic.yaml` and fill in your Snowflake details.

## Usage

```bash
sidemantic query "SELECT orders.total_revenue, orders.region FROM orders"
```
