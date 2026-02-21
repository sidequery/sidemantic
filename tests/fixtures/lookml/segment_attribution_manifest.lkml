# Source: EffinAmazing/segment_attribution (MIT License)
# File: manifest.lkml
# Tests: constants for schema, event, value, domain

project_name: "attribution"

constant: segment_schema_name {
  value: "your_segment_schema"
}

constant: conversion_event {
  value: "your_conversion_event_slug"
}

constant: conversion_value {
  value: "your_conversion_value_field"
}

constant: site_domain {
  value: "your_domain.com"
}
