# Source: EffinAmazing/segment_attribution (MIT License)
# File: attribution.model.lkml
# Tests: model with explores and joins, connection, includes

connection: "st"

include: "/views/*.view.lkml"
include: "*.dashboard"

explore: sessions {}

explore: conversion_attribution {
  label: "Conversion Attribution"
  join: conversion_event {
    sql_on: ${conversion_event.id} = ${conversion_attribution.conversion_id} ;;
    relationship: many_to_one
  }
  join: sessions {
    sql_on: ${sessions.id} = ${conversion_attribution.session_id} ;;
    relationship: many_to_one
  }
}
