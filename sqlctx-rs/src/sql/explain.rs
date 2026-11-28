//! Opt-in compiler observations. Events are recorded at planning decisions,
//! never inferred from generated SQL or a second approximation of the planner.

use std::cell::RefCell;

use serde_json::{json, Value};

use crate::error::Result;
use crate::sql::SemanticQuery;

thread_local! {
    static EVENTS: RefCell<Option<Vec<Value>>> = const { RefCell::new(None) };
}

/// The caller must enter the protected semantic stack before capture so all
/// nested generators run on this thread. Drop restores state even on errors.
pub(crate) fn capture<T>(operation: impl FnOnce() -> Result<T>) -> Result<(T, Vec<Value>)> {
    struct Restore(Option<Vec<Value>>);
    impl Drop for Restore {
        fn drop(&mut self) {
            EVENTS.with(|events| *events.borrow_mut() = self.0.take());
        }
    }
    let previous = EVENTS.with(|events| events.replace(Some(Vec::new())));
    let _restore = Restore(previous);
    let result = operation()?;
    let events = EVENTS.with(|events| events.borrow_mut().take().unwrap_or_default());
    Ok((result, events))
}

pub(super) fn record(event: impl FnOnce() -> Value) {
    EVENTS.with(|events| {
        if let Some(events) = events.borrow_mut().as_mut() {
            events.push(event());
        }
    });
}

pub(super) fn query(query: &SemanticQuery) {
    record(|| {
        json!({
            "kind": "query", "metrics": query.metrics, "dimensions": query.dimensions,
            "ungrouped": query.ungrouped, "filters": query.filters, "segments": query.segments,
            "population_anchor": query.consumption_base_model,
            "rollup_routing_enabled": query.use_preaggregations,
        })
    });
}

pub(super) fn join(step: &crate::core::JoinStep, kind: &str) {
    let effect = match kind {
        "INNER JOIN" => "keep matching rows on both sides",
        "LEFT JOIN" => "preserve left rows; unmatched right fields are null",
        "RIGHT JOIN" => "preserve right rows; unmatched left fields are null",
        "FULL JOIN" => "preserve unmatched rows on both sides",
        "CROSS JOIN" => "cartesian population; an empty side removes all rows",
        _ => "see compiled SQL",
    };
    record(|| {
        json!({
            "kind": "join", "from": step.from_model, "to": step.to_model,
            "edge_id": step.edge_id, "relationship": step.relationship_type,
            "from_keys": step.from_keys, "to_keys": step.to_keys,
            "condition": step.custom_condition, "join_type": kind,
            "population_effect": effect, "expands_left_grain": step.causes_fan_out(),
        })
    });
}
