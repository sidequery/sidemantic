//! Resolve raw declarations before projecting away policy and importer fields.

use std::collections::{HashMap, HashSet};

use serde_json::{Map, Value};

use super::{invalid, object, DESCRIPTIVE_FIELDS};
use crate::error::Result;

pub(super) fn resolve(models: &[Value]) -> Result<Vec<Value>> {
    let mut declarations = HashMap::new();
    let mut names = Vec::new();
    for (index, model) in models.iter().enumerate() {
        let path = format!("models[{index}]");
        let raw = object(model.clone(), &path)?;
        let name = raw
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid(&path, "model requires a name"))?
            .to_owned();
        if declarations.insert(name.clone(), raw).is_some() {
            return Err(invalid(&path, format!("duplicate model '{name}'")));
        }
        names.push(name);
    }
    let mut resolved = HashMap::new();
    let mut visiting = HashSet::new();
    for name in &names {
        resolve_model(name, &declarations, &mut resolved, &mut visiting)?;
    }
    Ok(names
        .into_iter()
        .map(|name| Value::Object(resolved.remove(&name).unwrap()))
        .collect())
}

fn resolve_model(
    name: &str,
    declarations: &HashMap<String, Map<String, Value>>,
    resolved: &mut HashMap<String, Map<String, Value>>,
    visiting: &mut HashSet<String>,
) -> Result<Map<String, Value>> {
    if let Some(model) = resolved.get(name) {
        return Ok(model.clone());
    }
    let path = format!("models.{name}.extends");
    let mut child = declarations
        .get(name)
        .cloned()
        .ok_or_else(|| invalid(&path, format!("unknown parent model '{name}'")))?;
    if !visiting.insert(name.to_owned()) {
        return Err(invalid(&path, "circular model inheritance"));
    }
    let model = match child.remove("extends") {
        None | Some(Value::Null) => child,
        Some(Value::String(parent)) => {
            let parent = resolve_model(&parent, declarations, resolved, visiting)?;
            merge(child, parent, name)?
        }
        Some(_) => return Err(invalid(&path, "expected a parent model name")),
    };
    visiting.remove(name);
    resolved.insert(name.to_owned(), model.clone());
    Ok(model)
}

fn merge(
    mut child: Map<String, Value>,
    mut parent: Map<String, Value>,
    name: &str,
) -> Result<Map<String, Value>> {
    // Match Python merge_model: named collections override by name; invariant
    // filters always accumulate, and the parent's security remains authoritative.
    for field in [
        "dimensions",
        "metrics",
        "relationships",
        "segments",
        "pre_aggregations",
        "invariant_filters",
    ] {
        let path = format!("models.{name}.{field}");
        let mut items = Vec::<Value>::new();
        let mut positions = HashMap::<String, usize>::new();
        for value in [parent.remove(field), child.remove(field)]
            .into_iter()
            .flatten()
        {
            let entries = value
                .as_array()
                .ok_or_else(|| invalid(&path, "expected an array"))?;
            for entry in entries {
                if field == "invariant_filters" {
                    items.push(entry.clone());
                    continue;
                }
                let item_name = entry
                    .get("name")
                    .and_then(Value::as_str)
                    .ok_or_else(|| invalid(&path, "collection item requires a name"))?;
                if let Some(position) = positions.get(item_name) {
                    items[*position] = entry.clone();
                } else {
                    positions.insert(item_name.to_owned(), items.len());
                    items.push(entry.clone());
                }
            }
        }
        parent.insert(field.to_owned(), Value::Array(items));
    }
    for field in [
        "name",
        "table",
        "sql",
        "source_uri",
        "description",
        "primary_key",
        "unique_keys",
        "default_time_dimension",
        "default_grain",
        "freshness",
        "metadata",
        "auto_dimensions",
        "schema_exposure",
        "meta",
    ] {
        if let Some(value) = child.remove(field) {
            parent.insert(field.to_owned(), value);
        }
    }
    // Python retains these fields from the parent, including inherited policy.
    // Preserve unexpected child fields so the normal decoder still rejects them.
    for (field, value) in child {
        if !matches!(field.as_str(), "security" | "dax" | "expression_language")
            && !DESCRIPTIVE_FIELDS.contains(&field.as_str())
        {
            parent.insert(field, value);
        }
    }
    Ok(parent)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn inheritance_is_transitive_and_keeps_parent_policy_and_named_order() {
        let models = vec![
            json!({"name":"leaf", "extends":"child", "invariant_filters":["amount < 100"]}),
            json!({"name":"base", "table":"sales", "primary_key":"id",
                "security":{"row_filters":["tenant = 1"]},
                "invariant_filters":["not deleted"],
                "dimensions":[{"name":"id"},{"name":"amount","sql":"raw_amount"}]}),
            json!({"name":"child", "extends":"base", "primary_key":null,
                "security":null, "invariant_filters":["amount > 10"],
                "dimensions":[{"name":"amount","sql":"net_amount"},{"name":"region"}]}),
        ];
        let output = resolve(&models).unwrap();
        let leaf = &output[0];
        assert_eq!(leaf["name"], "leaf");
        assert_eq!(leaf["table"], "sales");
        assert!(leaf["primary_key"].is_null());
        assert_eq!(leaf["security"], models[1]["security"]);
        assert_eq!(
            leaf["invariant_filters"],
            json!(["not deleted", "amount > 10", "amount < 100"])
        );
        assert_eq!(
            leaf["dimensions"],
            json!([{"name":"id"},{"name":"amount","sql":"net_amount"},{"name":"region"}])
        );
        assert!(leaf.get("extends").is_none());
    }

    #[test]
    fn invalid_inheritance_fails_with_validation_errors() {
        for models in [
            vec![json!({"name":"a", "extends":"missing"})],
            vec![
                json!({"name":"a", "extends":"b"}),
                json!({"name":"b", "extends":"a"}),
            ],
            vec![json!({"name":"a", "extends":5})],
            vec![json!({"name":"a"}), json!({"name":"a"})],
        ] {
            assert!(matches!(
                resolve(&models),
                Err(crate::error::SidemanticError::ValidationIssue { .. })
            ));
        }
    }
}
