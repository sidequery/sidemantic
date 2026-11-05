//! Offline evaluator for the assertion vocabulary used by the immutable Ossie
//! logical schemas. This is deliberately not a general JSON Schema API: every
//! keyword in every embedded schema is checked before evaluation, and references
//! resolve only within that embedded document. No network or optional runtime.
use once_cell::sync::Lazy;
use serde_json::Value;

use super::ossie::{diagnostic, OssieDiagnostic, OssieProfile};

const LEGACY: &str = include_str!("../../schemas/ossie/0.2.0.dev0.json");
const CURRENT: &str = include_str!("../../schemas/ossie/0.2.0.dev0-b6c702e.json");
const LATEST: &str = include_str!("../../schemas/ossie/0.2.0.dev0-172a3e5.json");
const STABLE: &str = include_str!("../../schemas/ossie/0.1.1.json");

static SCHEMAS: Lazy<Vec<Result<Value, String>>> = Lazy::new(|| {
    [STABLE, LEGACY, CURRENT, LATEST]
        .iter()
        .map(|text| {
            let schema: Value = serde_json::from_str(text).map_err(|error| error.to_string())?;
            check_schema(&schema, &schema)?;
            Ok(schema)
        })
        .collect()
});

fn schema(profile: &OssieProfile) -> &Result<Value, String> {
    &SCHEMAS[match profile.schema_revision.as_deref() {
        Some(super::ossie::LATEST_SCHEMA_REVISION) => 3,
        Some(super::ossie::CURRENT_SCHEMA_REVISION) => 2,
        _ if profile.validation_schema_version == "0.1.1" => 0,
        _ => 1,
    }]
}

fn check_schema(schema: &Value, root: &Value) -> Result<(), String> {
    let Some(object) = schema.as_object() else {
        return Err("Embedded Ossie schema must be an object".into());
    };
    for (key, value) in object {
        match key.as_str() {
            "$schema" | "$id" | "title" | "description" | "examples" | "deprecated" | "type"
            | "const" | "enum" | "required" | "minLength" | "minItems" => {}
            "$ref" => {
                let reference = value
                    .as_str()
                    .and_then(|s| s.strip_prefix('#'))
                    .ok_or_else(|| "Only local pinned schema references are allowed".to_string())?;
                if root.pointer(reference).is_none() {
                    return Err(format!("Unresolved pinned schema reference {reference}"));
                }
            }
            "properties" | "$defs" => {
                for child in value.as_object().ok_or("Invalid schema map")?.values() {
                    check_schema(child, root)?;
                }
            }
            "items" => check_schema(value, root)?,
            "additionalProperties" if value.is_boolean() => {}
            "anyOf" | "oneOf" => {
                for child in value.as_array().ok_or("Invalid schema alternatives")? {
                    check_schema(child, root)?;
                }
            }
            _ => return Err(format!("Unsupported pinned schema assertion {key:?}")),
        }
    }
    Ok(())
}

fn pointer(parent: &str, key: &str) -> String {
    format!("{parent}/{}", key.replace('~', "~0").replace('/', "~1"))
}

fn evaluate(
    value: &Value,
    schema: &Value,
    root: &Value,
    path: &str,
    errors: &mut Vec<OssieDiagnostic>,
) {
    if let Some(reference) = schema.get("$ref").and_then(Value::as_str) {
        evaluate(
            value,
            root.pointer(&reference[1..])
                .expect("checked pinned reference"),
            root,
            path,
            errors,
        );
    }
    if let Some(kind) = schema.get("type").and_then(Value::as_str) {
        let matches = match kind {
            "object" => value.is_object(),
            "array" => value.is_array(),
            "string" => value.is_string(),
            "boolean" => value.is_boolean(),
            "null" => value.is_null(),
            "number" => value.is_number(),
            "integer" => value.is_i64() || value.is_u64(),
            _ => false,
        };
        if !matches {
            errors.push(diagnostic(
                "ossie.schema.type",
                format!("Expected {kind}."),
                path,
                None,
            ));
            return;
        }
    }
    if schema
        .get("const")
        .is_some_and(|expected| value != expected)
    {
        errors.push(diagnostic(
            "ossie.schema.const",
            "Value differs from the pinned constant.",
            path,
            None,
        ));
    }
    if schema
        .get("enum")
        .and_then(Value::as_array)
        .is_some_and(|items| !items.contains(value))
    {
        errors.push(diagnostic(
            "ossie.schema.enum",
            "Value is outside the pinned enumeration.",
            path,
            None,
        ));
    }
    if let (Some(text), Some(min)) = (
        value.as_str(),
        schema.get("minLength").and_then(Value::as_u64),
    ) {
        if text.chars().count() < min as usize {
            errors.push(diagnostic(
                "ossie.schema.min_length",
                format!("Expected at least {min} characters."),
                path,
                None,
            ));
        }
    }
    if let Some(items) = value.as_array() {
        if let Some(min) = schema.get("minItems").and_then(Value::as_u64) {
            if items.len() < min as usize {
                errors.push(diagnostic(
                    "ossie.schema.min_items",
                    format!("Expected at least {min} items."),
                    path,
                    None,
                ));
            }
        }
        if let Some(item_schema) = schema.get("items") {
            for (i, item) in items.iter().enumerate() {
                evaluate(
                    item,
                    item_schema,
                    root,
                    &pointer(path, &i.to_string()),
                    errors,
                );
            }
        }
    }
    if let Some(object) = value.as_object() {
        if let Some(required) = schema.get("required").and_then(Value::as_array) {
            for name in required.iter().filter_map(Value::as_str) {
                if !object.contains_key(name) {
                    errors.push(diagnostic(
                        "ossie.schema.required",
                        format!("Required property {name:?} is missing."),
                        path,
                        None,
                    ));
                }
            }
        }
        let properties = schema.get("properties").and_then(Value::as_object);
        for (key, child) in object {
            if let Some(child_schema) = properties.and_then(|props| props.get(key)) {
                evaluate(child, child_schema, root, &pointer(path, key), errors);
            } else if schema.get("additionalProperties") == Some(&Value::Bool(false)) {
                errors.push(diagnostic(
                    "ossie.schema.additional_properties",
                    format!("Unexpected property {key:?}."),
                    path,
                    None,
                ));
            }
        }
    }
    for keyword in ["anyOf", "oneOf"] {
        if let Some(alternatives) = schema.get(keyword).and_then(Value::as_array) {
            let valid = alternatives
                .iter()
                .filter(|alternative| {
                    let mut branch_errors = Vec::new();
                    evaluate(value, alternative, root, path, &mut branch_errors);
                    branch_errors.is_empty()
                })
                .count();
            if valid == 0 || (keyword == "oneOf" && valid != 1) {
                errors.push(diagnostic(
                    if keyword == "oneOf" {
                        "ossie.schema.one_of"
                    } else {
                        "ossie.schema.any_of"
                    },
                    format!("Value does not satisfy {keyword} alternatives."),
                    path,
                    None,
                ));
            }
        }
    }
}

pub(super) fn validate(
    document: &Value,
    profile: &OssieProfile,
    errors: &mut Vec<OssieDiagnostic>,
) {
    let schema = match schema(profile) {
        Ok(schema) => schema,
        Err(message) => {
            errors.push(diagnostic(
                "ossie.schema.unsupported_assertion",
                message,
                "",
                None,
            ));
            return;
        }
    };
    let mut document = document.clone();
    if profile.compatibility_alias_for.is_some() {
        document["version"] = Value::String(profile.validation_schema_version.clone());
    }
    evaluate(&document, schema, schema, "", errors);
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn every_pinned_logical_assertion_and_reference_is_supported() {
        for text in [STABLE, LEGACY, CURRENT, LATEST] {
            let schema: Value = serde_json::from_str(text).unwrap();
            check_schema(&schema, &schema).unwrap();
        }
    }
    #[test]
    fn unexpected_keywords_and_remote_references_fail_closed() {
        for schema in [
            serde_json::json!({"maxLength": 3}),
            serde_json::json!({"$ref":"https://example.invalid/schema"}),
            serde_json::json!({"$ref":"#/$defs/missing"}),
        ] {
            assert!(check_schema(&schema, &schema).is_err());
        }
    }
}
