//! Model access gates and request-specific row-filter rendering.
//!
//! Attribute values stay native during template evaluation. Only rendered output
//! expressions become SQL literals, so conditionals retain their usual truthiness
//! while a string attribute cannot escape into SQL syntax.

use minijinja::{value::ValueKind, Environment, Error, ErrorKind, UndefinedBehavior};
use once_cell::sync::Lazy;
use polyglot_sql::{DialectType, Expression};
use regex::{Captures, Regex};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::HashMap;
use thiserror::Error as ThisError;

/// Trusted request state prepared by the semantic boundary, never deserialized
/// from query JSON. Predicates are already expanded for their source CTE.
#[doc(hidden)]
#[derive(Debug, Clone, Default)]
pub struct PreparedPolicies {
    pub row_filters: HashMap<String, Vec<String>>,
    pub invariant_filters: HashMap<String, Vec<String>>,
}

impl PreparedPolicies {
    pub fn has_row_filters(&self) -> bool {
        self.row_filters.values().any(|filters| !filters.is_empty())
    }

    pub fn model_names(&self) -> impl Iterator<Item = &String> {
        self.row_filters.keys().chain(self.invariant_filters.keys())
    }

    pub fn filters_for_model(&self, model: &str) -> impl Iterator<Item = &String> {
        self.row_filters
            .get(model)
            .into_iter()
            .flatten()
            .chain(self.invariant_filters.get(model).into_iter().flatten())
    }
}

/// A literal access decision or a Jinja expression over the `user` namespace.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum AccessRule {
    Bool(bool),
    Expression(String),
}

impl Default for AccessRule {
    fn default() -> Self {
        Self::Bool(true)
    }
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SecurityPolicy {
    #[serde(default)]
    pub access: AccessRule,
    #[serde(default)]
    pub row_filters: Vec<String>,
}

/// Policy failure is a denial, never an unsupported-feature fallback.
#[derive(Debug, Clone, PartialEq, ThisError)]
pub enum PolicyError {
    #[error(
        "Model '{model}' declares a security policy but the query supplied no user_attributes; pass user_attributes (use {{}} for an empty attribute set)."
    )]
    MissingUserAttributes { model: String },
    #[error("Access denied to model '{model}' by its security policy.")]
    AccessDenied { model: String },
    #[error("Access expression {expression:?} failed to evaluate: {detail}")]
    AccessExpression { expression: String, detail: String },
    #[error("Row filter {template:?} failed to render: {detail}")]
    RowFilter { template: String, detail: String },
}

impl SecurityPolicy {
    /// Check access and render predicates for one participating model.
    ///
    /// The caller must parse and place these predicates inside this model's own
    /// source CTE before joins and aggregation. No SQL or graph mutation happens
    /// during evaluation.
    pub fn render_for_model(
        &self,
        model_name: &str,
        user_attributes: Option<&Map<String, Value>>,
    ) -> Result<Vec<String>, PolicyError> {
        self.render_for_model_with_dialect(model_name, user_attributes, DialectType::DuckDB)
    }

    pub(crate) fn render_for_model_with_dialect(
        &self,
        model_name: &str,
        user_attributes: Option<&Map<String, Value>>,
        dialect: DialectType,
    ) -> Result<Vec<String>, PolicyError> {
        let attributes = user_attributes.ok_or_else(|| PolicyError::MissingUserAttributes {
            model: model_name.to_owned(),
        })?;
        if !evaluate_access(&self.access, Some(attributes))? {
            return Err(PolicyError::AccessDenied {
                model: model_name.to_owned(),
            });
        }
        self.row_filters
            .iter()
            .map(|template| render_row_filter_in_dialect(template, attributes, dialect))
            .collect()
    }
}

pub fn evaluate_access(
    access: &AccessRule,
    user_attributes: Option<&Map<String, Value>>,
) -> Result<bool, PolicyError> {
    let AccessRule::Expression(source) = access else {
        return Ok(matches!(access, AccessRule::Bool(true)));
    };
    let mut expression_source = source.trim();
    if expression_source.starts_with("{{") && expression_source.ends_with("}}") {
        expression_source = expression_source[2..expression_source.len() - 2].trim();
    }

    let mut environment = Environment::new();
    environment.set_undefined_behavior(UndefinedBehavior::Strict);
    let empty_attributes = Map::new();
    let attributes = user_attributes.unwrap_or(&empty_attributes);
    let evaluate = || -> Result<bool, Error> {
        let expression = environment.compile_expression(expression_source)?;
        let result = expression.eval(serde_json::json!({"user": attributes}))?;
        // Expression::eval returns a Value; checking its truthiness directly
        // would otherwise turn an undefined final value into a silent false.
        if result.is_undefined() {
            return Err(Error::new(
                ErrorKind::UndefinedError,
                "access expression references an undefined user attribute",
            ));
        }
        Ok(result.is_true())
    };
    evaluate().map_err(|error| PolicyError::AccessExpression {
        expression: source.clone(),
        detail: error.to_string(),
    })
}

// Remove author quotes immediately surrounding an output placeholder. The
// formatter supplies the complete SQL literal, including quotes when needed.
// Separate alternatives replace Python's same-quote backreference, which the
// Rust regex engine intentionally does not support.
static HUGGING_QUOTES: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r#"'\s*(\{\{.*?\}\})\s*'|"\s*(\{\{.*?\}\})\s*""#)
        .expect("valid quoted policy placeholder pattern")
});

pub fn render_row_filter(
    template: &str,
    user_attributes: &Map<String, Value>,
) -> Result<String, PolicyError> {
    render_row_filter_in_dialect(template, user_attributes, DialectType::DuckDB)
}

fn render_row_filter_in_dialect(
    template: &str,
    user_attributes: &Map<String, Value>,
    dialect: DialectType,
) -> Result<String, PolicyError> {
    let normalized = HUGGING_QUOTES.replace_all(template, |captures: &Captures<'_>| {
        captures
            .get(1)
            .or_else(|| captures.get(2))
            .expect("quoted placeholder alternative has a capture")
            .as_str()
            .to_owned()
    });
    let mut environment = Environment::new();
    environment.set_undefined_behavior(UndefinedBehavior::Strict);
    environment.set_formatter(move |output, _state, value| {
        match value.kind() {
            ValueKind::Undefined => {
                return Err(Error::new(
                    ErrorKind::UndefinedError,
                    "row filter references an undefined user attribute",
                ));
            }
            ValueKind::None => output.write_str("NULL")?,
            ValueKind::Bool => output.write_str(if value.is_true() { "TRUE" } else { "FALSE" })?,
            ValueKind::Number => {
                let numeric = f64::try_from(value.clone())?;
                if !numeric.is_finite() {
                    return Err(Error::new(
                        ErrorKind::InvalidOperation,
                        "row filter requires a finite numeric attribute",
                    ));
                }
                write!(output, "{value}")?;
            }
            ValueKind::String => {
                let text = value.as_str().expect("string kind has a string value");
                // Values must be quoted for the parser that consumes the
                // rendered template, including dialect-specific backslashes.
                let literal = Expression::Literal(polyglot_sql::expressions::Literal::String(
                    text.to_owned(),
                ));
                let sql =
                    crate::semantic_input::dialects::emit(literal, DialectType::DuckDB, dialect)
                        .map_err(|error| {
                            Error::new(ErrorKind::InvalidOperation, error.to_string())
                        })?;
                output.write_str(&sql)?;
            }
            kind => {
                return Err(Error::new(
                    ErrorKind::InvalidOperation,
                    format!("unsupported user-attribute type for a row filter: {kind}"),
                ));
            }
        }
        Ok(())
    });
    let render = || -> Result<String, Error> {
        environment
            .template_from_str(&normalized)?
            .render(serde_json::json!({"user": user_attributes}))
    };
    render().map_err(|error| PolicyError::RowFilter {
        template: template.to_owned(),
        detail: error.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn attributes(value: Value) -> Map<String, Value> {
        value
            .as_object()
            .expect("test attributes are a map")
            .clone()
    }

    #[test]
    fn default_policy_requires_supplied_attributes() {
        let policy: SecurityPolicy = serde_json::from_value(json!({})).unwrap();
        assert_eq!(policy.access, AccessRule::Bool(true));
        assert!(matches!(
            policy.render_for_model("orders", None),
            Err(PolicyError::MissingUserAttributes { model }) if model == "orders"
        ));
        assert_eq!(
            policy
                .render_for_model("orders", Some(&Map::new()))
                .unwrap(),
            Vec::<String>::new()
        );
    }

    #[test]
    fn access_accepts_literal_bare_and_wrapped_rules() {
        let user = attributes(json!({"role": "analyst", "enabled": true}));
        for source in [
            "user.role == 'analyst'",
            "{{ user.role == 'analyst' }}",
            "user.enabled",
        ] {
            assert!(evaluate_access(&AccessRule::Expression(source.into()), Some(&user)).unwrap());
        }
        assert!(evaluate_access(&AccessRule::Bool(true), None).unwrap());
        assert!(!evaluate_access(&AccessRule::Bool(false), Some(&user)).unwrap());
        assert!(!evaluate_access(
            &AccessRule::Expression("user.role == 'admin'".into()),
            Some(&user)
        )
        .unwrap());
    }

    #[test]
    fn denied_access_never_renders_row_filters() {
        let policy = SecurityPolicy {
            access: AccessRule::Bool(false),
            row_filters: vec!["id = {{ user.missing }}".into()],
        };
        assert!(matches!(
            policy.render_for_model("orders", Some(&Map::new())),
            Err(PolicyError::AccessDenied { model }) if model == "orders"
        ));
    }

    #[test]
    fn access_undefined_and_malformed_expressions_fail_closed() {
        for source in [
            "user.missing",
            "user.missing == 'admin'",
            "user.role ==",
            "{{",
        ] {
            let error = evaluate_access(&AccessRule::Expression(source.into()), Some(&Map::new()));
            assert!(
                matches!(error, Err(PolicyError::AccessExpression { .. })),
                "{source}"
            );
        }
    }

    #[test]
    fn quoted_and_unquoted_placeholders_escape_strings() {
        let user = attributes(json!({"region": "US' OR 1=1 --"}));
        for template in [
            "region = {{ user.region }}",
            "region = '{{ user.region }}'",
            "region = \"{{ user.region }}\"",
        ] {
            assert_eq!(
                render_row_filter(template, &user).unwrap(),
                "region = 'US'' OR 1=1 --'"
            );
        }
    }

    #[test]
    fn rendered_outputs_are_typed_sql_literals() {
        let user = attributes(json!({"enabled": false, "id": 12, "ratio": 1.5, "missing": null}));
        assert_eq!(
            render_row_filter(
                "enabled = {{ user.enabled }} AND id = {{ user.id }} AND ratio > {{ user.ratio }} AND missing IS {{ user.missing }}",
                &user
            ).unwrap(),
            "enabled = FALSE AND id = 12 AND ratio > 1.5 AND missing IS NULL"
        );
    }

    #[test]
    fn conditionals_keep_raw_attribute_truthiness_and_comparisons() {
        let template = "{% if user.is_admin %}1 = 1{% elif user.role == 'analyst' %}tenant_id = {{ user.tenant_id }}{% else %}1 = 0{% endif %}";
        for (user, expected) in [
            (json!({"is_admin": true}), "1 = 1"),
            (
                json!({"is_admin": false, "role": "analyst", "tenant_id": 7}),
                "tenant_id = 7",
            ),
            (json!({"is_admin": false, "role": "guest"}), "1 = 0"),
        ] {
            assert_eq!(
                render_row_filter(template, &attributes(user)).unwrap(),
                expected
            );
        }
    }

    #[test]
    fn filter_undefined_malformed_and_non_scalar_outputs_fail_closed() {
        for template in [
            "id = {{ user.missing }}",
            "{% if user.missing %}1=1{% endif %}",
            "id = {{",
        ] {
            assert!(matches!(
                render_row_filter(template, &Map::new()),
                Err(PolicyError::RowFilter { .. })
            ));
        }
        for value in [json!([1, 2]), json!({"id": 1})] {
            let error = render_row_filter("id = {{ user.id }}", &attributes(json!({"id": value})));
            assert!(
                matches!(error, Err(PolicyError::RowFilter { detail, .. }) if detail.contains("unsupported user-attribute type"))
            );
        }
    }

    #[test]
    fn policy_renders_every_predicate_without_rewriting_sql_scopes() {
        let policy: SecurityPolicy = serde_json::from_value(json!({
            "access": "user.role == 'analyst'",
            "row_filters": ["tenant_id = {{ user.tenant_id }}", "id IN (SELECT id FROM allowed)"]
        }))
        .unwrap();
        let user = attributes(json!({"role": "analyst", "tenant_id": 2}));
        assert_eq!(
            policy.render_for_model("orders", Some(&user)).unwrap(),
            vec!["tenant_id = 2", "id IN (SELECT id FROM allowed)"]
        );
    }

    #[test]
    fn policy_rejects_unknown_or_invalid_definition_fields() {
        for value in [
            json!({"access": null}),
            json!({"access": []}),
            json!({"row_filters": "1=1"}),
            json!({"unknown": true}),
        ] {
            assert!(serde_json::from_value::<SecurityPolicy>(value).is_err());
        }
    }
}
