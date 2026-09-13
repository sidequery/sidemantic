//! Adapter-provided join kinds retain their declared direction through role paths.

use crate::core::{JoinStep, SemanticGraph};
use crate::error::{Result, SidemanticError};

pub(super) fn explicit_join_type(
    graph: &SemanticGraph,
    step: &JoinStep,
) -> Result<Option<&'static str>> {
    for (source, target, reverse) in [
        (&step.from_model, &step.to_model, false),
        (&step.to_model, &step.from_model, true),
    ] {
        let Some(model) = graph.get_model(source) else {
            continue;
        };
        let mut selected = None;
        for relationship in &model.relationships {
            if graph.relationship_target_instance(source, relationship) != Some(target.as_str())
                || relationship.edge_id != step.edge_id
            {
                continue;
            }
            let how = relationship
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.get("bsl_how"))
                .and_then(|value| value.as_str())
                .unwrap_or("")
                .to_ascii_lowercase();
            let kind = match (how.as_str(), reverse) {
                ("inner", _) => Some("INNER JOIN"),
                ("left", false) | ("right", true) => Some("LEFT JOIN"),
                ("right", false) | ("left", true) => Some("RIGHT JOIN"),
                ("outer" | "full" | "full_outer", _) => Some("FULL JOIN"),
                _ => None,
            };
            // Parallel declarations with identical keys may have deduplicated in
            // the graph. Conflicting preservation semantics still make them ambiguous.
            if selected.is_some_and(|previous| previous != kind) {
                return Err(SidemanticError::AmbiguousJoinPath {
                    from: step.from_model.clone(),
                    to: step.to_model.clone(),
                });
            }
            selected = Some(kind);
        }
        if let Some(kind) = selected {
            return Ok(kind);
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{Dimension, Model, Relationship};
    use crate::sql::{SemanticQuery, SqlGenerator};
    use serde_json::json;

    fn graph_with_role(how: &str) -> SemanticGraph {
        let mut graph = SemanticGraph::new();
        let mut relationship = Relationship::many_to_one("buyer").with_keys("buyer_id", "id");
        relationship.target_model = Some("customers".into());
        relationship.edge_id = Some("orders_buyer".into());
        relationship.metadata = Some(json!({"bsl_how": how}));
        graph
            .add_model(
                Model::new("orders", "id")
                    .with_table("orders")
                    .with_dimension(Dimension::categorical("id"))
                    .with_relationship(relationship),
            )
            .unwrap();
        graph
            .add_model(
                Model::new("customers", "id")
                    .with_table("customers")
                    .with_dimension(Dimension::categorical("label")),
            )
            .unwrap();
        graph
    }

    #[test]
    fn explicit_kinds_preserve_forward_and_reverse_role_directions() {
        for (how, forward, reverse) in [
            ("inner", "INNER JOIN", "INNER JOIN"),
            ("LEFT", "LEFT JOIN", "RIGHT JOIN"),
            ("right", "RIGHT JOIN", "LEFT JOIN"),
            ("outer", "FULL JOIN", "FULL JOIN"),
            ("full", "FULL JOIN", "FULL JOIN"),
            ("full_outer", "FULL JOIN", "FULL JOIN"),
        ] {
            let graph = graph_with_role(how);
            let path = graph.find_join_path("orders", "buyer").unwrap();
            assert_eq!(
                explicit_join_type(&graph, &path.steps[0]).unwrap(),
                Some(forward)
            );
            let path = graph.find_join_path("buyer", "orders").unwrap();
            assert_eq!(
                explicit_join_type(&graph, &path.steps[0]).unwrap(),
                Some(reverse)
            );
        }
    }

    #[test]
    fn explicit_left_join_overrides_filter_default_without_removing_policy() {
        let graph = graph_with_role("left");
        let mut query = SemanticQuery::default()
            .with_dimensions(vec!["orders.id".into(), "buyer.label".into()]);
        query
            .prepared_policies
            .row_filters
            .insert("buyer".into(), vec!["visible = TRUE".into()]);
        let sql = SqlGenerator::new(&graph).generate(&query).unwrap();
        assert!(sql.contains("WHERE visible = TRUE"), "{sql}");
        assert!(sql.contains("LEFT JOIN buyer_cte"), "{sql}");
        assert!(!sql.contains("INNER JOIN buyer_cte"), "{sql}");
    }

    #[test]
    fn nested_roles_use_the_declaring_relationship_and_skip_inactive_edges() {
        let mut graph = graph_with_role("inner");
        let mut customers = graph.get_model("customers").unwrap().clone();
        let mut country = Relationship::many_to_one("country").with_keys("country_id", "id");
        country.target_model = Some("countries".into());
        country.metadata = Some(json!({"bsl_how": "left"}));
        let mut inactive = country.clone();
        inactive.active = false;
        inactive.metadata = Some(json!({"bsl_how": "right"}));
        customers.relationships = vec![inactive, country];
        graph.replace_model(customers).unwrap();
        graph
            .add_model(Model::new("countries", "id").with_table("countries"))
            .unwrap();
        let path = graph.find_join_path("buyer", "buyer$country").unwrap();
        assert_eq!(
            explicit_join_type(&graph, &path.steps[0]).unwrap(),
            Some("LEFT JOIN")
        );
        let path = graph.find_join_path("buyer$country", "buyer").unwrap();
        assert_eq!(
            explicit_join_type(&graph, &path.steps[0]).unwrap(),
            Some("RIGHT JOIN")
        );
    }

    #[test]
    fn conflicting_explicit_kinds_are_ambiguous_even_with_identical_join_keys() {
        let mut graph = graph_with_role("left");
        let mut model = graph.get_model("orders").unwrap().clone();
        // Use canonical edges to permit otherwise equivalent parallel declarations.
        model.relationships[0].name = "customers".into();
        model.relationships[0].target_model = None;
        let mut duplicate = model.relationships[0].clone();
        duplicate.metadata = Some(json!({"bsl_how": "inner"}));
        model.relationships.push(duplicate);
        graph.replace_model(model).unwrap();
        let path = graph.find_join_path("orders", "customers").unwrap();
        assert!(matches!(
            explicit_join_type(&graph, &path.steps[0]),
            Err(SidemanticError::AmbiguousJoinPath { .. })
        ));
    }
}
