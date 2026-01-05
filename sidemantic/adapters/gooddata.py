"""GoodData LDM adapter for importing/exporting logical data models."""

import json
from pathlib import Path
from typing import Any

from sidemantic.adapters.base import BaseAdapter
from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.relationship import Relationship
from sidemantic.core.semantic_graph import SemanticGraph

GOODDATA_METADATA_KEY = "gooddata"


class GoodDataParseError(ValueError):
    """Raised when GoodData LDM payloads are invalid or unsupported."""


class GoodDataAdapter(BaseAdapter):
    """Adapter for GoodData LDM JSON definitions (cloud + legacy).

    Transforms GoodData LDM definitions into Sidemantic format:
    - Datasets -> Models
    - Attributes -> Dimensions
    - Facts -> Metrics (default aggregation: sum for numeric facts)
    - References -> Relationships
    - Date instances/dimensions -> Date models with time dimensions
    """

    def parse(self, source: str | Path) -> SemanticGraph:
        """Parse GoodData LDM JSON into semantic graph.

        Args:
            source: Path to a .json file or directory of JSON files

        Returns:
            Semantic graph with imported models
        """
        graph = SemanticGraph()
        source_path = Path(source)

        json_files = list(source_path.rglob("*.json")) if source_path.is_dir() else [source_path]

        for json_file in json_files:
            with open(json_file) as f:
                data = json.load(f)

            ldm_kind, ldm_payload = self._detect_payload(data)
            if not ldm_kind or not ldm_payload:
                raise GoodDataParseError(f"{json_file} does not look like GoodData LDM JSON")

            if ldm_kind == "cloud":
                models, date_models = self._parse_cloud_ldm(ldm_payload)
            else:
                models, date_models = self._parse_legacy_ldm(ldm_payload)

            for model in models + date_models:
                if model.name not in graph.models:
                    graph.add_model(model)

        self._apply_reference_primary_keys(graph)
        return graph

    def export(self, graph: SemanticGraph, output_path: str | Path) -> None:
        """Export semantic graph to GoodData LDM JSON format (cloud style)."""
        output_path = Path(output_path)

        from sidemantic.core.inheritance import resolve_model_inheritance

        resolved_models = resolve_model_inheritance(graph.models)

        datasets = []
        date_instances = []
        for model in resolved_models.values():
            gd_meta = (model.metadata or {}).get(GOODDATA_METADATA_KEY, {})
            if gd_meta.get("kind") in ("date_instance", "date_dimension"):
                date_instances.append(self._export_date_instance(model, gd_meta))
            else:
                datasets.append(self._export_dataset(model))

        payload = {"ldm": {"datasets": datasets}}
        if date_instances:
            payload["ldm"]["dateInstances"] = date_instances

        if output_path.is_dir() or not output_path.suffix:
            output_path.mkdir(parents=True, exist_ok=True)
            file_path = output_path / "ldm.json"
        else:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            file_path = output_path

        with open(file_path, "w") as f:
            json.dump(payload, f, indent=2)

    def _detect_payload(self, data: Any) -> tuple[str | None, dict[str, Any] | None]:
        if not isinstance(data, dict):
            return None, None

        if "ldm" in data and isinstance(data["ldm"], dict):
            return "cloud", data["ldm"]
        if "data" in data and isinstance(data["data"], dict) and isinstance(data["data"].get("ldm"), dict):
            return "cloud", data["data"]["ldm"]
        if "projectModel" in data and isinstance(data["projectModel"], dict):
            return "legacy", data["projectModel"]
        if "model" in data and isinstance(data["model"], dict) and isinstance(data["model"].get("projectModel"), dict):
            return "legacy", data["model"]["projectModel"]

        if "dateDimensions" in data:
            return "legacy", data

        if "datasets" in data or "dateInstances" in data or "date_instances" in data:
            datasets = data.get("datasets")
            if isinstance(datasets, list) and datasets and isinstance(datasets[0], dict):
                if "dataset" in datasets[0] or "anchor" in datasets[0]:
                    return "legacy", data
            return "cloud", data

        return None, None

    def _parse_cloud_ldm(self, ldm: dict[str, Any]) -> tuple[list[Model], list[Model]]:
        datasets = self._as_list(ldm.get("datasets") or ldm.get("dataSets") or ldm.get("data_sets"))
        date_instances = self._as_list(ldm.get("dateInstances") or ldm.get("date_instances"))

        models = []
        for dataset_def in datasets:
            model = self._parse_cloud_dataset(dataset_def)
            if model:
                models.append(model)

        date_models = []
        for date_def in date_instances:
            date_model = self._parse_date_instance(date_def, kind="date_instance")
            if date_model:
                date_models.append(date_model)

        return models, date_models

    def _parse_cloud_dataset(self, dataset_def: dict[str, Any]) -> Model | None:
        if "dataset" in dataset_def and isinstance(dataset_def["dataset"], dict):
            dataset_def = dataset_def["dataset"]

        dataset_id = self._extract_identifier(dataset_def, keys=("id", "identifier", "name"))
        if not dataset_id:
            raise GoodDataParseError("Dataset is missing an id/identifier")

        table = self._coerce_table_path(
            self._get_first(
                dataset_def,
                "dataSourceTableId",
                "data_source_table_id",
                "tablePath",
                "table_path",
                "table",
            )
        )
        sql = dataset_def.get("sql")
        if sql:
            table = None

        grain_ids = self._parse_grain_ids(dataset_def)
        primary_key = grain_ids[0] if grain_ids else "id"

        attributes = self._as_list(dataset_def.get("attributes"))
        facts = self._as_list(dataset_def.get("facts"))

        fields = dataset_def.get("fields")
        if fields:
            for field in self._normalize_fields(fields):
                field_type = (field.get("type") or "").lower()
                if field_type == "attribute":
                    attributes.append(field)
                elif field_type == "fact":
                    facts.append(field)

        dimensions = []
        for attr_def in attributes:
            dim = self._parse_cloud_attribute(attr_def)
            if dim:
                dimensions.append(dim)

        metrics = []
        for fact_def in facts:
            metric = self._parse_cloud_fact(fact_def)
            if metric:
                metrics.append(metric)

        if primary_key and not any(dim.name == primary_key for dim in dimensions):
            dimensions.append(
                Dimension(
                    name=primary_key,
                    type="categorical",
                    sql=primary_key,
                    metadata={GOODDATA_METADATA_KEY: {"generated": True}},
                )
            )

        relationships = []
        for ref_def in self._as_list(dataset_def.get("references")):
            rel = self._parse_cloud_reference(ref_def)
            if rel:
                relationships.append(rel)

        metadata = {
            GOODDATA_METADATA_KEY: {
                "id": dataset_id,
                "title": dataset_def.get("title"),
                "description": dataset_def.get("description"),
                "tags": dataset_def.get("tags"),
                "data_source_id": dataset_def.get("dataSourceId") or dataset_def.get("data_source_id"),
                "data_source_table_id": dataset_def.get("dataSourceTableId") or dataset_def.get("data_source_table_id"),
                "table_path": dataset_def.get("tablePath") or dataset_def.get("table_path"),
                "grain": grain_ids,
                "extra": self._extract_extra(dataset_def, self._cloud_dataset_keys()),
            }
        }

        return Model(
            name=dataset_id,
            table=table,
            sql=sql,
            description=dataset_def.get("description") or dataset_def.get("title"),
            primary_key=primary_key,
            dimensions=dimensions,
            metrics=metrics,
            relationships=relationships,
            metadata=metadata,
        )

    def _parse_cloud_attribute(self, attr_def: dict[str, Any]) -> Dimension | None:
        if "attribute" in attr_def and isinstance(attr_def["attribute"], dict):
            attr_def = attr_def["attribute"]

        attr_id = self._extract_identifier(attr_def, keys=("id", "identifier", "name"))
        if not attr_id:
            raise GoodDataParseError("Attribute is missing an id/identifier")

        labels = self._normalize_labels(attr_def.get("labels"))
        default_view = attr_def.get("defaultView") or attr_def.get("default_view")
        default_view_id = self._extract_identifier(default_view, keys=("id", "identifier", "name"))
        label_def = self._select_label(labels, default_view_id)

        source_column = self._get_first(label_def or {}, "sourceColumn", "source_column") or self._get_first(
            attr_def, "sourceColumn", "source_column"
        )
        data_type = self._get_first(label_def or {}, "dataType", "data_type") or self._get_first(
            attr_def, "dataType", "data_type"
        )

        dim_type, granularity = self._map_dimension_type(data_type)

        metadata = {
            GOODDATA_METADATA_KEY: {
                "id": attr_id,
                "title": attr_def.get("title"),
                "description": attr_def.get("description"),
                "tags": attr_def.get("tags"),
                "source_column": source_column,
                "data_type": data_type,
                "labels": labels,
                "default_view": default_view_id,
                "extra": self._extract_extra(attr_def, self._cloud_attribute_keys()),
            }
        }

        return Dimension(
            name=attr_id,
            type=dim_type,
            sql=source_column or attr_id,
            label=attr_def.get("title") or (label_def or {}).get("title"),
            description=attr_def.get("description"),
            granularity=granularity,
            metadata=metadata,
        )

    def _parse_cloud_fact(self, fact_def: dict[str, Any]) -> Metric | None:
        if "fact" in fact_def and isinstance(fact_def["fact"], dict):
            fact_def = fact_def["fact"]

        fact_id = self._extract_identifier(fact_def, keys=("id", "identifier", "name"))
        if not fact_id:
            raise GoodDataParseError("Fact is missing an id/identifier")

        source_column = self._get_first(fact_def, "sourceColumn", "source_column")
        data_type = self._get_first(fact_def, "dataType", "data_type")
        agg = self._map_fact_aggregation(fact_def, data_type)

        metadata = {
            GOODDATA_METADATA_KEY: {
                "id": fact_id,
                "title": fact_def.get("title"),
                "description": fact_def.get("description"),
                "tags": fact_def.get("tags"),
                "source_column": source_column,
                "data_type": data_type,
                "aggregation": fact_def.get("aggregation"),
                "extra": self._extract_extra(fact_def, self._cloud_fact_keys()),
            }
        }

        return Metric(
            name=fact_id,
            agg=agg,
            sql=source_column or fact_id,
            label=fact_def.get("title"),
            description=fact_def.get("description"),
            metadata=metadata,
        )

    def _parse_cloud_reference(self, ref_def: Any) -> Relationship | None:
        if isinstance(ref_def, str):
            target_id = ref_def
            ref_def = {}
        else:
            identifier = self._get_first(ref_def, "identifier", "dataset", "reference")
            if isinstance(identifier, dict):
                target_id = self._extract_identifier(identifier, keys=("id", "identifier", "name"))
            else:
                target_id = identifier

        if not target_id:
            return None

        multivalue = self._get_first(ref_def, "multivalue", "multiValue") is True
        rel_type = "many_to_many" if multivalue else "many_to_one"

        source_columns = self._get_first(ref_def, "sourceColumns", "source_columns", "sourceColumn", "source_column")
        foreign_key = None
        if isinstance(source_columns, list) and len(source_columns) == 1:
            foreign_key = source_columns[0]
        elif isinstance(source_columns, str):
            foreign_key = source_columns

        metadata = {
            GOODDATA_METADATA_KEY: {
                "identifier": target_id,
                "source_columns": source_columns,
                "multivalue": multivalue,
                "extra": self._extract_extra(ref_def, self._cloud_reference_keys()),
            }
        }

        return Relationship(name=target_id, type=rel_type, foreign_key=foreign_key, metadata=metadata)

    def _parse_date_instance(self, date_def: dict[str, Any], kind: str) -> Model | None:
        if "dateInstance" in date_def and isinstance(date_def["dateInstance"], dict):
            date_def = date_def["dateInstance"]
        if "dateDimension" in date_def and isinstance(date_def["dateDimension"], dict):
            date_def = date_def["dateDimension"]

        date_id = self._extract_identifier(date_def, keys=("id", "identifier", "name"))
        if not date_id:
            raise GoodDataParseError("Date instance/dimension is missing an id/identifier")

        granularity_values = self._as_list(date_def.get("granularities") or date_def.get("granularity"))
        granularities = [str(g).lower() for g in granularity_values if g]
        primary_granularity = "day" if "day" in granularities else (granularities[0] if granularities else "day")

        time_dim_name = "date" if "day" in granularities else primary_granularity

        dimension = Dimension(
            name=time_dim_name,
            type="time",
            sql=time_dim_name,
            granularity=primary_granularity,
            supported_granularities=granularities or None,
            label=date_def.get("title"),
            description=date_def.get("description"),
            metadata={
                GOODDATA_METADATA_KEY: {
                    "id": date_id,
                    "granularities": granularities,
                }
            },
        )

        metadata = {
            GOODDATA_METADATA_KEY: {
                "id": date_id,
                "title": date_def.get("title"),
                "description": date_def.get("description"),
                "tags": date_def.get("tags"),
                "data_source_id": date_def.get("dataSourceId") or date_def.get("data_source_id"),
                "data_source_table_id": date_def.get("dataSourceTableId") or date_def.get("data_source_table_id"),
                "table_path": date_def.get("tablePath") or date_def.get("table_path"),
                "granularities": granularities,
                "granularitiesFormatting": date_def.get("granularitiesFormatting")
                or date_def.get("granularities_formatting"),
                "kind": kind,
                "extra": self._extract_extra(date_def, self._cloud_date_instance_keys()),
            }
        }

        table = self._coerce_table_path(
            self._get_first(
                date_def,
                "dataSourceTableId",
                "data_source_table_id",
                "tablePath",
                "table_path",
                "table",
            )
        )

        return Model(
            name=date_id,
            table=table or date_id,
            description=date_def.get("description") or date_def.get("title"),
            primary_key=dimension.name,
            dimensions=[dimension],
            metadata=metadata,
        )

    def _parse_legacy_ldm(self, project_model: dict[str, Any]) -> tuple[list[Model], list[Model]]:
        datasets = self._as_list(project_model.get("datasets") or project_model.get("dataSets"))
        date_dimensions = self._as_list(project_model.get("dateDimensions") or project_model.get("date_dimensions"))

        date_models = []
        for date_def in date_dimensions:
            date_model = self._parse_date_instance(date_def, kind="date_dimension")
            if date_model:
                date_models.append(date_model)

        models = []
        for dataset_def in datasets:
            model = self._parse_legacy_dataset(dataset_def)
            if model:
                models.append(model)

        return models, date_models

    def _parse_legacy_dataset(self, dataset_def: dict[str, Any]) -> Model | None:
        if "dataset" in dataset_def and isinstance(dataset_def["dataset"], dict):
            dataset_def = dataset_def["dataset"]

        dataset_id = self._extract_identifier(dataset_def, keys=("identifier", "id", "name"))
        if not dataset_id:
            raise GoodDataParseError("Legacy dataset is missing an identifier")

        label_map = self._build_legacy_label_map(dataset_def.get("labels"))

        dimensions = []
        primary_key = "id"

        anchor_def = dataset_def.get("anchor")
        if isinstance(anchor_def, dict):
            anchor_attr = anchor_def.get("attribute") if isinstance(anchor_def.get("attribute"), dict) else anchor_def
            anchor_dim = self._parse_legacy_attribute(anchor_attr, label_map)
            if anchor_dim:
                dimensions.append(anchor_dim)
                primary_key = anchor_dim.name

        for attr_def in self._as_list(dataset_def.get("attributes")):
            attr = attr_def.get("attribute") if isinstance(attr_def, dict) and "attribute" in attr_def else attr_def
            dim = self._parse_legacy_attribute(attr, label_map)
            if dim and dim.name not in {d.name for d in dimensions}:
                dimensions.append(dim)

        metrics = []
        for fact_def in self._as_list(dataset_def.get("facts")):
            fact = fact_def.get("fact") if isinstance(fact_def, dict) and "fact" in fact_def else fact_def
            metric = self._parse_legacy_fact(fact)
            if metric:
                metrics.append(metric)

        references = []
        for ref_def in self._as_list(dataset_def.get("references")):
            rel = self._parse_legacy_reference(ref_def)
            if rel:
                references.append(rel)

        metadata = {
            GOODDATA_METADATA_KEY: {
                "identifier": dataset_id,
                "title": dataset_def.get("title"),
                "description": dataset_def.get("description"),
                "tags": dataset_def.get("tags"),
                "extra": self._extract_extra(dataset_def, self._legacy_dataset_keys()),
            }
        }

        table = dataset_def.get("table") or dataset_id
        return Model(
            name=dataset_id,
            table=table,
            sql=dataset_def.get("sql"),
            description=dataset_def.get("description") or dataset_def.get("title"),
            primary_key=primary_key,
            dimensions=dimensions,
            metrics=metrics,
            relationships=references,
            metadata=metadata,
        )

    def _parse_legacy_attribute(self, attr_def: dict[str, Any] | None, label_map: dict[str, dict]) -> Dimension | None:
        if not isinstance(attr_def, dict):
            return None

        attr_id = self._extract_identifier(attr_def, keys=("identifier", "id", "name"))
        if not attr_id:
            raise GoodDataParseError("Legacy attribute is missing an identifier")

        label_ids = self._as_list(attr_def.get("labels"))
        label_def = label_map.get(label_ids[0]) if label_ids else None

        source_column = self._get_first(label_def or {}, "sourceColumn", "source_column") or self._get_first(
            attr_def, "sourceColumn", "source_column"
        )
        data_type = self._get_first(label_def or {}, "dataType", "data_type") or self._get_first(
            attr_def, "dataType", "data_type"
        )

        dim_type, granularity = self._map_dimension_type(data_type)

        metadata = {
            GOODDATA_METADATA_KEY: {
                "identifier": attr_id,
                "title": attr_def.get("title"),
                "description": attr_def.get("description"),
                "labels": label_ids,
                "label_details": label_def,
                "source_column": source_column,
                "data_type": data_type,
                "extra": self._extract_extra(attr_def, self._legacy_attribute_keys()),
            }
        }

        return Dimension(
            name=attr_id,
            type=dim_type,
            sql=source_column or attr_id,
            label=attr_def.get("title") or (label_def or {}).get("title"),
            description=attr_def.get("description"),
            granularity=granularity,
            metadata=metadata,
        )

    def _parse_legacy_fact(self, fact_def: dict[str, Any] | None) -> Metric | None:
        if not isinstance(fact_def, dict):
            return None

        fact_id = self._extract_identifier(fact_def, keys=("identifier", "id", "name"))
        if not fact_id:
            raise GoodDataParseError("Legacy fact is missing an identifier")

        source_column = self._get_first(fact_def, "sourceColumn", "source_column")
        data_type = self._get_first(fact_def, "dataType", "data_type")
        agg = self._map_fact_aggregation(fact_def, data_type)

        metadata = {
            GOODDATA_METADATA_KEY: {
                "identifier": fact_id,
                "title": fact_def.get("title"),
                "description": fact_def.get("description"),
                "source_column": source_column,
                "data_type": data_type,
                "extra": self._extract_extra(fact_def, self._legacy_fact_keys()),
            }
        }

        return Metric(
            name=fact_id,
            agg=agg,
            sql=source_column or fact_id,
            label=fact_def.get("title"),
            description=fact_def.get("description"),
            metadata=metadata,
        )

    def _parse_legacy_reference(self, ref_def: Any) -> Relationship | None:
        if isinstance(ref_def, str):
            target_id = ref_def
            ref_def = {}
        elif isinstance(ref_def, dict):
            target_id = self._extract_identifier(ref_def, keys=("identifier", "id", "name"))
            if not target_id:
                target_id = ref_def.get("dataset") or ref_def.get("reference")
        else:
            return None

        if not target_id:
            return None

        metadata = {
            GOODDATA_METADATA_KEY: {
                "identifier": target_id,
                "extra": ref_def if isinstance(ref_def, dict) else {},
            }
        }

        return Relationship(name=target_id, type="many_to_one", metadata=metadata)

    def _build_legacy_label_map(self, labels_def: Any) -> dict[str, dict]:
        label_map: dict[str, dict] = {}
        for label_def in self._as_list(labels_def):
            label = label_def.get("label") if isinstance(label_def, dict) and "label" in label_def else label_def
            if not isinstance(label, dict):
                continue
            label_id = self._extract_identifier(label, keys=("identifier", "id", "name"))
            if label_id:
                label_map[label_id] = label
        return label_map

    def _export_dataset(self, model: Model) -> dict[str, Any]:
        gd_meta = (model.metadata or {}).get(GOODDATA_METADATA_KEY, {})
        dataset_id = gd_meta.get("id") or gd_meta.get("identifier") or model.name

        dataset: dict[str, Any] = {
            "id": dataset_id,
            "title": gd_meta.get("title") or model.name,
        }

        if model.description:
            dataset["description"] = model.description

        if gd_meta.get("tags"):
            dataset["tags"] = gd_meta["tags"]

        if model.table:
            dataset["dataSourceTableId"] = model.table
        elif gd_meta.get("data_source_table_id"):
            dataset["dataSourceTableId"] = gd_meta["data_source_table_id"]
        elif gd_meta.get("table_path"):
            dataset["tablePath"] = gd_meta["table_path"]

        if gd_meta.get("data_source_id"):
            dataset["dataSourceId"] = gd_meta["data_source_id"]

        if model.sql:
            dataset["sql"] = model.sql

        grain_ids = gd_meta.get("grain") or [model.primary_key]
        dataset["grain"] = [{"id": grain_id, "type": "attribute"} for grain_id in grain_ids if grain_id]

        attributes = []
        for dim in model.dimensions:
            attr_meta = (dim.metadata or {}).get(GOODDATA_METADATA_KEY, {})
            labels = attr_meta.get("labels")
            if not labels:
                label_id = f"{dim.name}_label"
                labels = [
                    {
                        "id": label_id,
                        "title": dim.label or dim.name,
                        "sourceColumn": dim.sql or dim.name,
                        "dataType": self._map_dimension_to_data_type(dim),
                    }
                ]
                default_view_id = label_id
            else:
                default_view_id = attr_meta.get("default_view") or self._extract_identifier(
                    labels[0], keys=("id", "identifier", "name")
                )

            attr_def: dict[str, Any] = {
                "id": attr_meta.get("id") or dim.name,
                "title": dim.label or dim.name,
                "sourceColumn": dim.sql or dim.name,
                "labels": labels,
                "defaultView": {"id": default_view_id, "type": "label"},
            }

            if dim.description:
                attr_def["description"] = dim.description

            if attr_meta.get("tags"):
                attr_def["tags"] = attr_meta["tags"]

            attributes.append(attr_def)

        if attributes:
            dataset["attributes"] = attributes

        facts = []
        for metric in model.metrics:
            fact_meta = (metric.metadata or {}).get(GOODDATA_METADATA_KEY, {})
            fact_def: dict[str, Any] = {
                "id": fact_meta.get("id") or metric.name,
                "title": metric.label or metric.name,
                "sourceColumn": metric.sql or metric.name,
                "dataType": fact_meta.get("data_type") or "NUMERIC",
            }

            if metric.description:
                fact_def["description"] = metric.description

            if fact_meta.get("tags"):
                fact_def["tags"] = fact_meta["tags"]

            if fact_meta.get("aggregation"):
                fact_def["aggregation"] = fact_meta["aggregation"]

            facts.append(fact_def)

        if facts:
            dataset["facts"] = facts

        references = []
        for rel in model.relationships:
            if rel.type not in ("many_to_one", "many_to_many", "one_to_one"):
                continue

            rel_meta = (rel.metadata or {}).get(GOODDATA_METADATA_KEY, {})
            ref: dict[str, Any] = {
                "identifier": {"id": rel.name, "type": "dataset"},
                "multivalue": rel.type == "many_to_many" or rel_meta.get("multivalue") is True,
            }

            source_columns = rel_meta.get("source_columns")
            if not source_columns and rel.foreign_key:
                source_columns = [rel.foreign_key]
            if source_columns:
                ref["sourceColumns"] = source_columns

            references.append(ref)

        if references:
            dataset["references"] = references

        extra = gd_meta.get("extra")
        if isinstance(extra, dict):
            for key, value in extra.items():
                if key not in dataset:
                    dataset[key] = value

        return dataset

    def _export_date_instance(self, model: Model, gd_meta: dict[str, Any]) -> dict[str, Any]:
        date_id = gd_meta.get("id") or model.name
        date_instance: dict[str, Any] = {"id": date_id, "title": gd_meta.get("title") or model.name}

        if model.description:
            date_instance["description"] = model.description

        if gd_meta.get("tags"):
            date_instance["tags"] = gd_meta["tags"]

        if model.table:
            date_instance["dataSourceTableId"] = model.table
        elif gd_meta.get("data_source_table_id"):
            date_instance["dataSourceTableId"] = gd_meta["data_source_table_id"]
        elif gd_meta.get("table_path"):
            date_instance["tablePath"] = gd_meta["table_path"]

        if gd_meta.get("data_source_id"):
            date_instance["dataSourceId"] = gd_meta["data_source_id"]

        granularities = gd_meta.get("granularities")
        if not granularities and model.dimensions:
            dim = model.dimensions[0]
            if dim.supported_granularities:
                granularities = dim.supported_granularities
            elif dim.granularity:
                granularities = [dim.granularity]

        if granularities:
            date_instance["granularities"] = [str(g).upper() for g in granularities]

        if gd_meta.get("granularitiesFormatting"):
            date_instance["granularitiesFormatting"] = gd_meta["granularitiesFormatting"]

        extra = gd_meta.get("extra")
        if isinstance(extra, dict):
            for key, value in extra.items():
                if key not in date_instance:
                    date_instance[key] = value

        return date_instance

    def _apply_reference_primary_keys(self, graph: SemanticGraph) -> None:
        for model in graph.models.values():
            for relationship in model.relationships:
                if relationship.primary_key:
                    continue
                target = graph.models.get(relationship.name)
                if target and target.primary_key:
                    relationship.primary_key = target.primary_key

    def _parse_grain_ids(self, dataset_def: dict[str, Any]) -> list[str]:
        grain = dataset_def.get("grain") or dataset_def.get("primary_key") or dataset_def.get("primaryKey")
        if not grain:
            return []
        grain_items = self._as_list(grain)

        ids = []
        for item in grain_items:
            if isinstance(item, dict):
                grain_id = self._extract_identifier(item, keys=("id", "identifier", "name"))
            else:
                grain_id = item
            if isinstance(grain_id, str):
                ids.append(grain_id)
        return ids

    def _normalize_fields(self, fields: Any) -> list[dict[str, Any]]:
        if isinstance(fields, dict):
            normalized = []
            for field_id, field_def in fields.items():
                if isinstance(field_def, dict):
                    if "id" not in field_def:
                        field_def = {**field_def, "id": field_id}
                    normalized.append(field_def)
            return normalized
        if isinstance(fields, list):
            return [f for f in fields if isinstance(f, dict)]
        return []

    def _normalize_labels(self, labels: Any) -> list[dict[str, Any]]:
        if isinstance(labels, dict):
            normalized = []
            for label_id, label_def in labels.items():
                if isinstance(label_def, dict):
                    if "id" not in label_def:
                        label_def = {**label_def, "id": label_id}
                    normalized.append(label_def)
            return normalized
        if isinstance(labels, list):
            return [label for label in labels if isinstance(label, dict)]
        return []

    def _select_label(self, labels: list[dict[str, Any]], default_label_id: str | None) -> dict[str, Any] | None:
        if default_label_id:
            for label in labels:
                label_id = self._extract_identifier(label, keys=("id", "identifier", "name"))
                if label_id == default_label_id:
                    return label
        return labels[0] if labels else None

    def _coerce_table_path(self, value: Any) -> str | None:
        if isinstance(value, list):
            parts = [str(part) for part in value if part]
            return ".".join(parts) if parts else None
        if isinstance(value, dict):
            schema = value.get("schema")
            table = value.get("table")
            if schema and table:
                return f"{schema}.{table}"
        if isinstance(value, str):
            return value
        return None

    def _extract_identifier(self, data: Any, keys: tuple[str, ...]) -> str | None:
        if isinstance(data, dict):
            for key in keys:
                if key in data and isinstance(data[key], str):
                    return data[key]
            return None
        if isinstance(data, str):
            return data
        return None

    def _get_first(self, data: dict[str, Any], *keys: str) -> Any:
        for key in keys:
            if key in data and data[key] is not None:
                return data[key]
        return None

    def _as_list(self, value: Any) -> list[Any]:
        if not value:
            return []
        if isinstance(value, list):
            return value
        return [value]

    def _map_dimension_type(self, data_type: str | None) -> tuple[str, str | None]:
        if not data_type:
            return "categorical", None

        dt = str(data_type).upper()
        if dt in {"BOOLEAN", "BOOL"}:
            return "boolean", None
        if dt in {
            "INT",
            "INTEGER",
            "BIGINT",
            "SMALLINT",
            "TINYINT",
            "DECIMAL",
            "NUMERIC",
            "FLOAT",
            "DOUBLE",
            "REAL",
        }:
            return "numeric", None
        if dt == "DATE":
            return "time", "day"
        if dt in {
            "DATETIME",
            "TIMESTAMP",
            "TIMESTAMP_TZ",
            "TIMESTAMP WITH TIME ZONE",
            "TIME",
        }:
            return "time", "hour"
        return "categorical", None

    def _map_dimension_to_data_type(self, dim: Dimension) -> str:
        if dim.type == "boolean":
            return "BOOLEAN"
        if dim.type == "numeric":
            return "NUMERIC"
        if dim.type == "time":
            return "DATE" if dim.granularity == "day" else "TIMESTAMP"
        return "TEXT"

    def _map_fact_aggregation(self, fact_def: dict[str, Any], data_type: str | None) -> str | None:
        agg = self._get_first(fact_def, "aggregation", "agg", "aggregate")
        if isinstance(agg, str):
            normalized = agg.lower()
            mapping = {
                "sum": "sum",
                "avg": "avg",
                "average": "avg",
                "count": "count",
                "count_distinct": "count_distinct",
                "count distinct": "count_distinct",
                "min": "min",
                "max": "max",
                "median": "median",
            }
            return mapping.get(normalized, normalized)

        dim_type, _ = self._map_dimension_type(data_type)
        if dim_type == "numeric":
            return "sum"
        return None

    def _extract_extra(self, data: dict[str, Any], known_keys: set[str]) -> dict[str, Any]:
        return {key: value for key, value in data.items() if key not in known_keys}

    def _cloud_dataset_keys(self) -> set[str]:
        return {
            "id",
            "identifier",
            "name",
            "title",
            "description",
            "tags",
            "dataSourceId",
            "data_source_id",
            "dataSourceTableId",
            "data_source_table_id",
            "tablePath",
            "table_path",
            "table",
            "sql",
            "grain",
            "primary_key",
            "primaryKey",
            "attributes",
            "facts",
            "references",
            "fields",
        }

    def _cloud_attribute_keys(self) -> set[str]:
        return {
            "id",
            "identifier",
            "name",
            "title",
            "description",
            "tags",
            "sourceColumn",
            "source_column",
            "dataType",
            "data_type",
            "labels",
            "defaultView",
            "default_view",
            "type",
        }

    def _cloud_fact_keys(self) -> set[str]:
        return {
            "id",
            "identifier",
            "name",
            "title",
            "description",
            "tags",
            "sourceColumn",
            "source_column",
            "dataType",
            "data_type",
            "aggregation",
            "type",
        }

    def _cloud_reference_keys(self) -> set[str]:
        return {
            "identifier",
            "dataset",
            "reference",
            "sourceColumns",
            "source_columns",
            "sourceColumn",
            "source_column",
            "multivalue",
            "multiValue",
        }

    def _cloud_date_instance_keys(self) -> set[str]:
        return {
            "id",
            "identifier",
            "name",
            "title",
            "description",
            "tags",
            "dataSourceId",
            "data_source_id",
            "dataSourceTableId",
            "data_source_table_id",
            "tablePath",
            "table_path",
            "granularities",
            "granularity",
            "granularitiesFormatting",
            "granularities_formatting",
        }

    def _legacy_dataset_keys(self) -> set[str]:
        return {
            "identifier",
            "id",
            "name",
            "title",
            "description",
            "tags",
            "anchor",
            "attributes",
            "facts",
            "labels",
            "references",
            "table",
            "sql",
        }

    def _legacy_attribute_keys(self) -> set[str]:
        return {
            "identifier",
            "id",
            "name",
            "title",
            "description",
            "labels",
            "sourceColumn",
            "source_column",
            "dataType",
            "data_type",
        }

    def _legacy_fact_keys(self) -> set[str]:
        return {
            "identifier",
            "id",
            "name",
            "title",
            "description",
            "sourceColumn",
            "source_column",
            "dataType",
            "data_type",
        }
