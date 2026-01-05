"""AtScale SML adapter for importing/exporting SML semantic models."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from sidemantic.adapters.base import BaseAdapter
from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.pre_aggregation import PreAggregation
from sidemantic.core.relationship import Relationship
from sidemantic.core.semantic_graph import SemanticGraph

_TIME_UNIT_MAP = {
    "year": "year",
    "halfyear": "quarter",
    "trimester": "quarter",
    "quarter": "quarter",
    "month": "month",
    "week": "week",
    "day": "day",
    "hour": "hour",
    "minute": "minute",
    "second": "second",
}

_CALC_METHOD_AGG_MAP = {
    "sum": "sum",
    "average": "avg",
    "minimum": "min",
    "maximum": "max",
    "count distinct": "count_distinct",
    "count non-null": "count",
    "estimated count distinct": "count_distinct",
}

_CALC_METHOD_SQL_MAP = {
    "stddev_pop": "STDDEV_POP",
    "stddev_samp": "STDDEV_SAMP",
    "var_pop": "VAR_POP",
    "var_samp": "VAR_SAMP",
}


@dataclass(frozen=True)
class DatasetInfo:
    name: str
    table: str | None
    sql: str | None
    description: str | None
    columns: list[dict[str, Any]]
    connection_id: str | None


@dataclass(frozen=True)
class DimensionAttr:
    name: str
    dataset: str
    sql: str
    dim_type: str
    granularity: str | None
    label: str | None
    description: str | None
    parent: str | None


@dataclass(frozen=True)
class MetricInfo:
    name: str
    dataset: str | None
    agg: str | None
    sql: str | None
    metric_type: str | None
    label: str | None
    description: str | None
    format_str: str | None


def _normalize_calc_method(method: str | None) -> str | None:
    if not method:
        return None
    return " ".join(method.strip().lower().split())


def _data_type_to_dimension_type(data_type: str | None) -> str:
    if not data_type:
        return "categorical"
    dtype = data_type.strip().lower()

    if "date" in dtype or "time" in dtype:
        return "time"

    numeric_prefixes = ("decimal", "numeric", "number")
    numeric_types = {
        "int",
        "integer",
        "long",
        "bigint",
        "tinyint",
        "float",
        "double",
    }
    if dtype.startswith(numeric_prefixes) or dtype in numeric_types:
        return "numeric"

    if dtype in {"boolean", "bool"}:
        return "boolean"

    return "categorical"


def _parse_named_quantile(value: str | None) -> float | None:
    if not value:
        return None
    lowered = value.strip().lower()
    if lowered == "median":
        return 0.5
    if lowered.startswith("p") and lowered[1:].isdigit():
        return int(lowered[1:]) / 100
    try:
        numeric = float(lowered)
    except ValueError:
        return None
    if numeric > 1:
        numeric = numeric / 100
    if 0 < numeric < 1:
        return numeric
    return None


def _data_type_to_granularity(data_type: str | None) -> str | None:
    if not data_type:
        return None
    dtype = data_type.strip().lower()
    if "date" in dtype and "time" not in dtype:
        return "day"
    if "time" in dtype:
        return "hour"
    return None


def _dimension_type_to_data_type(dim_type: str) -> str:
    mapping = {
        "categorical": "string",
        "numeric": "decimal(18,4)",
        "time": "datetime",
        "boolean": "boolean",
    }
    return mapping.get(dim_type, "string")


class AtScaleSMLAdapter(BaseAdapter):
    """Adapter for importing/exporting AtScale SML repositories."""

    _supported_object_types = {
        "catalog",
        "connection",
        "dataset",
        "dimension",
        "metric",
        "metric_calc",
        "model",
        "composite_model",
    }

    _object_type_by_dir = {
        "catalog": "catalog",
        "connections": "connection",
        "datasets": "dataset",
        "dimensions": "dimension",
        "metrics": "metric",
        "models": "model",
    }

    def parse(self, source: str | Path) -> SemanticGraph:
        """Parse SML repository files into semantic graph.

        Args:
            source: Path to a SML file or repository directory

        Returns:
            Semantic graph with imported models
        """
        source_path = Path(source)
        if not source_path.exists():
            raise FileNotFoundError(f"SML source not found: {source_path}")

        files = self._collect_yaml_files(source_path)
        objects = self._load_objects(files)

        connections = objects["connection"]
        datasets = self._resolve_datasets(objects["dataset"], connections)
        dimensions = objects["dimension"]
        metrics = objects["metric"]
        calculations = objects["metric_calc"]
        models = objects["model"]

        dimension_attrs = self._collect_dimension_attrs(dimensions, datasets)
        metric_infos = self._collect_metric_infos(metrics, calculations, dimensions)

        graph = SemanticGraph()

        if models:
            dataset_to_model = self._build_models_from_model_defs(
                models=models,
                datasets=datasets,
                metric_infos=metric_infos,
                graph=graph,
                dimensions=dimensions,
            )
            self._apply_dimension_attrs_to_models(dimension_attrs, graph, datasets, dataset_to_model)
            self._apply_metric_infos_to_models(metric_infos, graph, datasets, dataset_to_model)
            self._apply_relationships_from_dimensions(dimensions, datasets, dataset_to_model, graph)
        else:
            for dataset in datasets.values():
                model = self._dataset_to_model(dataset)
                if model and model.name not in graph.models:
                    graph.add_model(model)

            self._apply_dimension_attrs_to_models(dimension_attrs, graph, datasets, {})
            self._apply_metric_infos_to_models(metric_infos, graph, datasets, {})
            self._apply_relationships_from_dimensions(dimensions, datasets, {}, graph)

        return graph

    def export(self, graph: SemanticGraph, output_path: str | Path) -> None:
        """Export semantic graph to SML repository structure.

        Args:
            graph: Semantic graph to export
            output_path: Path to output directory
        """
        output_path = Path(output_path)
        output_path.mkdir(parents=True, exist_ok=True)

        from sidemantic.core.inheritance import resolve_model_inheritance

        resolved_models = resolve_model_inheritance(graph.models)

        datasets_dir = output_path / "datasets"
        dimensions_dir = output_path / "dimensions"
        metrics_dir = output_path / "metrics"
        models_dir = output_path / "models"

        datasets_dir.mkdir(exist_ok=True)
        dimensions_dir.mkdir(exist_ok=True)
        metrics_dir.mkdir(exist_ok=True)
        models_dir.mkdir(exist_ok=True)

        self._write_catalog(output_path)

        for model in resolved_models.values():
            dataset_def = self._export_dataset(model)
            with open(datasets_dir / f"{model.name}.yml", "w") as f:
                yaml.dump(dataset_def, f, sort_keys=False, default_flow_style=False)

            dimension_def = self._export_dimension(model)
            with open(dimensions_dir / f"{model.name}.yml", "w") as f:
                yaml.dump(dimension_def, f, sort_keys=False, default_flow_style=False)

            model_def = self._export_model(model, resolved_models)
            with open(models_dir / f"{model.name}.yml", "w") as f:
                yaml.dump(model_def, f, sort_keys=False, default_flow_style=False)

            for metric in model.metrics:
                metric_def = self._export_metric(metric, model)
                with open(metrics_dir / f"{metric.name}.yml", "w") as f:
                    yaml.dump(metric_def, f, sort_keys=False, default_flow_style=False)

    def _collect_yaml_files(self, source_path: Path) -> list[Path]:
        if source_path.is_dir():
            files = list(source_path.rglob("*.yml")) + list(source_path.rglob("*.yaml"))
        else:
            files = [source_path]
        return files

    def _load_objects(self, files: list[Path]) -> dict[str, dict[str, dict[str, Any]]]:
        objects: dict[str, dict[str, dict[str, Any]]] = {
            "catalog": {},
            "connection": {},
            "dataset": {},
            "dimension": {},
            "metric": {},
            "metric_calc": {},
            "model": {},
            "composite_model": {},
        }

        for file_path in files:
            with open(file_path) as f:
                data = yaml.safe_load(f)

            if not data:
                continue

            if isinstance(data, list):
                for item in data:
                    self._register_object(item, file_path, objects)
            elif isinstance(data, dict):
                self._register_object(data, file_path, objects)

        return objects

    def _register_object(
        self,
        data: dict[str, Any],
        file_path: Path,
        objects: dict[str, dict[str, dict[str, Any]]],
    ) -> None:
        if not isinstance(data, dict):
            return

        obj_type = data.get("object_type")
        if not obj_type and file_path.name in {"catalog.yml", "catalog.yaml", "atscale.yml", "atscale.yaml"}:
            obj_type = "catalog"
        if not obj_type:
            for part in file_path.parts:
                candidate = self._object_type_by_dir.get(part)
                if candidate:
                    obj_type = candidate
                    break

        if obj_type not in self._supported_object_types:
            return

        unique_name = data.get("unique_name")
        if unique_name:
            objects[obj_type][unique_name] = data

    def _resolve_datasets(
        self, datasets: dict[str, dict[str, Any]], connections: dict[str, dict[str, Any]]
    ) -> dict[str, DatasetInfo]:
        resolved: dict[str, DatasetInfo] = {}

        for name, dataset in datasets.items():
            connection_id = dataset.get("connection_id")
            connection = connections.get(connection_id) if connection_id else None

            table = dataset.get("table")
            sql = dataset.get("sql")

            if table and connection:
                database = connection.get("database")
                schema = connection.get("schema")
                if "." not in table:
                    parts = [p for p in [database, schema, table] if p]
                    if parts:
                        table = ".".join(parts)
                elif table.count(".") == 1 and database:
                    table = f"{database}.{table}"

            resolved[name] = DatasetInfo(
                name=name,
                table=table,
                sql=sql,
                description=dataset.get("description") or dataset.get("label"),
                columns=dataset.get("columns") or [],
                connection_id=connection_id,
            )

        return resolved

    def _dataset_to_model(self, dataset: DatasetInfo) -> Model | None:
        if not dataset.name:
            return None

        primary_key = self._infer_primary_key(dataset.columns)

        return Model(
            name=dataset.name,
            table=dataset.table,
            sql=dataset.sql,
            description=dataset.description,
            primary_key=primary_key,
            dimensions=[],
            metrics=[],
        )

    def _infer_primary_key(self, columns: list[dict[str, Any]]) -> str:
        for column in columns:
            name = column.get("name")
            if not name:
                continue
            lowered = name.lower()
            if lowered == "id" or lowered.endswith("_id") or lowered.endswith("key"):
                return name
        return "id"

    def _collect_dimension_attrs(
        self,
        dimensions: dict[str, dict[str, Any]],
        datasets: dict[str, DatasetInfo],
    ) -> dict[str, list[DimensionAttr]]:
        attrs_by_dataset: dict[str, list[DimensionAttr]] = {}

        for dim_def in dimensions.values():
            dim_type = (dim_def.get("type") or "standard").lower()
            is_time_dimension = dim_type == "time"

            hierarchies = dim_def.get("hierarchies") or []
            parent_map = self._build_parent_map(hierarchies)

            for attr in dim_def.get("level_attributes") or []:
                self._collect_attrs_from_attribute(
                    attr,
                    dim_def,
                    datasets,
                    attrs_by_dataset,
                    parent=parent_map.get(attr.get("unique_name")),
                    is_time_dimension=is_time_dimension,
                )

            for hierarchy in hierarchies:
                for level in hierarchy.get("levels") or []:
                    if not isinstance(level, dict):
                        continue
                    level_name = level.get("unique_name")
                    for secondary in level.get("secondary_attributes") or []:
                        self._collect_attrs_from_attribute(
                            secondary,
                            dim_def,
                            datasets,
                            attrs_by_dataset,
                            parent=level_name,
                            is_time_dimension=is_time_dimension,
                        )
                    for alias in level.get("aliases") or []:
                        self._collect_attrs_from_attribute(
                            alias,
                            dim_def,
                            datasets,
                            attrs_by_dataset,
                            parent=level_name,
                            is_time_dimension=is_time_dimension,
                        )

        return attrs_by_dataset

    def _collect_attrs_from_attribute(
        self,
        attr: dict[str, Any],
        dim_def: dict[str, Any],
        datasets: dict[str, DatasetInfo],
        attrs_by_dataset: dict[str, list[DimensionAttr]],
        parent: str | None,
        is_time_dimension: bool,
    ) -> None:
        name = attr.get("unique_name")
        if not name:
            return

        shared_columns = attr.get("shared_degenerate_columns") or []
        if shared_columns:
            for shared in shared_columns:
                dataset_name = shared.get("dataset")
                if not dataset_name:
                    continue
                sql = shared.get("name_column")
                if not sql:
                    key_columns = shared.get("key_columns") or []
                    if key_columns:
                        sql = key_columns[0]
                if not sql:
                    sql = name
                self._append_dimension_attr(
                    dataset_name,
                    name,
                    sql,
                    dim_def,
                    attr,
                    datasets,
                    attrs_by_dataset,
                    parent,
                    is_time_dimension,
                )
            return

        dataset_name = attr.get("dataset")
        if not dataset_name:
            return

        sql = attr.get("name_column")
        if not sql:
            key_columns = attr.get("key_columns") or []
            if key_columns:
                sql = key_columns[0]
        if not sql:
            sql = name

        self._append_dimension_attr(
            dataset_name,
            name,
            sql,
            dim_def,
            attr,
            datasets,
            attrs_by_dataset,
            parent,
            is_time_dimension,
        )

    def _append_dimension_attr(
        self,
        dataset_name: str,
        name: str,
        sql: str,
        dim_def: dict[str, Any],
        attr: dict[str, Any],
        datasets: dict[str, DatasetInfo],
        attrs_by_dataset: dict[str, list[DimensionAttr]],
        parent: str | None,
        is_time_dimension: bool,
    ) -> None:
        dataset = datasets.get(dataset_name)
        data_type = self._lookup_column_type(dataset, sql)

        dim_type = "time" if is_time_dimension or attr.get("time_unit") else _data_type_to_dimension_type(data_type)
        granularity = None
        if dim_type == "time":
            granularity = self._map_time_unit(attr.get("time_unit"), data_type)

        attrs_by_dataset.setdefault(dataset_name, []).append(
            DimensionAttr(
                name=name,
                dataset=dataset_name,
                sql=sql,
                dim_type=dim_type,
                granularity=granularity,
                label=attr.get("label") or dim_def.get("label"),
                description=attr.get("description"),
                parent=parent,
            )
        )

    def _lookup_column_type(self, dataset: DatasetInfo | None, column_name: str) -> str | None:
        if not dataset:
            return None
        for column in dataset.columns:
            if column.get("name") == column_name:
                return column.get("data_type")
        return None

    def _map_time_unit(self, time_unit: str | None, data_type: str | None) -> str | None:
        if time_unit:
            return _TIME_UNIT_MAP.get(time_unit.lower())
        return _data_type_to_granularity(data_type)

    def _build_parent_map(self, hierarchies: list[dict[str, Any]]) -> dict[str, str]:
        parent_map: dict[str, str] = {}
        for hierarchy in hierarchies:
            levels = hierarchy.get("levels") or []
            level_names: list[str] = []
            for level in levels:
                if isinstance(level, dict):
                    level_name = level.get("unique_name")
                else:
                    level_name = level
                if level_name:
                    level_names.append(level_name)
            for idx in range(1, len(level_names)):
                child = level_names[idx]
                parent_map.setdefault(child, level_names[idx - 1])
        return parent_map

    def _collect_metric_infos(
        self,
        metrics: dict[str, dict[str, Any]],
        calculations: dict[str, dict[str, Any]],
        dimensions: dict[str, dict[str, Any]],
    ) -> dict[str, list[MetricInfo]]:
        metrics_by_dataset: dict[str, list[MetricInfo]] = {}

        for metric_def in metrics.values():
            metric_info = self._metric_from_definition(metric_def)
            if metric_info and metric_info.dataset:
                metrics_by_dataset.setdefault(metric_info.dataset, []).append(metric_info)

        for calc_def in calculations.values():
            metric_info = self._metric_from_calc(calc_def)
            if metric_info:
                metrics_by_dataset.setdefault("__global__", []).append(metric_info)

        for dim_def in dimensions.values():
            for hierarchy in dim_def.get("hierarchies") or []:
                for level in hierarchy.get("levels") or []:
                    if not isinstance(level, dict):
                        continue
                    for metrical in level.get("metrics") or []:
                        metric_info = self._metric_from_metrical_attribute(metrical)
                        if metric_info and metric_info.dataset:
                            metrics_by_dataset.setdefault(metric_info.dataset, []).append(metric_info)

        return metrics_by_dataset

    def _metric_from_definition(self, metric_def: dict[str, Any]) -> MetricInfo | None:
        name = metric_def.get("unique_name")
        dataset = metric_def.get("dataset")
        if not name or not dataset:
            return None

        method = _normalize_calc_method(metric_def.get("calculation_method"))
        column = metric_def.get("column")
        agg = None
        metric_type = None
        sql = None

        if method in _CALC_METHOD_AGG_MAP:
            agg = _CALC_METHOD_AGG_MAP[method]
            sql = column
        elif method == "sum distinct":
            metric_type = "derived"
            if column:
                sql = f"SUM(DISTINCT {column})"
        elif method == "percentile":
            quantile = _parse_named_quantile(metric_def.get("named_quantiles"))
            if quantile == 0.5 and (metric_def.get("named_quantiles") or "").lower() == "median":
                agg = "median"
                sql = column
            else:
                metric_type = "derived"
                quantile_value = 0.5 if quantile is None else quantile
                if column:
                    sql = f"PERCENTILE_CONT({quantile_value:g}) WITHIN GROUP (ORDER BY {column})"
        elif method in _CALC_METHOD_SQL_MAP:
            metric_type = "derived"
            if column:
                sql = f"{_CALC_METHOD_SQL_MAP[method]}({column})"
        else:
            metric_type = "derived"
            sql = column

        if metric_type == "derived" and not sql:
            return None

        return MetricInfo(
            name=name,
            dataset=dataset,
            agg=agg,
            sql=sql,
            metric_type=metric_type,
            label=metric_def.get("label"),
            description=metric_def.get("description"),
            format_str=metric_def.get("format"),
        )

    def _metric_from_metrical_attribute(self, metrical: dict[str, Any]) -> MetricInfo | None:
        if not metrical:
            return None

        name = metrical.get("unique_name")
        dataset = metrical.get("dataset")
        if not name or not dataset:
            return None

        metric_def = {
            "unique_name": name,
            "dataset": dataset,
            "calculation_method": metrical.get("calculation_method"),
            "column": metrical.get("column"),
            "label": metrical.get("label"),
            "description": metrical.get("description"),
            "format": metrical.get("format"),
            "named_quantiles": metrical.get("named_quantiles"),
        }

        return self._metric_from_definition(metric_def)

    def _metric_from_calc(self, calc_def: dict[str, Any]) -> MetricInfo | None:
        name = calc_def.get("unique_name")
        expression = calc_def.get("expression")
        if not name or not expression:
            return None

        return MetricInfo(
            name=name,
            dataset=None,
            agg=None,
            sql=expression,
            metric_type="derived",
            label=calc_def.get("label"),
            description=calc_def.get("description"),
            format_str=calc_def.get("format"),
        )

    def _apply_dimension_attrs_to_models(
        self,
        dimension_attrs: dict[str, list[DimensionAttr]],
        graph: SemanticGraph,
        datasets: dict[str, DatasetInfo],
        dataset_to_model: dict[str, str],
    ) -> None:
        for dataset_name, attrs in dimension_attrs.items():
            model_name = dataset_to_model.get(dataset_name, dataset_name)
            model = graph.models.get(model_name)
            if not model:
                dataset_info = datasets.get(dataset_name)
                if dataset_info:
                    model = Model(
                        name=model_name,
                        table=dataset_info.table,
                        sql=dataset_info.sql,
                        description=dataset_info.description,
                        primary_key=self._infer_primary_key(dataset_info.columns),
                        dimensions=[],
                        metrics=[],
                    )
                else:
                    model = Model(name=model_name, table=None, primary_key="id")
                graph.add_model(model)

            for attr in attrs:
                if model.get_dimension(attr.name):
                    continue
                model.dimensions.append(
                    Dimension(
                        name=attr.name,
                        type=attr.dim_type,
                        sql=attr.sql,
                        label=attr.label,
                        description=attr.description,
                        granularity=attr.granularity,
                        parent=attr.parent,
                    )
                )

    def _apply_metric_infos_to_models(
        self,
        metric_infos: dict[str, list[MetricInfo]],
        graph: SemanticGraph,
        datasets: dict[str, DatasetInfo],
        dataset_to_model: dict[str, str],
    ) -> None:
        for dataset_name, metrics in metric_infos.items():
            if dataset_name == "__global__":
                continue
            if dataset_name in dataset_to_model:
                continue
            model_name = dataset_to_model.get(dataset_name, dataset_name)
            model = graph.models.get(model_name)
            if not model:
                dataset_info = datasets.get(dataset_name)
                if dataset_info:
                    model = Model(
                        name=model_name,
                        table=dataset_info.table,
                        sql=dataset_info.sql,
                        description=dataset_info.description,
                        primary_key=self._infer_primary_key(dataset_info.columns),
                        dimensions=[],
                        metrics=[],
                    )
                else:
                    model = Model(name=model_name, table=None, primary_key="id")
                graph.add_model(model)

            for info in metrics:
                if model.get_metric(info.name):
                    continue
                model.metrics.append(
                    Metric(
                        name=info.name,
                        agg=info.agg,
                        sql=info.sql,
                        type=info.metric_type,
                        label=info.label,
                        description=info.description,
                        format=info.format_str,
                    )
                )

        if not dataset_to_model:
            for info in metric_infos.get("__global__", []):
                for model in graph.models.values():
                    if model.get_metric(info.name):
                        continue
                    model.metrics.append(
                        Metric(
                            name=info.name,
                            agg=info.agg,
                            sql=info.sql,
                            type=info.metric_type,
                            label=info.label,
                            description=info.description,
                            format=info.format_str,
                        )
                    )

    def _build_models_from_model_defs(
        self,
        models: dict[str, dict[str, Any]],
        datasets: dict[str, DatasetInfo],
        metric_infos: dict[str, list[MetricInfo]],
        graph: SemanticGraph,
        dimensions: dict[str, dict[str, Any]],
    ) -> dict[str, str]:
        dataset_to_model: dict[str, str] = {}

        for model_def in models.values():
            model_name = model_def.get("unique_name")
            if not model_name:
                continue

            primary_dataset = self._resolve_primary_dataset(model_def, metric_infos, dimensions)
            if not primary_dataset:
                continue

            dataset_info = datasets.get(primary_dataset)
            if not dataset_info:
                continue
            dataset_to_model.setdefault(primary_dataset, model_name)

            model = Model(
                name=model_name,
                table=dataset_info.table,
                sql=dataset_info.sql,
                description=model_def.get("description") or model_def.get("label"),
                primary_key=self._infer_primary_key(dataset_info.columns),
                dimensions=[],
                metrics=[],
            )

            graph.add_model(model)
            self._attach_metrics_for_model(model_def, metric_infos, primary_dataset, model)
            self._apply_model_aggregates(model_def, model)
            self._apply_model_drillthroughs(model_def, model)
            self._apply_model_relationships(model_def, dimensions, dataset_to_model, graph)

        return dataset_to_model

    def _resolve_primary_dataset(
        self,
        model_def: dict[str, Any],
        metric_infos: dict[str, list[MetricInfo]],
        dimensions: dict[str, dict[str, Any]],
    ) -> str | None:
        from_datasets = {rel.get("from", {}).get("dataset") for rel in model_def.get("relationships") or []}
        from_datasets.discard(None)
        if len(from_datasets) == 1:
            return next(iter(from_datasets))
        if len(from_datasets) > 1:
            return None

        metric_names = [metric.get("unique_name") for metric in model_def.get("metrics") or []]
        metric_datasets = {
            info.dataset
            for dataset_metrics in metric_infos.values()
            for info in dataset_metrics
            if info.name in metric_names and info.dataset
        }
        if len(metric_datasets) == 1:
            return next(iter(metric_datasets))

        dimension_names = model_def.get("dimensions") or []
        dim_datasets = set()
        for dim_name in dimension_names:
            dim_def = dimensions.get(dim_name)
            if not dim_def:
                continue
            for attr in dim_def.get("level_attributes") or []:
                dataset_name = attr.get("dataset")
                if dataset_name:
                    dim_datasets.add(dataset_name)
        if len(dim_datasets) == 1:
            return next(iter(dim_datasets))

        return None

    def _attach_metrics_for_model(
        self,
        model_def: dict[str, Any],
        metric_infos: dict[str, list[MetricInfo]],
        primary_dataset: str,
        model: Model,
    ) -> None:
        metric_names = [metric.get("unique_name") for metric in model_def.get("metrics") or []]
        include_all = not metric_names

        for info in metric_infos.get(primary_dataset, []):
            if not include_all and info.name not in metric_names:
                continue
            if model.get_metric(info.name):
                continue
            model.metrics.append(
                Metric(
                    name=info.name,
                    agg=info.agg,
                    sql=info.sql,
                    type=info.metric_type,
                    label=info.label,
                    description=info.description,
                    format=info.format_str,
                )
            )

        for info in metric_infos.get("__global__", []):
            if not include_all and info.name not in metric_names:
                continue
            if model.get_metric(info.name):
                continue
            model.metrics.append(
                Metric(
                    name=info.name,
                    agg=info.agg,
                    sql=info.sql,
                    type=info.metric_type,
                    label=info.label,
                    description=info.description,
                    format=info.format_str,
                )
            )

    def _apply_model_relationships(
        self,
        model_def: dict[str, Any],
        dimensions: dict[str, dict[str, Any]],
        dataset_to_model: dict[str, str],
        graph: SemanticGraph,
    ) -> None:
        for rel_def in model_def.get("relationships") or []:
            rel = self._build_relationship(rel_def, dimensions, dataset_to_model)
            if not rel:
                continue

            source_dataset = rel_def.get("from", {}).get("dataset")
            if not source_dataset:
                continue
            source_model_name = model_def.get("unique_name")
            if source_model_name not in graph.models:
                source_model_name = dataset_to_model.get(source_dataset, source_dataset)

            model = graph.models.get(source_model_name)
            if not model:
                model = Model(name=source_model_name, table=None, primary_key="id")
                graph.add_model(model)

            if not any(existing.name == rel.name for existing in model.relationships):
                model.relationships.append(rel)

    def _apply_relationships_from_dimensions(
        self,
        dimensions: dict[str, dict[str, Any]],
        datasets: dict[str, DatasetInfo],
        dataset_to_model: dict[str, str],
        graph: SemanticGraph,
    ) -> None:
        for dim_def in dimensions.values():
            for rel_def in dim_def.get("relationships") or []:
                rel = self._build_relationship(rel_def, dimensions, dataset_to_model)
                if not rel:
                    continue

                source_dataset = rel_def.get("from", {}).get("dataset")
                if not source_dataset:
                    continue
                source_model_name = dataset_to_model.get(source_dataset, source_dataset)

                if source_model_name not in graph.models:
                    dataset_info = datasets.get(source_dataset)
                    if dataset_info:
                        graph.add_model(
                            Model(
                                name=source_model_name,
                                table=dataset_info.table,
                                sql=dataset_info.sql,
                                description=dataset_info.description,
                                primary_key=self._infer_primary_key(dataset_info.columns),
                                dimensions=[],
                                metrics=[],
                            )
                        )
                    else:
                        graph.add_model(Model(name=source_model_name, table=None, primary_key="id"))

                model = graph.models[source_model_name]
                if not any(existing.name == rel.name for existing in model.relationships):
                    model.relationships.append(rel)

    def _build_relationship(
        self,
        rel_def: dict[str, Any],
        dimensions: dict[str, dict[str, Any]],
        dataset_to_model: dict[str, str],
    ) -> Relationship | None:
        from_def = rel_def.get("from") or {}
        to_def = rel_def.get("to") or {}

        from_dataset = from_def.get("dataset")
        join_columns = from_def.get("join_columns") or []
        if not from_dataset or not join_columns:
            return None

        dimension_name = to_def.get("dimension")
        level_name = to_def.get("level")
        target_dataset, target_key = self._resolve_dimension_target(dimensions, dimension_name, level_name)

        if not target_dataset:
            return None

        if target_dataset == from_dataset:
            return None

        target_model_name = dataset_to_model.get(target_dataset, target_dataset)

        rel_type = rel_def.get("type")
        if rel_def.get("m2m") is True:
            rel_type = "many_to_many"
        if rel_type not in {"many_to_one", "one_to_one", "one_to_many", "many_to_many"}:
            rel_type = "many_to_one"

        return Relationship(
            name=target_model_name,
            type=rel_type,
            foreign_key=join_columns[0],
            primary_key=target_key,
        )

    def _resolve_dimension_target(
        self,
        dimensions: dict[str, dict[str, Any]],
        dimension_name: str | None,
        level_name: str | None,
    ) -> tuple[str | None, str | None]:
        if not dimension_name:
            return None, None

        dim_def = dimensions.get(dimension_name)
        if not dim_def:
            return None, None

        level_attributes = dim_def.get("level_attributes") or []
        target_attr = None

        if level_name:
            for attr in level_attributes:
                if attr.get("unique_name") == level_name:
                    target_attr = attr
                    break

        if not target_attr and level_attributes:
            target_attr = level_attributes[0]

        if not target_attr:
            return None, None

        dataset_name = target_attr.get("dataset")
        key_columns = target_attr.get("key_columns") or []
        primary_key = key_columns[0] if key_columns else None

        if not dataset_name:
            shared = target_attr.get("shared_degenerate_columns") or []
            if shared:
                dataset_name = shared[0].get("dataset")
                shared_keys = shared[0].get("key_columns") or []
                if not primary_key and shared_keys:
                    primary_key = shared_keys[0]
        return dataset_name, primary_key

    def _apply_model_aggregates(self, model_def: dict[str, Any], model: Model) -> None:
        for aggregate in model_def.get("aggregates") or []:
            agg_name = aggregate.get("unique_name")
            if not agg_name:
                continue

            metrics = []
            for metric in aggregate.get("metrics") or []:
                if isinstance(metric, dict):
                    metric_name = metric.get("unique_name")
                else:
                    metric_name = metric
                if metric_name:
                    metrics.append(metric_name)

            attributes = []
            for attr in aggregate.get("attributes") or []:
                if not isinstance(attr, dict):
                    continue
                attr_name = attr.get("name") or attr.get("dimension")
                if attr_name:
                    attributes.append(attr_name)

            model.pre_aggregations.append(
                PreAggregation(
                    name=agg_name,
                    measures=metrics or None,
                    dimensions=attributes or None,
                )
            )

    def _apply_model_drillthroughs(self, model_def: dict[str, Any], model: Model) -> None:
        drillthroughs = model_def.get("drillthroughs") or []
        if not drillthroughs:
            return

        for drill in drillthroughs:
            metrics = drill.get("metrics") or []
            attributes = drill.get("attributes") or []
            drill_fields = []
            for attr in attributes:
                if isinstance(attr, dict):
                    attr_name = attr.get("name")
                else:
                    attr_name = attr
                if attr_name:
                    drill_fields.append(attr_name)

            if not drill_fields:
                continue

            for metric_name in metrics:
                metric = model.get_metric(metric_name)
                if not metric:
                    continue
                if metric.drill_fields:
                    metric.drill_fields = list({*metric.drill_fields, *drill_fields})
                else:
                    metric.drill_fields = drill_fields

    def _write_catalog(self, output_path: Path) -> None:
        catalog_path = output_path / "catalog.yml"
        if catalog_path.exists():
            return

        catalog = {
            "unique_name": "sidemantic-export",
            "object_type": "catalog",
            "label": "Sidemantic Export",
            "version": 1.0,
            "aggressive_agg_promotion": False,
            "build_speculative_aggs": False,
        }

        with open(catalog_path, "w") as f:
            yaml.dump(catalog, f, sort_keys=False, default_flow_style=False)

    def _export_dataset(self, model: Model) -> dict[str, Any]:
        dataset: dict[str, Any] = {
            "unique_name": model.name,
            "object_type": "dataset",
            "label": model.description or model.name,
            "connection_id": "default",
        }

        if model.sql:
            dataset["sql"] = model.sql
        elif model.table:
            dataset["table"] = model.table

        columns = []
        for dim in model.dimensions:
            column = {
                "name": dim.name,
                "data_type": _dimension_type_to_data_type(dim.type),
            }
            if dim.sql and dim.sql != dim.name:
                column["sql"] = dim.sql
            columns.append(column)

        if columns:
            dataset["columns"] = columns

        return dataset

    def _export_dimension(self, model: Model) -> dict[str, Any]:
        dim_type = "time" if model.dimensions and all(d.type == "time" for d in model.dimensions) else "standard"

        dimension: dict[str, Any] = {
            "unique_name": model.name,
            "object_type": "dimension",
            "label": model.description or model.name,
            "type": dim_type,
        }

        level_attributes = []
        for dim in model.dimensions:
            attr = {
                "unique_name": dim.name,
                "label": dim.label or dim.name,
                "dataset": model.name,
                "name_column": dim.sql or dim.name,
                "key_columns": [dim.sql or dim.name],
            }
            if dim.type == "time" and dim.granularity:
                attr["time_unit"] = dim.granularity
            if dim.description:
                attr["description"] = dim.description
            level_attributes.append(attr)

        if level_attributes:
            dimension["level_attributes"] = level_attributes

        if level_attributes:
            hierarchy_levels = [{"unique_name": attr["unique_name"]} for attr in level_attributes]
            dimension["hierarchies"] = [
                {
                    "unique_name": model.name,
                    "levels": hierarchy_levels,
                }
            ]

        return dimension

    def _export_metric(self, metric: Metric, model: Model) -> dict[str, Any]:
        if metric.type and metric.type != "derived":
            return self._export_metric_calc(metric)

        if metric.agg is None:
            return self._export_metric_calc(metric)

        method_mapping = {
            "sum": "sum",
            "avg": "average",
            "count": "count non-null",
            "count_distinct": "count distinct",
            "min": "minimum",
            "max": "maximum",
            "median": "percentile",
        }

        metric_def = {
            "unique_name": metric.name,
            "object_type": "metric",
            "label": metric.label or metric.name,
            "calculation_method": method_mapping.get(metric.agg, "sum"),
            "dataset": model.name,
            "column": metric.sql or model.primary_key,
        }

        if metric.description:
            metric_def["description"] = metric.description
        if metric.format:
            metric_def["format"] = metric.format

        return metric_def

    def _export_metric_calc(self, metric: Metric) -> dict[str, Any]:
        return {
            "unique_name": metric.name,
            "object_type": "metric_calc",
            "label": metric.label or metric.name,
            "expression": metric.sql or metric.name,
        }

    def _resolve_relationship_level(self, related_model: Model | None, rel: Relationship, join_column: str) -> str:
        if not related_model:
            return rel.primary_key or "id"

        candidates = [rel.primary_key, related_model.primary_key, join_column]
        for candidate in candidates:
            if not candidate:
                continue
            for dimension in related_model.dimensions:
                if dimension.name == candidate:
                    return dimension.name
                if dimension.sql == candidate:
                    return dimension.name

        if related_model.dimensions:
            return related_model.dimensions[0].name

        return rel.primary_key or related_model.primary_key or "id"

    def _export_model(self, model: Model, models: dict[str, Model]) -> dict[str, Any]:
        model_def: dict[str, Any] = {
            "unique_name": model.name,
            "object_type": "model",
            "label": model.description or model.name,
        }

        if model.relationships:
            relationships = []
            for rel in model.relationships:
                join_column = rel.foreign_key or f"{rel.name}_id"
                related_model = models.get(rel.name)
                related_level = self._resolve_relationship_level(related_model, rel, join_column)

                relationships.append(
                    {
                        "unique_name": f"{model.name}_{rel.name}",
                        "from": {"dataset": model.name, "join_columns": [join_column]},
                        "to": {"dimension": rel.name, "level": related_level},
                        "type": rel.type,
                    }
                )

            if relationships:
                model_def["relationships"] = relationships

        dimension_names = [model.name]
        for rel in model.relationships:
            if rel.name not in dimension_names:
                dimension_names.append(rel.name)

        if dimension_names:
            model_def["dimensions"] = dimension_names

        if model.metrics:
            model_def["metrics"] = [{"unique_name": metric.name} for metric in model.metrics]

        if model.pre_aggregations:
            model_def["aggregates"] = []
            for preagg in model.pre_aggregations:
                aggregate = {
                    "unique_name": preagg.name,
                    "label": preagg.name,
                }
                if preagg.measures:
                    aggregate["metrics"] = [{"unique_name": m} for m in preagg.measures]
                if preagg.dimensions:
                    aggregate["attributes"] = [{"name": d} for d in preagg.dimensions]
                model_def["aggregates"].append(aggregate)

        return model_def
