"""Opt-in bridges to installed Apache Ossie converter CLIs.

These adapters accept a deliberately narrow native subset. They never install
packages or call a vendor API. Install ``ossie-lightdash`` or ``ossie-sigma`` in
an isolated environment and put its entry point on PATH (or supply executable).
Native presentation metadata/IDs are not retained in the executable graph.
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import tempfile
import warnings
from pathlib import Path
from typing import Any

import yaml

from sidemantic.adapters.base import BaseAdapter
from sidemantic.adapters.ossie import OssieAdapter
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.fidelity import (
    record_export_feature,
    record_export_note,
    record_import_feature,
    record_import_note,
)


class OfficialConverterError(ValueError):
    """The installed converter cannot safely complete this conversion."""


def _object(value: Any, allowed: set[str], location: str) -> dict:
    if not isinstance(value, dict):
        raise OfficialConverterError(f"{location}: expected an object")
    unknown = set(value) - allowed
    if unknown:
        raise OfficialConverterError(
            f"{location}: unsupported native properties {', '.join(sorted(unknown))}; "
            "the official bridge cannot preserve their execution or access semantics"
        )
    return value


def _items(value: Any, location: str, *, nonempty: bool = False) -> list:
    if not isinstance(value, list) or (nonempty and not value):
        raise OfficialConverterError(f"{location}: expected {'a nonempty' if nonempty else 'a'} list")
    return value


def _no_templates(value: Any, location: str) -> None:
    if isinstance(value, str) and any(token in value for token in ("{{", "{%", "${ld.", "${lightdash.")):
        raise OfficialConverterError(f"{location}: templates, parameters and user attributes are unsupported")
    if isinstance(value, dict):
        for key, child in value.items():
            _no_templates(child, f"{location}/{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _no_templates(child, f"{location}/{index}")


def _check_lightdash(document: Any, location: str) -> None:
    model = _object(
        document,
        {
            "type",
            "name",
            "description",
            "sql_from",
            "primary_key",
            "dimensions",
            "metrics",
            "joins",
            "label",
            "ai_hint",
        },
        location,
    )
    if model.get("type") not in {"model", "model/v1", "model/v1beta"}:
        raise OfficialConverterError(f"{location}: only native Lightdash model files are supported, not dbt schemas")
    if not isinstance(model.get("sql_from"), str) or not model["sql_from"].strip():
        raise OfficialConverterError(f"{location}: native model requires sql_from")
    if not isinstance(model.get("name"), str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", model["name"]):
        raise OfficialConverterError(f"{location}: invalid Lightdash model name")
    _no_templates(model, location)

    def metrics(value: Any, where: str) -> None:
        if not isinstance(value, dict):
            raise OfficialConverterError(f"{where}: expected named metrics")
        for name, metric in value.items():
            _object(metric, {"type", "sql", "description", "label", "ai_hint", "percentile"}, f"{where}/{name}")

    for index, dimension in enumerate(_items(model.get("dimensions"), f"{location}/dimensions", nonempty=True)):
        field = _object(
            dimension,
            {
                "name",
                "type",
                "sql",
                "description",
                "label",
                "ai_hint",
                "time_intervals",
                "metrics",
                "hidden",
            },
            f"{location}/dimensions/{index}",
        )
        # Hidden measure-only dimensions are an official export representation;
        # they are a visibility hint, not a security restriction.
        if field.get("time_intervals") not in (None, "OFF"):
            raise OfficialConverterError(f"{location}: custom time_intervals are unsupported")
        # The converter emits dimension references as physical columns instead
        # of expanding the referenced dimension's SQL. Keep physical expressions
        # explicit so a renamed/calculated dimension cannot bind to another
        # warehouse column with the same name.
        sql = field.get("sql", "")
        if isinstance(sql, str) and any(reference != "TABLE" for reference in re.findall(r"\$\{([^}]+)\}", sql)):
            raise OfficialConverterError(
                f"{location}/dimensions/{index}: calculated-field references are unsupported; "
                "dimension SQL may reference physical columns through ${TABLE} only"
            )
        if "metrics" in field:
            metrics(field["metrics"], f"{location}/dimensions/{index}/metrics")
    if "metrics" in model:
        metrics(model["metrics"], f"{location}/metrics")
    for index, join in enumerate(_items(model.get("joins", []), f"{location}/joins")):
        edge = _object(join, {"join", "sql_on", "relationship"}, f"{location}/joins/{index}")
        if edge.get("relationship", "many-to-one") != "many-to-one":
            raise OfficialConverterError(f"{location}: only many-to-one Lightdash joins are supported")


def _check_sigma(document: Any, location: str) -> None:
    model = _object(
        document,
        {
            "kind",
            "name",
            "description",
            "pages",
            "schemaVersion",
            "dataModelId",
            "folderId",
            "documentVersion",
            "latestDocumentVersion",
            "createdAt",
            "createdBy",
            "updatedAt",
            "updatedBy",
            "ownerId",
            "url",
        },
        location,
    )
    if model.get("kind") != "data-model":
        raise OfficialConverterError(f"{location}: expected a Sigma data-model code representation")
    elements = []
    for index, page in enumerate(_items(model.get("pages"), f"{location}/pages", nonempty=True)):
        node = _object(page, {"id", "name", "elements"}, f"{location}/pages/{index}")
        elements.extend(_items(node.get("elements"), f"{location}/pages/{index}/elements"))
    if not elements:
        raise OfficialConverterError(f"{location}: no Sigma table elements")
    connections = {element.get("source", {}).get("connectionId") for element in elements if isinstance(element, dict)}
    if len(connections) != 1 or not all(value is None or (isinstance(value, str) and value) for value in connections):
        raise OfficialConverterError(
            f"{location}: Sigma tables cannot mix connectionId values; explicit source mapping is required"
        )
    elements_by_id = {element.get("id"): element for element in elements if isinstance(element, dict)}
    if len(elements_by_id) != len(elements) or any(not isinstance(key, str) or not key for key in elements_by_id):
        raise OfficialConverterError(f"{location}: Sigma tables require unique nonempty IDs")
    for index, element in enumerate(elements):
        where = f"{location}/elements/{index}"
        node = _object(
            element,
            {
                "id",
                "kind",
                "name",
                "description",
                "source",
                "columns",
                "metrics",
                "relationships",
                "uniqueKeys",
            },
            where,
        )
        if node.get("kind") != "table":
            raise OfficialConverterError(f"{where}: only table elements are supported")
        source = _object(node.get("source"), {"kind", "connectionId", "path"}, f"{where}/source")
        if source.get("kind") != "warehouse-table":
            raise OfficialConverterError(f"{where}: only warehouse-table sources are supported")
        path = _items(source.get("path"), f"{where}/source/path", nonempty=True)
        if any(not isinstance(part, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", part) for part in path):
            raise OfficialConverterError(
                f"{where}/source/path: Sigma source path components must be simple unquoted identifiers; "
                "the official converter cannot preserve quoted names or component boundaries containing dots"
            )
        for kind in ("columns", "metrics"):
            for item_index, item in enumerate(
                _items(node.get(kind, []), f"{where}/{kind}", nonempty=kind == "columns")
            ):
                _object(item, {"id", "name", "formula", "description", "format"}, f"{where}/{kind}/{item_index}")
        columns = node["columns"]
        for column in columns:
            formula = column.get("formula")
            match = re.fullmatch(r"\[([^\[\]/]+)\]", formula) if isinstance(formula, str) else None
            if not match or any(other is not column and other.get("name") == match[1] for other in columns):
                raise OfficialConverterError(
                    f"{where}/columns: calculated-field references are unsupported; "
                    "Sigma columns require direct unqualified physical references, without sibling-column references"
                )
        for item_index, item in enumerate(_items(node.get("relationships", []), f"{where}/relationships")):
            rel = _object(
                item,
                {
                    "id",
                    "name",
                    "description",
                    "targetElementId",
                    "relationshipType",
                    "keys",
                },
                f"{where}/relationships/{item_index}",
            )
            if rel.get("relationshipType", "N:1") != "N:1":
                raise OfficialConverterError(f"{where}: only N:1 Sigma relationships are supported")
            for pair in _items(rel.get("keys"), f"{where}/relationships/{item_index}/keys", nonempty=True):
                _object(pair, {"sourceColumnId", "targetColumnId"}, f"{where}/relationship/key")
                target = elements_by_id.get(rel.get("targetElementId"))
                if target is None:
                    raise OfficialConverterError(f"{where}: relationship target must be a modeled table")
                for key, owner in (("sourceColumnId", node), ("targetColumnId", target)):
                    identifier = pair.get(key)
                    columns = owner.get("columns", [])
                    matches = [column for column in columns if column.get("id") == identifier]
                    if not isinstance(identifier, str) or not identifier or len(matches) != 1:
                        raise OfficialConverterError(
                            f"{where}: every relationship key requires {key} naming exactly one modeled column"
                        )


class _OfficialAdapter(BaseAdapter):
    vendor: str
    command: str

    def __init__(
        self,
        *,
        target_dialect: str = "duckdb",
        expression_dialect: str | None = None,
        export_scope_name: str = "sidemantic",
        executable: str | Path | None = None,
        timeout: float = 60,
    ) -> None:
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        self.target_dialect = target_dialect
        self.expression_dialect = expression_dialect.upper() if expression_dialect else None
        self.export_scope_name = export_scope_name
        self.executable = str(executable) if executable is not None else self.command
        self.timeout = timeout

    def _run(self, arguments: list[str], *, exporting: bool = False) -> None:
        executable = shutil.which(self.executable)
        if executable is None:
            raise OfficialConverterError(
                f"{self.command} is required: install the official Apache Ossie {self.vendor} converter "
                "in a separate environment and put its executable on PATH. Sidemantic does not install it automatically."
            )
        try:
            result = subprocess.run(
                [executable, *arguments],
                capture_output=True,
                text=True,
                timeout=self.timeout,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            raise OfficialConverterError(f"{self.command} exceeded its {self.timeout:g}s timeout") from exc
        except OSError as exc:
            raise OfficialConverterError(f"Cannot execute {self.command}: {exc}") from exc
        diagnostics = result.stderr.strip()
        if diagnostics:
            warnings.warn(f"{self.command}:\n{diagnostics}", UserWarning, stacklevel=3)
        if result.returncode:
            raise OfficialConverterError(
                f"{self.command} failed (exit {result.returncode}): {diagnostics or result.stdout.strip()}"
            )
        # Both supported CLI contracts have stable issue headers. Reject unknown
        # issues too, so upgrading a converter cannot silently broaden semantics.
        if self.vendor == "sigma":
            warning_headers = re.findall(r"(?m)^\[WARNING\]([^\n]*)", diagnostics)
            issues = []
            for header in warning_headers:
                match = re.match(r"\s+([A-Z][A-Z_]+):", header)
                issues.append(match[1] if match else "UNRECOGNIZED_WARNING")
            safe = {"OPAQUE_DATATYPE"}
        else:
            issues = re.findall(r"(?m)^(\S+)\s+\(\d+ elements?\)", diagnostics)
            safe = {"SOURCE_UNQUALIFIED", "METRIC_REFERENCE_INLINED", "DIMENSION_TYPE_DEFAULTED"}
        note = record_export_note if exporting else record_import_note
        feature = record_export_feature if exporting else record_import_feature
        for issue in issues:
            note("official_converter_issue", f"{self.command}: {issue}\n{diagnostics}", severity="approximated")
        rejected = set(issues) - safe
        if rejected:
            feature("official_converter_semantics", "rejected", detail=", ".join(sorted(rejected)))
        if (
            rejected
            or (self.vendor == "sigma" and diagnostics.count("[WARNING]") != len(issues))
            or re.search(r"(?mi)^Skipped ", diagnostics)
        ):
            raise OfficialConverterError(
                f"{self.command} reported unsupported conversion issues: {', '.join(sorted(rejected)) or diagnostics}"
            )

    @staticmethod
    def _read(path: Path) -> Any:
        if not path.is_file() or not path.stat().st_size:
            raise OfficialConverterError(f"Converter produced no nonempty output: {path}")
        try:
            return json.loads(path.read_text()) if path.suffix == ".json" else yaml.safe_load(path.read_text())
        except (ValueError, yaml.YAMLError) as exc:
            raise OfficialConverterError(f"Invalid converter output {path}: {exc}") from exc

    def _arguments(self, exporting: bool, source: Path, output: Path) -> list[str]:
        if self.vendor == "sigma":
            return ["ossie-to-sigma" if exporting else "sigma-to-ossie", "-i", str(source), "-o", str(output)]
        if self.expression_dialect is None:
            raise OfficialConverterError(
                "Lightdash conversion requires an explicit expression_dialect (--ossie-expression-dialect)"
            )
        return [
            "export" if exporting else "import",
            "-i",
            str(source),
            "-o",
            str(output),
            "--dialect",
            self.expression_dialect,
            "--verbose",
        ]

    def _stage_native(self, source: Path, directory: Path) -> Path:
        if not source.exists():
            raise FileNotFoundError(source)
        if self.vendor == "sigma":
            if not source.is_file():
                raise OfficialConverterError("Sigma requires one native JSON file")
            document = self._read(source)
            _check_sigma(document, str(source))
            staged = directory / "source.json"
            staged.write_text(json.dumps(document))
            return staged
        files = (
            [source]
            if source.is_file()
            else sorted(path for path in source.rglob("*") if path.is_file() and path.suffix in {".yaml", ".yml"})
        )
        staged = directory / "source"
        staged.mkdir()
        count = 0
        for path in files:
            document = self._read(path)
            if path.name == "lightdash.config.yml":
                # Only the generated connection-free project envelope is accepted.
                config = _object(document, {"name", "version", "warehouse"}, str(path))
                _object(config.get("warehouse", {}), {"type"}, f"{path}/warehouse")
                continue
            _check_lightdash(document, str(path))
            (staged / f"model_{count}.yml").write_text(yaml.safe_dump(document, sort_keys=False))
            count += 1
        if not count:
            raise OfficialConverterError("No native Lightdash model files found")
        return staged

    def parse(self, source: str | Path) -> SemanticGraph:
        try:
            return self._parse(source)
        except ValueError as exc:
            record_import_feature("official_converter_semantics", "rejected", detail=str(exc))
            raise

    def _parse(self, source: str | Path) -> SemanticGraph:
        with tempfile.TemporaryDirectory(prefix="sidemantic-official-") as temporary:
            directory = Path(temporary)
            staged = self._stage_native(Path(source), directory)
            native_relationships = self._native_relationships(staged)
            output = directory / "model.ossie.yaml"
            self._run(self._arguments(False, staged, output))
            document = self._read(output)
            if not isinstance(document, dict) or not document.get("datasets"):
                raise OfficialConverterError("Official converter returned no executable datasets")
            self._reject_runtime(document)
            if self.vendor == "sigma":
                self._bind_sigma_metric_references(document)
                output.write_text(yaml.safe_dump(document, sort_keys=False))
            self._check_counts(self._native_counts(staged), self._ossie_counts(document))
            self._check_relationships(native_relationships, self._ossie_relationships(document))
            record_import_note(
                "native_presentation",
                "The official bridge imports executable core semantics; native IDs and presentation metadata are not retained.",
                severity="approximated",
            )
            graph = OssieAdapter(target_dialect=self.target_dialect).parse(output)
            if not graph.models:
                raise OfficialConverterError("Official converter returned an empty graph")
            return graph

    @staticmethod
    def _bind_sigma_metric_references(document: dict) -> None:
        """Bind Sigma's quoted display names to Ossie's logical declarations.

        The official converter quotes every Sigma metric identifier verbatim,
        but writes declaration names unquoted. Ossie folds those declarations
        to uppercase. Bind exact native names before lowering; never change the
        physical SQL expressions on dataset fields or guess unresolved names.
        """
        import sqlglot
        from sqlglot import exp

        from sidemantic.interchange.ossie.identifier import normalize_identifier

        datasets = {dataset["name"]: dataset for dataset in document["datasets"]}
        for metric in document.get("metrics", []):
            for entry in metric.get("expression", {}).get("dialects", []):
                if entry.get("dialect") != "ANSI_SQL":
                    continue
                tree = sqlglot.parse_one(entry["expression"])
                for column in tree.find_all(exp.Column):
                    if column.table not in datasets:
                        continue
                    dataset = datasets[column.table]
                    names = {field["name"] for field in dataset.get("fields", [])}
                    if column.name not in names:
                        continue
                    column.set("table", exp.to_identifier(normalize_identifier(dataset["name"]), quoted=True))
                    column.set("this", exp.to_identifier(normalize_identifier(column.name), quoted=True))
                entry["expression"] = tree.sql()

    @staticmethod
    def _reject_runtime(value: Any) -> None:
        if isinstance(value, dict):
            if str(value.get("vendor_name", "")).upper() == "SIDEMANTIC":
                raise OfficialConverterError("Official converters cannot execute Sidemantic runtime extensions")
            for item in value.values():
                _OfficialAdapter._reject_runtime(item)
        elif isinstance(value, list):
            for item in value:
                _OfficialAdapter._reject_runtime(item)

    @staticmethod
    def _ossie_counts(document: dict) -> tuple[int, int, int, int]:
        datasets = document.get("datasets", [])
        return (
            len(datasets),
            sum(len(item.get("fields", [])) for item in datasets),
            len(document.get("metrics", [])) + sum(len(item.get("metrics", [])) for item in datasets),
            len(document.get("relationships", [])),
        )

    def _native_counts(self, path: Path) -> tuple[int, int, int, int]:
        if self.vendor == "sigma":
            document = self._read(path)
            items = [item for page in document["pages"] for item in page["elements"]]
            return (
                len(items),
                sum(len(item["columns"]) for item in items),
                sum(len(item.get("metrics", [])) for item in items),
                sum(len(item.get("relationships", [])) for item in items),
            )
        files = [path] if path.is_file() else sorted(path.rglob("*.yml"))
        models = [self._read(file) for file in files if file.name != "lightdash.config.yml"]
        return (
            len(models),
            sum(len(item["dimensions"]) for item in models),
            sum(
                len(item.get("metrics", {})) + sum(len(dim.get("metrics", {})) for dim in item["dimensions"])
                for item in models
            ),
            sum(len(item.get("joins", [])) for item in models),
        )

    @staticmethod
    def _ossie_relationships(document: dict) -> list[tuple]:
        result = []
        for relationship in document.get("relationships", []):
            left, right = relationship.get("from_columns", []), relationship.get("to_columns", [])
            if len(left) != len(right):
                raise OfficialConverterError("Official converter produced unequal relationship key lengths")
            result.append((relationship["from"], relationship["to"], tuple(zip(left, right))))
        return sorted(result)

    def _native_relationships(self, path: Path) -> list[tuple]:
        result = []
        if self.vendor == "sigma":
            document = self._read(path)
            elements = {item["id"]: item for page in document["pages"] for item in page["elements"]}

            def field_name(element: dict, identifier: str) -> str:
                field = next(column for column in element["columns"] if column["id"] == identifier)
                if field.get("name"):
                    return field["name"]
                match = re.fullmatch(r"\[([^\[\]]+)\]", field.get("formula", ""))
                return match[1].rsplit("/", 1)[-1] if match else field["id"]

            for element in elements.values():
                for relationship in element.get("relationships", []):
                    target = elements[relationship["targetElementId"]]
                    pairs = tuple(
                        (field_name(element, pair["sourceColumnId"]), field_name(target, pair["targetColumnId"]))
                        for pair in relationship["keys"]
                    )
                    result.append((element.get("name", element["id"]), target.get("name", target["id"]), pairs))
        else:
            files = [path] if path.is_file() else sorted(path.rglob("*.yml"))
            for file in files:
                if file.name == "lightdash.config.yml":
                    continue
                model = self._read(file)
                for relationship in model.get("joins", []):
                    source, target = model["name"], relationship.get("join")
                    pairs = []
                    for predicate in re.split(r"\s+AND\s+", relationship.get("sql_on", ""), flags=re.IGNORECASE):
                        match = re.fullmatch(
                            r"\s*\$\{([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\}\s*=\s*"
                            r"\$\{([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\}\s*",
                            predicate,
                        )
                        if not match or {match[1], match[3]} != {source, target} or source == target:
                            raise OfficialConverterError(
                                "Lightdash bridge supports only direct equality join pairs joined by AND"
                            )
                        pairs.append((match[2], match[4]) if match[1] == source else (match[4], match[2]))
                    result.append((source, target, tuple(pairs)))
        return sorted(result)

    @staticmethod
    def _check_relationships(expected: list[tuple], actual: list[tuple]) -> None:
        if expected != actual:
            raise OfficialConverterError(
                "Official converter changed relationship identity or ordered key pairs; "
                f"expected {expected!r}, got {actual!r}"
            )

    @staticmethod
    def _check_counts(expected: tuple[int, int, int, int], actual: tuple[int, int, int, int]) -> None:
        if expected != actual or not actual[0] or not actual[1]:
            raise OfficialConverterError(
                "Official converter changed semantic member counts (models, fields, metrics, relationships): "
                f"expected {expected}, got {actual}"
            )

    def export(self, graph: SemanticGraph, output_path: str | Path) -> None:
        try:
            self._export(graph, output_path)
        except ValueError as exc:
            record_export_feature("official_converter_semantics", "rejected", detail=str(exc))
            raise

    def _export(self, graph: SemanticGraph, output_path: str | Path) -> None:
        if self.expression_dialect is None:
            raise OfficialConverterError(
                "Official converter export requires --ossie-expression-dialect (the graph SQL dialect)"
            )
        if self.vendor == "sigma" and self.expression_dialect != "ANSI_SQL":
            raise OfficialConverterError(
                "Sigma export requires ANSI_SQL; this bridge does not relabel other SQL dialects"
            )
        if not graph.models:
            raise OfficialConverterError("Cannot export an empty semantic graph")
        destination = Path(output_path)
        if (
            self.vendor == "lightdash"
            and destination.exists()
            and (not destination.is_dir() or any(destination.iterdir()))
        ):
            raise OfficialConverterError("Lightdash export requires a new or empty destination directory")
        with tempfile.TemporaryDirectory(prefix="sidemantic-official-") as temporary:
            directory = Path(temporary)
            source = directory / "model.ossie.yaml"
            OssieAdapter().export(
                graph,
                source,
                scope_name=self.export_scope_name,
                expression_dialect=self.expression_dialect,
                portable_only=True,
                official_compatible=True,
            )
            document = self._read(source)
            self._reject_runtime(document)
            output = directory / ("native.json" if self.vendor == "sigma" else "project")
            self._run(self._arguments(True, source, output), exporting=True)
            if self.vendor == "sigma":
                _check_sigma(self._read(output), str(output))
                self._check_counts(self._ossie_counts(document), self._native_counts(output))
                self._check_relationships(self._ossie_relationships(document), self._native_relationships(output))
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(output, destination)
            else:
                models = sorted((output / "lightdash" / "models").glob("*.yml"))
                if not models:
                    raise OfficialConverterError("Official converter produced no Lightdash model files")
                for model in models:
                    _check_lightdash(self._read(model), str(model))
                self._read(output / "lightdash.config.yml")
                self._check_counts(self._ossie_counts(document), self._native_counts(output))
                self._check_relationships(self._ossie_relationships(document), self._native_relationships(output))
                shutil.copytree(output, destination, dirs_exist_ok=True)
            record_export_feature(
                "official_converter_delivery",
                "exact",
                detail="Offline native files only; connection settings and deployment have not been verified with the vendor.",
            )


class LightdashAdapter(_OfficialAdapter):
    """Native Lightdash model files through the separately installed official CLI."""

    vendor = "lightdash"
    command = "ossie-lightdash"


class SigmaAdapter(_OfficialAdapter):
    """Warehouse-table Sigma code representations through the official CLI."""

    vendor = "sigma"
    command = "ossie-sigma"
