"""Semantic graph for managing models and relationships."""

from collections import deque
from dataclasses import dataclass

from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.parameter import Parameter
from sidemantic.core.table_calculation import TableCalculation


@dataclass
class JoinPath:
    """Represents a join between two models."""

    from_model: str
    to_model: str
    from_entity: str
    to_entity: str
    relationship: str  # many_to_one, one_to_many, one_to_one


class SemanticGraph:
    """Semantic graph managing models, metrics, and join relationships.

    The graph uses entities as edges to automatically discover join paths
    between models.
    """

    def __init__(self):
        self.models: dict[str, Model] = {}
        self.metrics: dict[str, Metric] = {}
        self.table_calculations: dict[str, TableCalculation] = {}
        self.parameters: dict[str, Parameter] = {}
        self._adjacency: dict[
            str, list[tuple[str, str, str, str]]
        ] = {}  # model -> [(to_model, from_key, to_key, rel_type)]

    def add_model(self, model: Model) -> None:
        """Add a model to the graph.

        Args:
            model: Model to add
        """
        if model.name in self.models:
            raise ValueError(f"Model {model.name} already exists")

        self.models[model.name] = model

        # Auto-register graph-level metrics from model
        # Graph-level metric types: time_comparison, conversion
        # These need to be accessible without model prefix
        if model.metrics:
            for metric in model.metrics:
                if metric.type in ("time_comparison", "conversion"):
                    # Register at graph level if not already there
                    if metric.name not in self.metrics:
                        self.metrics[metric.name] = metric

        self.build_adjacency()

    def add_metric(self, measure: Metric) -> None:
        """Add a measure to the graph.

        Args:
            measure: Metric to add
        """
        if measure.name in self.metrics:
            raise ValueError(f"Measure {measure.name} already exists")

        self.metrics[measure.name] = measure

    def add_table_calculation(self, calc: TableCalculation) -> None:
        """Add a table calculation to the graph.

        Args:
            calc: Table calculation to add
        """
        if calc.name in self.table_calculations:
            raise ValueError(f"Table calculation {calc.name} already exists")

        self.table_calculations[calc.name] = calc

    def get_table_calculation(self, name: str) -> TableCalculation:
        """Get a table calculation by name.

        Args:
            name: Table calculation name

        Returns:
            Table calculation object

        Raises:
            KeyError: If table calculation doesn't exist
        """
        if name not in self.table_calculations:
            raise KeyError(f"Table calculation {name} not found")

        return self.table_calculations[name]

    def add_parameter(self, param: Parameter) -> None:
        """Add a parameter to the graph.

        Args:
            param: Parameter to add

        Raises:
            ValueError: If parameter already exists
        """
        if param.name in self.parameters:
            raise ValueError(f"Parameter {param.name} already exists")

        self.parameters[param.name] = param

    def get_parameter(self, name: str) -> Parameter:
        """Get a parameter by name.

        Args:
            name: Parameter name

        Returns:
            Parameter object

        Raises:
            KeyError: If parameter doesn't exist
        """
        if name not in self.parameters:
            raise KeyError(f"Parameter {name} not found")

        return self.parameters[name]

    def _add_metric_impl(self, measure: Metric) -> None:
        """Internal method to add a measure without checks (for legacy compatibility).

        Args:
            measure: Metric to add
        """
        if measure.name in self.metrics:
            raise ValueError(f"Measure {measure.name} already exists")

        self.metrics[measure.name] = measure

    def get_model(self, name: str) -> Model:
        """Get model by name.

        Args:
            name: Model name

        Returns:
            Model instance

        Raises:
            KeyError: If model not found
        """
        if name not in self.models:
            raise KeyError(f"Model {name} not found")
        return self.models[name]

    def get_metric(self, name: str) -> Metric:
        """Get measure by name.

        Args:
            name: Metric name

        Returns:
            Measure instance

        Raises:
            KeyError: If measure not found
        """
        if name not in self.metrics:
            raise KeyError(f"Measure {name} not found")
        return self.metrics[name]

    def build_adjacency(self) -> None:
        """Build adjacency list for join path discovery.

        Creates edges between models using join relationships.

        This is automatically called when models are added, but can be called
        manually if relationships are modified after models are registered.
        """
        if not hasattr(self, "_adjacency"):
            self._adjacency = {}
        self._adjacency.clear()

        def add_edge(from_model: str, to_model: str, from_key: str, to_key: str, relationship_type: str) -> None:
            if from_model not in self._adjacency:
                self._adjacency[from_model] = []
            self._adjacency[from_model].append((to_model, from_key, to_key, relationship_type))

        def invert_relationship(relationship_type: str) -> str:
            if relationship_type == "many_to_one":
                return "one_to_many"
            if relationship_type == "one_to_many":
                return "many_to_one"
            return relationship_type

        # Build adjacency from join relationships
        for model_name, model in self.models.items():
            for relationship in model.relationships:
                related_model = relationship.name
                if related_model not in self.models:
                    continue  # Skip if related model doesn't exist yet

                if relationship.type == "many_to_many":
                    junction_model = relationship.through
                    if not junction_model or junction_model not in self.models:
                        if not relationship.foreign_key:
                            continue
                        local_key = model.primary_key
                        remote_key = relationship.foreign_key
                        add_edge(model_name, related_model, local_key, remote_key, "one_to_many")
                        add_edge(related_model, model_name, remote_key, local_key, "many_to_one")
                        continue

                    junction_self_fk, junction_related_fk = relationship.junction_keys()
                    if not junction_self_fk or not junction_related_fk:
                        continue

                    base_pk = model.primary_key
                    related_pk = relationship.primary_key or self.models[related_model].primary_key

                    add_edge(model_name, junction_model, base_pk, junction_self_fk, "one_to_many")
                    add_edge(junction_model, model_name, junction_self_fk, base_pk, "many_to_one")

                    add_edge(junction_model, related_model, junction_related_fk, related_pk, "many_to_one")
                    add_edge(related_model, junction_model, related_pk, junction_related_fk, "one_to_many")
                    continue

                # Get the join key names
                if relationship.type == "many_to_one":
                    # This model has foreign key pointing to related model
                    # Example: orders many_to_one customers (orders.customer_id -> customers.id)
                    local_key = relationship.sql_expr  # customer_id (in orders)
                    remote_key = (
                        relationship.primary_key or self.models[related_model].primary_key
                    )  # Use related model's primary key
                else:
                    # one_to_one or one_to_many: related model has foreign key pointing here
                    # Example: customers one_to_many orders (customers.id <- orders.customer_id)
                    local_key = model.primary_key  # Use model's primary key
                    remote_key = relationship.foreign_key or relationship.sql_expr  # customer_id (in orders)

                add_edge(model_name, related_model, local_key, remote_key, relationship.type)
                add_edge(related_model, model_name, remote_key, local_key, invert_relationship(relationship.type))

    def find_relationship_path(self, from_model: str, to_model: str) -> list[JoinPath]:
        """Find join path between two models using BFS.

        Args:
            from_model: Source model name
            to_model: Target model name

        Returns:
            List of JoinPath objects representing the join sequence

        Raises:
            ValueError: If no join path exists
        """
        if from_model == to_model:
            return []

        if from_model not in self.models:
            raise KeyError(f"Model {from_model} not found")
        if to_model not in self.models:
            raise KeyError(f"Model {to_model} not found")

        # BFS to find shortest path
        queue = deque([(from_model, [])])
        visited = {from_model}

        while queue:
            current, path = queue.popleft()

            if current not in self._adjacency:
                continue

            for next_model, from_key, to_key, relationship_type in self._adjacency[current]:
                if next_model in visited:
                    continue

                visited.add(next_model)
                from_entity = from_key
                to_entity = to_key
                relationship = relationship_type

                new_path = path + [
                    JoinPath(
                        from_model=current,
                        to_model=next_model,
                        from_entity=from_entity,
                        to_entity=to_entity,
                        relationship=relationship,
                    )
                ]

                if next_model == to_model:
                    return new_path

                queue.append((next_model, new_path))

        raise ValueError(f"No join path found between {from_model} and {to_model}")

    def find_all_models_for_query(self, dimensions: list[str], measures: list[str]) -> set[str]:
        """Find all models needed for a query.

        Args:
            dimensions: List of dimension references (model.dimension)
            measures: List of measure references (model.measure)

        Returns:
            Set of model names needed for the query
        """
        models = set()

        for dim in dimensions:
            if "." in dim:
                model_name = dim.split(".")[0]
                models.add(model_name)

        for measure in measures:
            if "." in measure:
                model_name = measure.split(".")[0]
                models.add(model_name)

        return models
