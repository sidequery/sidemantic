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
        self._adjacency: dict[str, list[tuple[str, str]]] = {}  # model -> [(entity, target_model)]

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

        # Build adjacency from join relationships
        for model_name, model in self.models.items():
            for relationship in model.relationships:
                related_model = relationship.name
                if related_model not in self.models:
                    continue  # Skip if related model doesn't exist yet

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

                # Add bidirectional edge
                if model_name not in self._adjacency:
                    self._adjacency[model_name] = []
                if related_model not in self._adjacency:
                    self._adjacency[related_model] = []

                # Use a join_key that combines both keys for clarity
                join_key = f"{local_key}={remote_key}"

                self._adjacency[model_name].append((join_key, related_model))
                self._adjacency[related_model].append((join_key, model_name))

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

            for join_key, next_model in self._adjacency[current]:
                if next_model in visited:
                    continue

                visited.add(next_model)

                # Parse join key: "key1=key2"
                # The edge was added bidirectionally, so we need to figure out which key belongs to which model
                key1, key2 = join_key.split("=")

                # Determine which key belongs to current model and which to next model
                # Check the actual models to see which has which column
                current_model_obj = self.models[current]
                next_model_obj = self.models[next_model]

                # Check if current model defines key1 or key2
                # A model "has" a key if:
                # 1. It's the primary_key
                # 2. It's a foreign key in a many_to_one join
                # 3. Another model has one_to_many/one_to_one pointing here with that foreign key

                def model_has_key(model_obj, key):
                    if model_obj.primary_key == key:
                        return True
                    if any(j.sql_expr == key for j in model_obj.relationships):
                        return True
                    # Check if another model points here with one_to_many/one_to_one
                    for other_model in self.models.values():
                        for j in other_model.relationships:
                            if (
                                j.name == model_obj.name
                                and j.type in ("one_to_one", "one_to_many")
                                and j.sql_expr == key
                            ):
                                return True
                    return False

                current_has_key1 = model_has_key(current_model_obj, key1)
                current_has_key2 = model_has_key(current_model_obj, key2)

                if current_has_key1 and not current_has_key2:
                    # Only key1 is in current
                    from_entity = key1
                    to_entity = key2
                elif current_has_key2 and not current_has_key1:
                    # Only key2 is in current
                    from_entity = key2
                    to_entity = key1
                elif current_has_key1 and current_has_key2:
                    # Both keys are in current model - need to figure out which goes where
                    # Check if current model has a many_to_one join - if so, use the FK
                    many_to_one_fk = None
                    for j in current_model_obj.relationships:
                        if j.type == "many_to_one" and j.name == next_model:
                            many_to_one_fk = j.sql_expr
                            break

                    if many_to_one_fk:
                        # Current has many_to_one: use FK as from_entity
                        if key1 == many_to_one_fk:
                            from_entity = key1
                            to_entity = key2
                        else:
                            from_entity = key2
                            to_entity = key1
                    else:
                        # No many_to_one - could be one_to_many OR we're the child of a one_to_many
                        # If another model has one_to_many pointing to us, we have the FK
                        # Check if next_model has one_to_many pointing to current
                        next_one_to_many_fk = None
                        for j in next_model_obj.relationships:
                            if j.type in ("one_to_one", "one_to_many") and j.name == current:
                                next_one_to_many_fk = j.sql_expr
                                break

                        if next_one_to_many_fk:
                            # Next model has one_to_many to current, so current has the FK
                            if key1 == next_one_to_many_fk:
                                from_entity = key1
                                to_entity = key2
                            else:
                                from_entity = key2
                                to_entity = key1
                        else:
                            # Current has one_to_many to next, so use PK
                            if current_model_obj.primary_key == key1:
                                from_entity = key1
                                to_entity = key2
                            elif current_model_obj.primary_key == key2:
                                from_entity = key2
                                to_entity = key1
                            else:
                                from_entity = key1
                                to_entity = key2
                else:
                    # Neither found (shouldn't happen)
                    from_entity = key1
                    to_entity = key2

                # Determine relationship based on join types
                # Check if current model has one_to_many/one_to_one to next_model
                current_relationship = None
                for j in current_model_obj.relationships:
                    if j.name == next_model:
                        current_relationship = j
                        break

                # Check if next model has one_to_many/one_to_one/many_to_one to current
                next_relationship = None
                for j in next_model_obj.relationships:
                    if j.name == current:
                        next_relationship = j
                        break

                # Determine relationship
                if current_relationship and current_relationship.type == "one_to_many":
                    relationship = "one_to_many"
                elif current_relationship and current_relationship.type == "one_to_one":
                    relationship = "one_to_one"
                elif current_relationship and current_relationship.type == "many_to_one":
                    relationship = "many_to_one"
                elif next_relationship and next_relationship.type == "one_to_many":
                    relationship = "many_to_one"  # next has many current, so current is many to next
                elif next_relationship and next_relationship.type == "one_to_one":
                    relationship = "one_to_one"
                elif next_relationship and next_relationship.type == "many_to_one":
                    relationship = "one_to_many"  # next belongs to current, so current is one to next
                else:
                    relationship = "many_to_one"  # default

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
