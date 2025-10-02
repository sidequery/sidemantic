"""Semantic graph for managing models and relationships."""

from collections import deque
from dataclasses import dataclass

from sidemantic.core.measure import Measure
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
        self.metrics: dict[str, Measure] = {}
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
        self._build_adjacency()

    def add_metric(self, measure: Measure) -> None:
        """Add a measure to the graph.

        Args:
            measure: Measure to add
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

    def _add_metric_impl(self, measure: Measure) -> None:
        """Internal method to add a measure without checks (for legacy compatibility).

        Args:
            measure: Measure to add
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

    def get_metric(self, name: str) -> Measure:
        """Get measure by name.

        Args:
            name: Measure name

        Returns:
            Measure instance

        Raises:
            KeyError: If measure not found
        """
        if name not in self.metrics:
            raise KeyError(f"Measure {name} not found")
        return self.metrics[name]

    def _build_adjacency(self) -> None:
        """Build adjacency list for join path discovery.

        Creates edges between models using:
        1. Entity-based joins (legacy)
        2. Rails-like joins (belongs_to, has_one, has_many)
        """
        self._adjacency.clear()

        # Method 1: Entity-based joins (existing logic)
        entity_index: dict[str, list[tuple[str, str]]] = {}

        for model_name, model in self.models.items():
            for entity in model.entities:
                if entity.name not in entity_index:
                    entity_index[entity.name] = []
                entity_index[entity.name].append((model_name, entity.type))

        for entity_name, model_entities in entity_index.items():
            for i, (model_a, type_a) in enumerate(model_entities):
                for model_b, type_b in model_entities[i + 1 :]:
                    if model_a not in self._adjacency:
                        self._adjacency[model_a] = []
                    if model_b not in self._adjacency:
                        self._adjacency[model_b] = []

                    self._adjacency[model_a].append((entity_name, model_b))
                    self._adjacency[model_b].append((entity_name, model_a))

        # Method 2: Rails-like joins (new)
        for model_name, model in self.models.items():
            for join in model.joins:
                related_model = join.name
                if related_model not in self.models:
                    continue  # Skip if related model doesn't exist yet

                # Get the join key names
                if join.type == "belongs_to":
                    # This model has foreign key pointing to related model
                    # Example: orders belongs_to customers (orders.customer_id -> customers.id)
                    local_key = join.sql_expr  # customer_id (in orders)
                    remote_key = join.primary_key or "id"  # id (in customers)
                else:
                    # has_one or has_many: related model has foreign key pointing here
                    # Example: customers has_many orders (customers.id <- orders.customer_id)
                    local_key = model.primary_key or "id"  # id (in customers)
                    remote_key = join.foreign_key or join.sql_expr  # customer_id (in orders)

                # Add bidirectional edge
                if model_name not in self._adjacency:
                    self._adjacency[model_name] = []
                if related_model not in self._adjacency:
                    self._adjacency[related_model] = []

                # Use a join_key that combines both keys for clarity
                join_key = f"{local_key}={remote_key}"

                self._adjacency[model_name].append((join_key, related_model))
                self._adjacency[related_model].append((join_key, model_name))

    def find_join_path(self, from_model: str, to_model: str) -> list[JoinPath]:
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

                # Handle both entity-based and Rails-like joins
                if "=" in join_key:
                    # Rails-like join: "key1=key2"
                    # The edge was added bidirectionally, so we need to figure out which key belongs to which model
                    key1, key2 = join_key.split("=")

                    # Determine which key belongs to current model and which to next model
                    # Check the actual models to see which has which column
                    current_model_obj = self.models[current]
                    next_model_obj = self.models[next_model]

                    # Check if current model defines key1 or key2
                    # A model "has" a key if:
                    # 1. It's the primary_key
                    # 2. It's a foreign key in a belongs_to join
                    # 3. Another model has has_many/has_one pointing here with that foreign key

                    def model_has_key(model_obj, key):
                        if model_obj.primary_key == key:
                            return True
                        if any(j.sql_expr == key for j in model_obj.joins):
                            return True
                        # Check if another model points here with has_many/has_one
                        for other_model in self.models.values():
                            for j in other_model.joins:
                                if j.name == model_obj.name and j.type in ("has_one", "has_many") and j.sql_expr == key:
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
                        # Check if current model has a belongs_to join - if so, use the FK
                        belongs_to_fk = None
                        for j in current_model_obj.joins:
                            if j.type == "belongs_to" and j.name == next_model:
                                belongs_to_fk = j.sql_expr
                                break

                        if belongs_to_fk:
                            # Current has belongs_to: use FK as from_entity
                            if key1 == belongs_to_fk:
                                from_entity = key1
                                to_entity = key2
                            else:
                                from_entity = key2
                                to_entity = key1
                        else:
                            # No belongs_to - could be has_many OR we're the child of a has_many
                            # If another model has has_many pointing to us, we have the FK
                            # Check if next_model has has_many pointing to current
                            next_has_many_fk = None
                            for j in next_model_obj.joins:
                                if j.type in ("has_one", "has_many") and j.name == current:
                                    next_has_many_fk = j.sql_expr
                                    break

                            if next_has_many_fk:
                                # Next model has has_many to current, so current has the FK
                                if key1 == next_has_many_fk:
                                    from_entity = key1
                                    to_entity = key2
                                else:
                                    from_entity = key2
                                    to_entity = key1
                            else:
                                # Current has has_many to next, so use PK
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
                    # Check if current model has has_many/has_one to next_model
                    current_join = None
                    for j in current_model_obj.joins:
                        if j.name == next_model:
                            current_join = j
                            break

                    # Check if next model has has_many/has_one/belongs_to to current
                    next_join = None
                    for j in next_model_obj.joins:
                        if j.name == current:
                            next_join = j
                            break

                    # Determine relationship
                    if current_join and current_join.type == "has_many":
                        relationship = "one_to_many"
                    elif current_join and current_join.type == "has_one":
                        relationship = "one_to_one"
                    elif current_join and current_join.type == "belongs_to":
                        relationship = "many_to_one"
                    elif next_join and next_join.type == "has_many":
                        relationship = "many_to_one"  # next has many current, so current is many to next
                    elif next_join and next_join.type == "has_one":
                        relationship = "one_to_one"
                    elif next_join and next_join.type == "belongs_to":
                        relationship = "one_to_many"  # next belongs to current, so current is one to next
                    else:
                        relationship = "many_to_one"  # default
                else:
                    # Entity-based join: use entity name
                    entity_name = join_key
                    current_entity = self.models[current].get_entity(entity_name)
                    next_entity = self.models[next_model].get_entity(entity_name)

                    if not current_entity or not next_entity:
                        continue

                    # Determine relationship
                    if current_entity.type == "foreign" and next_entity.type == "primary":
                        relationship = "many_to_one"
                        local_key = current_entity.expr
                        remote_key = next_entity.expr
                    elif current_entity.type == "primary" and next_entity.type == "foreign":
                        relationship = "one_to_many"
                        local_key = current_entity.expr
                        remote_key = next_entity.expr
                    else:
                        relationship = "one_to_one"
                        local_key = current_entity.expr
                        remote_key = next_entity.expr

                    from_entity = entity_name
                    to_entity = entity_name

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
