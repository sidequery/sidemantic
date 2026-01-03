"""Post-query processor for table calculations.

Table calculations are applied to query results after they're fetched from the database.
"""

import ast
import operator

from sidemantic.core.table_calculation import TableCalculation


class TableCalculationProcessor:
    """Processes table calculations on query results."""

    # Safe operators for expression evaluation
    _SAFE_OPERATORS = {
        ast.Add: operator.add,
        ast.Sub: operator.sub,
        ast.Mult: operator.mul,
        ast.Div: operator.truediv,
        ast.FloorDiv: operator.floordiv,
        ast.Mod: operator.mod,
        ast.Pow: operator.pow,
        ast.UAdd: operator.pos,
        ast.USub: operator.neg,
    }

    def __init__(self, calculations: list[TableCalculation]):
        """Initialize processor with table calculations.

        Args:
            calculations: List of table calculations to apply
        """
        self.calculations = calculations

    def _safe_eval(self, expr: str) -> float | None:
        """Safely evaluate a mathematical expression.

        Only allows basic arithmetic operations, no function calls or attribute access.

        Args:
            expr: Expression string containing only numbers and operators

        Returns:
            Result of evaluation or None if expression is invalid

        Raises:
            ValueError: If expression contains disallowed operations
        """
        try:
            node = ast.parse(expr, mode="eval").body
            return self._eval_node(node)
        except Exception as e:
            raise ValueError(f"Invalid expression: {expr}") from e

    def _eval_node(self, node):
        """Recursively evaluate an AST node.

        Args:
            node: AST node to evaluate

        Returns:
            Evaluated result

        Raises:
            ValueError: If node type is not allowed
        """
        if isinstance(node, ast.Constant):
            # Python 3.8+ uses ast.Constant for literals
            return node.value
        elif isinstance(node, ast.BinOp):
            # Binary operation (e.g., a + b)
            op_type = type(node.op)
            if op_type not in self._SAFE_OPERATORS:
                raise ValueError(f"Unsupported operator: {op_type.__name__}")
            left = self._eval_node(node.left)
            right = self._eval_node(node.right)
            return self._SAFE_OPERATORS[op_type](left, right)
        elif isinstance(node, ast.UnaryOp):
            # Unary operation (e.g., -a)
            op_type = type(node.op)
            if op_type not in self._SAFE_OPERATORS:
                raise ValueError(f"Unsupported operator: {op_type.__name__}")
            operand = self._eval_node(node.operand)
            return self._SAFE_OPERATORS[op_type](operand)
        else:
            raise ValueError(f"Unsupported node type: {type(node).__name__}")

    def process(self, results: list[tuple], column_names: list[str]) -> tuple[list[tuple], list[str]]:
        """Apply table calculations to query results.

        Args:
            results: Query results as list of tuples
            column_names: Names of columns in results

        Returns:
            Tuple of (processed_results, updated_column_names)
        """
        if not self.calculations or not results:
            return results, column_names

        # Convert to list of dicts for easier processing
        rows = [dict(zip(column_names, row)) for row in results]

        # Apply each calculation
        for calc in self.calculations:
            rows = self._apply_calculation(calc, rows, column_names)
            # Add new column name
            if calc.name not in column_names:
                column_names = column_names + [calc.name]

        # Convert back to tuples
        processed_results = [tuple(row.get(col) for col in column_names) for row in rows]

        return processed_results, column_names

    def _apply_calculation(self, calc: TableCalculation, rows: list[dict], column_names: list[str]) -> list[dict]:
        """Apply a single table calculation to rows.

        Args:
            calc: Table calculation to apply
            rows: Rows as list of dicts
            column_names: Available column names

        Returns:
            Updated rows with calculation applied
        """
        if calc.type == "formula":
            return self._apply_formula(calc, rows)
        elif calc.type == "percent_of_total":
            return self._apply_percent_of_total(calc, rows)
        elif calc.type == "percent_of_previous":
            return self._apply_percent_of_previous(calc, rows)
        elif calc.type == "percent_of_column_total":
            return self._apply_percent_of_column_total(calc, rows)
        elif calc.type == "running_total":
            return self._apply_running_total(calc, rows)
        elif calc.type == "rank":
            return self._apply_rank(calc, rows)
        elif calc.type == "row_number":
            return self._apply_row_number(calc, rows)
        elif calc.type == "moving_average":
            return self._apply_moving_average(calc, rows)
        elif calc.type == "percentile":
            return self._apply_percentile(calc, rows)
        else:
            raise ValueError(f"Unknown table calculation type: {calc.type}")

    def _apply_formula(self, calc: TableCalculation, rows: list[dict]) -> list[dict]:
        """Apply formula calculation.

        Formulas use ${field_name} syntax to reference columns.
        Example: "${revenue} / ${cost}"
        """
        if not calc.expression:
            raise ValueError(f"Formula calculation {calc.name} missing expression")

        for row in rows:
            # Replace ${field} with actual values
            expr = calc.expression
            for field_name, value in row.items():
                expr = expr.replace(f"${{{field_name}}}", str(value if value is not None else 0))

            # Evaluate the expression using safe evaluator
            try:
                result = self._safe_eval(expr)
                row[calc.name] = result
            except Exception:
                row[calc.name] = None

        return rows

    def _apply_percent_of_total(self, calc: TableCalculation, rows: list[dict]) -> list[dict]:
        """Calculate percent of total for a field."""
        if not calc.field:
            raise ValueError(f"percent_of_total calculation {calc.name} missing field")

        # Calculate total
        total = sum(row.get(calc.field, 0) or 0 for row in rows)

        # Calculate percentage for each row
        for row in rows:
            value = row.get(calc.field, 0) or 0
            row[calc.name] = (value / total * 100) if total != 0 else 0

        return rows

    def _apply_percent_of_previous(self, calc: TableCalculation, rows: list[dict]) -> list[dict]:
        """Calculate percent change from previous row."""
        if not calc.field:
            raise ValueError(f"percent_of_previous calculation {calc.name} missing field")

        prev_value = None
        for row in rows:
            value = row.get(calc.field)
            if prev_value is not None and prev_value != 0:
                row[calc.name] = ((value - prev_value) / prev_value * 100) if value is not None else None
            else:
                row[calc.name] = None
            prev_value = value

        return rows

    def _apply_percent_of_column_total(self, calc: TableCalculation, rows: list[dict]) -> list[dict]:
        """Calculate percent of column total (within partition)."""
        if not calc.field:
            raise ValueError(f"percent_of_column_total calculation {calc.name} missing field")

        # If partition_by is specified, calculate total per partition
        if calc.partition_by:
            # Group by partition
            partitions = {}
            for row in rows:
                partition_key = tuple(row.get(p) for p in calc.partition_by)
                if partition_key not in partitions:
                    partitions[partition_key] = []
                partitions[partition_key].append(row)

            # Calculate percent within each partition
            for partition_rows in partitions.values():
                total = sum(r.get(calc.field, 0) or 0 for r in partition_rows)
                for row in partition_rows:
                    value = row.get(calc.field, 0) or 0
                    row[calc.name] = (value / total * 100) if total != 0 else 0
        else:
            # Same as percent_of_total
            return self._apply_percent_of_total(calc, rows)

        return rows

    def _apply_running_total(self, calc: TableCalculation, rows: list[dict]) -> list[dict]:
        """Calculate running total."""
        if not calc.field:
            raise ValueError(f"running_total calculation {calc.name} missing field")

        running_sum = 0
        for row in rows:
            value = row.get(calc.field, 0) or 0
            running_sum += value
            row[calc.name] = running_sum

        return rows

    def _apply_rank(self, calc: TableCalculation, rows: list[dict]) -> list[dict]:
        """Assign rank based on field value."""
        if not calc.field:
            raise ValueError(f"rank calculation {calc.name} missing field")

        # Sort by field value descending
        sorted_rows = sorted(rows, key=lambda r: r.get(calc.field, 0) or 0, reverse=True)

        # Assign ranks (handles ties)
        rank = 1
        prev_value = None
        for i, row in enumerate(sorted_rows):
            value = row.get(calc.field)
            if value != prev_value:
                rank = i + 1
            row[calc.name] = rank
            prev_value = value

        return rows

    def _apply_row_number(self, calc: TableCalculation, rows: list[dict]) -> list[dict]:
        """Assign sequential row number."""
        for i, row in enumerate(rows, 1):
            row[calc.name] = i

        return rows

    def _apply_moving_average(self, calc: TableCalculation, rows: list[dict]) -> list[dict]:
        """Calculate moving average."""
        if not calc.field or not calc.window_size:
            raise ValueError(f"moving_average calculation {calc.name} missing field or window_size")

        for i, row in enumerate(rows):
            # Get window of values
            start_idx = max(0, i - calc.window_size + 1)
            window_values = [rows[j].get(calc.field, 0) or 0 for j in range(start_idx, i + 1)]

            # Calculate average
            row[calc.name] = sum(window_values) / len(window_values) if window_values else 0

        return rows

    def _apply_percentile(self, calc: TableCalculation, rows: list[dict]) -> list[dict]:
        """Calculate percentile of a field and add as constant column.

        The percentile value (e.g., median for p=0.5) is calculated across all rows
        and added as a constant to each row. This is useful for comparing individual
        values against the distribution.
        """
        if not calc.field:
            raise ValueError(f"percentile calculation {calc.name} missing field")
        if calc.percentile is None:
            raise ValueError(f"percentile calculation {calc.name} missing percentile value")
        if not (0 <= calc.percentile <= 1):
            raise ValueError(f"percentile value must be between 0 and 1, got {calc.percentile}")

        # Collect all non-None values
        values = [row.get(calc.field) for row in rows if row.get(calc.field) is not None]

        if not values:
            # No values to calculate percentile from
            for row in rows:
                row[calc.name] = None
            return rows

        # Sort values
        sorted_values = sorted(values)
        n = len(sorted_values)

        # Calculate percentile using linear interpolation
        # This matches numpy's default "linear" interpolation method
        pos = calc.percentile * (n - 1)
        lower_idx = int(pos)
        upper_idx = min(lower_idx + 1, n - 1)
        fraction = pos - lower_idx

        if lower_idx == upper_idx:
            percentile_value = sorted_values[lower_idx]
        else:
            percentile_value = sorted_values[lower_idx] * (1 - fraction) + sorted_values[upper_idx] * fraction

        # Add percentile value to all rows
        for row in rows:
            row[calc.name] = percentile_value

        return rows
