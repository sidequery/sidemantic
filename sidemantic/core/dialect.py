"""SQLGlot dialect extensions for Sidemantic SQL syntax."""

from sqlglot import exp, parser, tokens
from sqlglot.dialects.dialect import Dialect

# Property name aliases (SQL syntax -> Python field name)
# Shared between parser and sql_definitions module
PROPERTY_ALIASES = {
    "expression": "sql",
    "aggregation": "agg",
    "filter": "filters",
}


class ModelDef(exp.Expression):
    """MODEL() definition statement.

    Syntax:
        MODEL (
            name orders,
            table orders,
            primary_key order_id
        );
    """

    arg_types = {"expressions": True}


class DimensionDef(exp.Expression):
    """DIMENSION() definition statement.

    Syntax:
        DIMENSION (
            name status,
            type categorical,
            sql status
        );
    """

    arg_types = {"expressions": True}


class RelationshipDef(exp.Expression):
    """RELATIONSHIP() definition statement.

    Syntax:
        RELATIONSHIP (
            name customer,
            type many_to_one,
            foreign_key customer_id
        );
    """

    arg_types = {"expressions": True}


class MetricDef(exp.Expression):
    """METRIC() definition statement.

    Syntax:
        METRIC (
            name revenue,
            expression SUM(amount),
            description 'Total revenue'
        );
    """

    arg_types = {"expressions": True}


class SegmentDef(exp.Expression):
    """SEGMENT() definition statement.

    Syntax:
        SEGMENT (
            name active_users,
            expression status = 'active'
        );
    """

    arg_types = {"expressions": True}


class PropertyEQ(exp.Expression):
    """Property assignment in METRIC/SEGMENT definitions.

    Represents: name value or name 'string value'
    """

    arg_types = {"this": True, "expression": True}


class SidemanticParser(parser.Parser):
    """Extended parser with MODEL, DIMENSION, RELATIONSHIP, METRIC, and SEGMENT support."""

    FUNCTIONS = {
        **parser.Parser.FUNCTIONS,
        "MODEL": lambda args: ModelDef(expressions=args),
        "DIMENSION": lambda args: DimensionDef(expressions=args),
        "RELATIONSHIP": lambda args: RelationshipDef(expressions=args),
        "METRIC": lambda args: MetricDef(expressions=args),
        "SEGMENT": lambda args: SegmentDef(expressions=args),
    }

    def _parse_statement(self) -> exp.Expression | None:
        """Override to handle MODEL, DIMENSION, RELATIONSHIP, METRIC, and SEGMENT as statements."""
        if self._match_texts(("MODEL", "DIMENSION", "RELATIONSHIP", "METRIC", "SEGMENT")):
            func_name = self._prev.text.upper()
            self._match(tokens.TokenType.L_PAREN)

            # Parse properties
            properties = []
            while not self._match(tokens.TokenType.R_PAREN):
                prop = self._parse_property()
                if prop:
                    properties.append(prop)

                # Handle comma between properties
                if not self._match(tokens.TokenType.COMMA):
                    self._match(tokens.TokenType.R_PAREN)
                    break

            # Return appropriate definition type
            if func_name == "MODEL":
                return ModelDef(expressions=properties)
            elif func_name == "DIMENSION":
                return DimensionDef(expressions=properties)
            elif func_name == "RELATIONSHIP":
                return RelationshipDef(expressions=properties)
            elif func_name == "METRIC":
                return MetricDef(expressions=properties)
            else:  # SEGMENT
                return SegmentDef(expressions=properties)

        return super()._parse_statement()

    def _parse_property(self) -> exp.Expression | None:
        """Parse property assignment: name value or name 'value'."""
        if not self._match_texts(self._get_property_names()):
            return None

        key = self._prev.text.lower()

        # Collect tokens until comma or closing paren, respecting parentheses depth
        depth = 0
        value_parts = []

        while self._curr:
            if self._curr.token_type == tokens.TokenType.L_PAREN:
                depth += 1
                # Don't add space before opening paren if last token was identifier/function name
                if value_parts and value_parts[-1] not in ("(", ",", "="):
                    value_parts.append("(")
                else:
                    value_parts.append("(")
                self._advance()
            elif self._curr.token_type == tokens.TokenType.R_PAREN:
                if depth == 0:
                    break
                depth -= 1
                value_parts.append(")")
                self._advance()
            elif self._curr.token_type == tokens.TokenType.COMMA and depth == 0:
                break
            elif self._curr.token_type == tokens.TokenType.STRING:
                # Preserve string quotes
                if value_parts and value_parts[-1] not in ("(", ",", "=", " "):
                    value_parts.append(" ")
                value_parts.append(f"'{self._curr.text}'")
                self._advance()
            else:
                # Add space before token if needed
                curr_text = self._curr.text
                needs_space_before = value_parts and value_parts[-1] not in ("(", ",", " ")
                needs_space_after_prev = value_parts and value_parts[-1] in (" ",)  # Space already added

                if needs_space_before and not needs_space_after_prev:
                    if curr_text not in (")", ","):
                        value_parts.append(" ")

                value_parts.append(curr_text)

                # Add space after =
                if curr_text == "=":
                    value_parts.append(" ")

                self._advance()

        value = "".join(value_parts).strip()

        if not value:
            return None

        return PropertyEQ(this=exp.Identifier(this=key), expression=exp.Literal.string(value))

    @staticmethod
    def _get_property_names() -> set[str]:
        """Derive property names from all Sidemantic models."""
        from sidemantic.core.dimension import Dimension
        from sidemantic.core.metric import Metric
        from sidemantic.core.model import Model
        from sidemantic.core.relationship import Relationship
        from sidemantic.core.segment import Segment

        # Get all field names from all models
        names = set()
        names.update(field.upper() for field in Model.model_fields.keys())
        names.update(field.upper() for field in Dimension.model_fields.keys())
        names.update(field.upper() for field in Relationship.model_fields.keys())
        names.update(field.upper() for field in Metric.model_fields.keys())
        names.update(field.upper() for field in Segment.model_fields.keys())

        # Add alias keys (SQL syntax variants)
        names.update(alias.upper() for alias in PROPERTY_ALIASES.keys())

        return names


class SidemanticDialect(Dialect):
    """Sidemantic SQL dialect with METRIC and SEGMENT support."""

    class Parser(SidemanticParser):
        pass


def parse_one(sql: str) -> exp.Expression:
    """Parse SQL with Sidemantic extensions.

    Args:
        sql: SQL string with METRIC/SEGMENT definitions

    Returns:
        Parsed expression tree
    """
    dialect = SidemanticDialect()
    return dialect.parse_one(sql)


def parse(sql: str) -> list[exp.Expression]:
    """Parse multiple SQL statements with Sidemantic extensions.

    Args:
        sql: SQL string with METRIC/SEGMENT definitions

    Returns:
        List of parsed expression trees
    """
    dialect = SidemanticDialect()
    return list(dialect.parse(sql))
