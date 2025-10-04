"""Base adapter interface for importing semantic models."""

from abc import ABC, abstractmethod
from pathlib import Path

from sidemantic.core.semantic_graph import SemanticGraph


class BaseAdapter(ABC):
    """Base adapter for importing/exporting semantic models from external formats."""

    @abstractmethod
    def parse(self, source: str | Path) -> SemanticGraph:
        """Parse external format into semantic graph.

        Args:
            source: Path to file or directory containing semantic model definitions

        Returns:
            Semantic graph with imported models and metrics
        """
        raise NotImplementedError

    def export(self, graph: SemanticGraph, output_path: str | Path) -> None:
        """Export semantic graph to external format.

        Args:
            graph: Semantic graph to export
            output_path: Path to output file

        Raises:
            NotImplementedError: If export is not supported by this adapter
        """
        raise NotImplementedError(f"{self.__class__.__name__} does not support export")

    def validate(self, graph: SemanticGraph) -> list[str]:
        """Validate imported semantic graph.

        Args:
            graph: Semantic graph to validate

        Returns:
            List of validation errors (empty if valid)
        """
        errors = []

        # Basic validation
        for model_name, model in graph.models.items():
            # Check for primary key
            if not model.primary_key:
                errors.append(f"Model {model_name} has no primary key")

            # Check for table or SQL
            if not model.table and not model.sql:
                errors.append(f"Model {model_name} has neither table nor sql definition")

        return errors
