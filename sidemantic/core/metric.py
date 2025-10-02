"""Metric definitions - DEPRECATED: Use Measure instead.

This module provides backward compatibility. The Metric class is now an alias for Measure.
"""

import warnings
from typing import Literal

from pydantic import Field

from .measure import Measure


class Metric(Measure):
    """DEPRECATED: Use Measure instead.

    Metric is now unified with Measure. This class exists for backward compatibility only.

    Migration guide:
        # Old way (still works but deprecated)
        Metric(name="revenue", type="simple", measure="orders.amount")

        # New way
        Measure(name="revenue", type="simple", expr="orders.amount")

        # Or for simple aggregations, just use Measure directly
        Measure(name="revenue", agg="sum", expr="amount")
    """

    # Override to keep old field name for backward compat
    measure: str | None = Field(None, description="DEPRECATED: Use expr instead")

    def __init__(self, **data):
        # Migrate old 'measure' field to 'expr' for simple metrics
        measure_val = None
        if "measure" in data and data.get("type") == "simple":
            measure_val = data.pop("measure")
            data["expr"] = measure_val

        # Warn about deprecation
        warnings.warn(
            "Metric is deprecated and will be removed in a future version. "
            "Use Measure instead. See migration guide in docstring.",
            DeprecationWarning,
            stacklevel=2,
        )

        super().__init__(**data)

        # Keep measure field populated for backward compat
        if measure_val:
            object.__setattr__(self, "measure", measure_val)
