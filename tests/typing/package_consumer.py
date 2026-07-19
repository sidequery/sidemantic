"""Downstream import smoke test for the PEP 561 package marker."""

from sidemantic import Dimension, Metric, Model, SemanticLayer
from sidemantic.adapters import (
    atscale_sml,
    bsl,
    cube,
    gooddata,
    graphene,
    hex,
    holistics,
    lookml,
    malloy,
    metricflow,
    omni,
    osi,
    rill,
    sidemantic,
    snowflake,
    superset,
    tableau,
    thoughtspot,
    tmdl,
    yardstick,
)

PUBLIC_TYPES = (Dimension, Metric, Model, SemanticLayer)
ADAPTER_MODULES = (
    atscale_sml,
    bsl,
    cube,
    gooddata,
    graphene,
    hex,
    holistics,
    lookml,
    malloy,
    metricflow,
    omni,
    osi,
    rill,
    sidemantic,
    snowflake,
    superset,
    tableau,
    thoughtspot,
    tmdl,
    yardstick,
)
