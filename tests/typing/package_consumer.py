"""Downstream import smoke test for the PEP 561 package marker."""

from sqlctx import Dimension, Metric, Model, SemanticLayer
from sqlctx.adapters import (
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
    snowflake,
    sqlctx,
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
    sqlctx,
    snowflake,
    superset,
    tableau,
    thoughtspot,
    tmdl,
    yardstick,
)
