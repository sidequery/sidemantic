"""Sidemantic anywidget for interactive metrics exploration."""

from __future__ import annotations

from typing import TYPE_CHECKING

__all__ = ["MetricsExplorer"]

if TYPE_CHECKING:
    from sidemantic.widget._widget import MetricsExplorer as MetricsExplorer


def __getattr__(name: str):
    if name != "MetricsExplorer":
        raise AttributeError(name)

    try:
        from sidemantic.widget._widget import MetricsExplorer
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "MetricsExplorer requires the optional widget dependencies. Install with `sidemantic[widget]`."
        ) from exc

    return MetricsExplorer
