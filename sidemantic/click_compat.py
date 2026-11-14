"""Click classes that belong to the installed Typer runtime.

Typer 0.25 and earlier are built on the standalone ``click`` package. Typer
0.26 and later vendor a trimmed copy of Click as ``typer._click`` and recognize
only those classes: a standalone ``click`` exception escapes Typer's error
handling as a traceback, and a standalone ``ParameterSource`` member never
matches the source Typer records. CLI code imports Click runtime types from
here instead of from ``click``.

``typer.Exit`` and ``typer.Abort`` are public in every supported Typer release,
so they are used directly. Group, option, and argument checks use
``typer.core.TyperGroup``, ``TyperOption``, and ``TyperArgument`` because the
vendored copy has no ``Group``, ``Option``, or ``Argument`` classes.
"""

from importlib.util import find_spec

if find_spec("typer._click") is None:
    from click import ClickException, Command, Context, Parameter, UsageError, get_current_context
    from click.core import ParameterSource
    from click.shell_completion import CompletionItem
else:
    from typer._click.core import Command, Context, Parameter, ParameterSource
    from typer._click.exceptions import ClickException, UsageError
    from typer._click.globals import get_current_context
    from typer._click.shell_completion import CompletionItem

__all__ = [
    "ClickException",
    "Command",
    "CompletionItem",
    "Context",
    "Parameter",
    "ParameterSource",
    "UsageError",
    "get_current_context",
]
