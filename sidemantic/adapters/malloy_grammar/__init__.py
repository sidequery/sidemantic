# Generated ANTLR parser for Malloy language
# Grammar files from: https://github.com/malloydata/malloy
#   (packages/malloy/src/lang/grammar/MalloyParser.g4 and MalloyLexer.g4)
#
# Regenerate with the ANTLR 4.13.2 tool (matching antlr4-python3-runtime):
#   antlr -Dlanguage=Python3 -visitor MalloyLexer.g4 MalloyParser.g4
#
# Upstream adaptation note: the upstream lexer's block-annotation support (#| ... |#)
# uses TypeScript-only embedded actions in an @members block. Those have been
# translated to Python in the vendored MalloyLexer.g4 so the parser generates valid
# Python. All other rules track upstream verbatim.

from .MalloyLexer import MalloyLexer
from .MalloyParser import MalloyParser
from .MalloyParserVisitor import MalloyParserVisitor

__all__ = ["MalloyLexer", "MalloyParser", "MalloyParserVisitor"]
