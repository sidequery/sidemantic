#!/bin/sh
set -e

# SIDEMANTIC_MODE: "serve" (default), "mcp", or "both"
MODE="${SIDEMANTIC_MODE:-serve}"

# Build arg arrays for each command.
# serve accepts: --connection, --db, --host, --port, --username, --password
# mcp-serve accepts: --db only

# Serve args
SERVE_ARGS="--host 0.0.0.0"
if [ -n "$SIDEMANTIC_CONNECTION" ]; then
    SERVE_ARGS="$SERVE_ARGS --connection \"$SIDEMANTIC_CONNECTION\""
fi
if [ -n "$SIDEMANTIC_DB" ]; then
    SERVE_ARGS="$SERVE_ARGS --db \"$SIDEMANTIC_DB\""
fi
if [ -n "$SIDEMANTIC_USERNAME" ]; then
    SERVE_ARGS="$SERVE_ARGS --username \"$SIDEMANTIC_USERNAME\""
fi
if [ -n "$SIDEMANTIC_PASSWORD" ]; then
    SERVE_ARGS="$SERVE_ARGS --password \"$SIDEMANTIC_PASSWORD\""
fi
if [ -n "$SIDEMANTIC_PORT" ]; then
    SERVE_ARGS="$SERVE_ARGS --port \"$SIDEMANTIC_PORT\""
fi

# MCP args (only --db is supported)
MCP_ARGS=""
if [ -n "$SIDEMANTIC_DB" ]; then
    MCP_ARGS="$MCP_ARGS --db \"$SIDEMANTIC_DB\""
fi

case "$MODE" in
    serve)
        eval exec sidemantic serve $SERVE_ARGS "$@"
        ;;
    mcp)
        eval exec sidemantic mcp-serve $MCP_ARGS "$@"
        ;;
    both)
        eval sidemantic serve $SERVE_ARGS &
        SERVE_PID=$!
        trap "kill $SERVE_PID 2>/dev/null" EXIT
        eval exec sidemantic mcp-serve $MCP_ARGS "$@"
        ;;
    *)
        echo "Unknown SIDEMANTIC_MODE: $MODE (use serve, mcp, or both)" >&2
        exit 1
        ;;
esac
