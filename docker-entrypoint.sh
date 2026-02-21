#!/bin/sh
set -e

# SIDEMANTIC_MODE: "serve" (default), "mcp", or "both"
MODE="${SIDEMANTIC_MODE:-serve}"

# Build shared args from environment variables
ARGS=""
if [ -n "$SIDEMANTIC_CONNECTION" ]; then
    ARGS="$ARGS --connection $SIDEMANTIC_CONNECTION"
fi
if [ -n "$SIDEMANTIC_DB" ]; then
    ARGS="$ARGS --db $SIDEMANTIC_DB"
fi

# Serve-specific args
SERVE_ARGS=""
if [ -n "$SIDEMANTIC_USERNAME" ]; then
    SERVE_ARGS="$SERVE_ARGS --username $SIDEMANTIC_USERNAME"
fi
if [ -n "$SIDEMANTIC_PASSWORD" ]; then
    SERVE_ARGS="$SERVE_ARGS --password $SIDEMANTIC_PASSWORD"
fi
if [ -n "$SIDEMANTIC_PORT" ]; then
    SERVE_ARGS="$SERVE_ARGS --port $SIDEMANTIC_PORT"
fi

case "$MODE" in
    serve)
        exec sidemantic serve --host 0.0.0.0 $ARGS $SERVE_ARGS "$@"
        ;;
    mcp)
        exec sidemantic mcp-serve $ARGS "$@"
        ;;
    both)
        # Start PG server in background, MCP on stdio in foreground
        sidemantic serve --host 0.0.0.0 $ARGS $SERVE_ARGS &
        SERVE_PID=$!
        trap "kill $SERVE_PID 2>/dev/null" EXIT
        exec sidemantic mcp-serve $ARGS "$@"
        ;;
    *)
        echo "Unknown SIDEMANTIC_MODE: $MODE (use serve, mcp, or both)" >&2
        exit 1
        ;;
esac
