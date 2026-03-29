#!/bin/sh
set -e

# SIDEMANTIC_MODE: "serve" (default), "mcp", "api", or "both"
MODE="${SIDEMANTIC_MODE:-serve}"
DEMO_ARGS=""
if [ -n "$SIDEMANTIC_DEMO" ]; then
    DEMO_ARGS="--demo"
fi

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

# HTTP API args
API_ARGS="--host 0.0.0.0"
if [ -n "$SIDEMANTIC_CONNECTION" ]; then
    API_ARGS="$API_ARGS --connection \"$SIDEMANTIC_CONNECTION\""
fi
if [ -n "$SIDEMANTIC_DB" ]; then
    API_ARGS="$API_ARGS --db \"$SIDEMANTIC_DB\""
fi
if [ -n "$SIDEMANTIC_API_TOKEN" ]; then
    API_ARGS="$API_ARGS --auth-token \"$SIDEMANTIC_API_TOKEN\""
fi
if [ -n "$SIDEMANTIC_API_PORT" ]; then
    API_ARGS="$API_ARGS --port \"$SIDEMANTIC_API_PORT\""
fi
if [ -n "$SIDEMANTIC_MAX_REQUEST_BODY_BYTES" ]; then
    API_ARGS="$API_ARGS --max-request-body-bytes \"$SIDEMANTIC_MAX_REQUEST_BODY_BYTES\""
fi
if [ -n "$SIDEMANTIC_CORS_ORIGINS" ]; then
    OLD_IFS="$IFS"
    IFS=','
    for ORIGIN in $SIDEMANTIC_CORS_ORIGINS; do
        API_ARGS="$API_ARGS --cors-origin \"$ORIGIN\""
    done
    IFS="$OLD_IFS"
fi

case "$MODE" in
    serve)
        eval exec sidemantic serve $SERVE_ARGS $DEMO_ARGS "$@"
        ;;
    mcp)
        eval exec sidemantic mcp-serve $MCP_ARGS $DEMO_ARGS "$@"
        ;;
    api)
        eval exec sidemantic api-serve $API_ARGS $DEMO_ARGS "$@"
        ;;
    both)
        eval sidemantic serve $SERVE_ARGS $DEMO_ARGS &
        SERVE_PID=$!
        trap "kill $SERVE_PID 2>/dev/null" EXIT
        eval exec sidemantic mcp-serve $MCP_ARGS $DEMO_ARGS "$@"
        ;;
    *)
        echo "Unknown SIDEMANTIC_MODE: $MODE (use serve, mcp, api, or both)" >&2
        exit 1
        ;;
esac
