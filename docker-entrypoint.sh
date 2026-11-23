#!/bin/sh
set -e

# SQLCTX_MODE: "serve" (default), "mcp", "api", or "both"
MODE="${SQLCTX_MODE:-serve}"

ENTRYPOINT_ARGS_FILE=$(mktemp)
trap 'rm -f "$ENTRYPOINT_ARGS_FILE"' EXIT
printf '%s\n' "$@" > "$ENTRYPOINT_ARGS_FILE"

# The CLI reads secrets from files so they never appear in process arguments.
# Copy a secret passed by environment variable into a private (0600) file.
write_secret_file() {
    SECRET_FILE=$(mktemp)
    printf '%s\n' "$1" > "$SECRET_FILE"
}

run_serve() {
    set -- sqlctx server postgres --host 0.0.0.0
    if [ -n "$SQLCTX_CONNECTION" ]; then
        set -- "$@" --connection "$SQLCTX_CONNECTION"
    fi
    if [ -n "$SQLCTX_DB" ]; then
        set -- "$@" --db "$SQLCTX_DB"
    fi
    if [ -n "$SQLCTX_USERNAME" ]; then
        set -- "$@" --username "$SQLCTX_USERNAME"
    fi
    if [ -n "$SQLCTX_PASSWORD" ]; then
        write_secret_file "$SQLCTX_PASSWORD"
        set -- "$@" --password-file "$SECRET_FILE"
    fi
    if [ -n "$SQLCTX_PORT" ]; then
        set -- "$@" --port "$SQLCTX_PORT"
    fi
    if [ -n "$SQLCTX_DEMO" ]; then
        set -- "$@" --demo
    fi
    while IFS= read -r arg; do
        set -- "$@" "$arg"
    done < "$ENTRYPOINT_ARGS_FILE"
    exec "$@"
}

run_mcp() {
    set -- sqlctx server mcp
    if [ -n "$SQLCTX_CONNECTION" ]; then
        set -- "$@" --connection "$SQLCTX_CONNECTION"
    fi
    if [ -n "$SQLCTX_DB" ]; then
        set -- "$@" --db "$SQLCTX_DB"
    fi
    if [ -n "$SQLCTX_DEMO" ]; then
        set -- "$@" --demo
    fi
    while IFS= read -r arg; do
        set -- "$@" "$arg"
    done < "$ENTRYPOINT_ARGS_FILE"
    exec "$@"
}

run_api() {
    set -- sqlctx server api --host 0.0.0.0
    if [ -n "$SQLCTX_CONNECTION" ]; then
        set -- "$@" --connection "$SQLCTX_CONNECTION"
    fi
    if [ -n "$SQLCTX_DB" ]; then
        set -- "$@" --db "$SQLCTX_DB"
    fi
    if [ -n "$SQLCTX_API_TOKEN" ]; then
        write_secret_file "$SQLCTX_API_TOKEN"
        set -- "$@" --auth-token-file "$SECRET_FILE"
    fi
    if [ -n "$SQLCTX_API_PORT" ]; then
        set -- "$@" --port "$SQLCTX_API_PORT"
    fi
    if [ -n "$SQLCTX_MAX_REQUEST_BODY_BYTES" ]; then
        set -- "$@" --max-request-body-bytes "$SQLCTX_MAX_REQUEST_BODY_BYTES"
    fi
    if [ -n "$SQLCTX_CORS_ORIGINS" ]; then
        OLD_IFS=$IFS
        IFS=','
        for ORIGIN in $SQLCTX_CORS_ORIGINS; do
            set -- "$@" --cors-origin "$ORIGIN"
        done
        IFS=$OLD_IFS
    fi
    if [ -n "$SQLCTX_DEMO" ]; then
        set -- "$@" --demo
    fi
    while IFS= read -r arg; do
        set -- "$@" "$arg"
    done < "$ENTRYPOINT_ARGS_FILE"
    exec "$@"
}

run_both() {
    set -- sqlctx server postgres --host 0.0.0.0
    if [ -n "$SQLCTX_CONNECTION" ]; then
        set -- "$@" --connection "$SQLCTX_CONNECTION"
    fi
    if [ -n "$SQLCTX_DB" ]; then
        set -- "$@" --db "$SQLCTX_DB"
    fi
    if [ -n "$SQLCTX_USERNAME" ]; then
        set -- "$@" --username "$SQLCTX_USERNAME"
    fi
    if [ -n "$SQLCTX_PASSWORD" ]; then
        write_secret_file "$SQLCTX_PASSWORD"
        set -- "$@" --password-file "$SECRET_FILE"
    fi
    if [ -n "$SQLCTX_PORT" ]; then
        set -- "$@" --port "$SQLCTX_PORT"
    fi
    if [ -n "$SQLCTX_DEMO" ]; then
        set -- "$@" --demo
    fi
    "$@" &
    SERVE_PID=$!
    trap 'kill "$SERVE_PID" 2>/dev/null; rm -f "$ENTRYPOINT_ARGS_FILE"' EXIT

    set -- sqlctx server mcp
    if [ -n "$SQLCTX_CONNECTION" ]; then
        set -- "$@" --connection "$SQLCTX_CONNECTION"
    fi
    if [ -n "$SQLCTX_DB" ]; then
        set -- "$@" --db "$SQLCTX_DB"
    fi
    if [ -n "$SQLCTX_DEMO" ]; then
        set -- "$@" --demo
    fi
    while IFS= read -r arg; do
        set -- "$@" "$arg"
    done < "$ENTRYPOINT_ARGS_FILE"
    exec "$@"
}

case "$MODE" in
    serve)
        run_serve
        ;;
    mcp)
        run_mcp
        ;;
    api)
        run_api
        ;;
    both)
        run_both
        ;;
    *)
        echo "Unknown SQLCTX_MODE: $MODE (use serve, mcp, api, or both)" >&2
        exit 1
        ;;
esac
