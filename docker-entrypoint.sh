#!/bin/sh
set -e

# SIDEMANTIC_MODE: "serve" (default), "mcp", "api", or "both"
MODE="${SIDEMANTIC_MODE:-serve}"

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
    set -- sidemantic server postgres --host 0.0.0.0
    if [ -n "$SIDEMANTIC_CONNECTION" ]; then
        set -- "$@" --connection "$SIDEMANTIC_CONNECTION"
    fi
    if [ -n "$SIDEMANTIC_DB" ]; then
        set -- "$@" --db "$SIDEMANTIC_DB"
    fi
    if [ -n "$SIDEMANTIC_USERNAME" ]; then
        set -- "$@" --username "$SIDEMANTIC_USERNAME"
    fi
    if [ -n "$SIDEMANTIC_PASSWORD" ]; then
        write_secret_file "$SIDEMANTIC_PASSWORD"
        set -- "$@" --password-file "$SECRET_FILE"
    fi
    if [ -n "$SIDEMANTIC_PORT" ]; then
        set -- "$@" --port "$SIDEMANTIC_PORT"
    fi
    if [ -n "$SIDEMANTIC_DEMO" ]; then
        set -- "$@" --demo
    fi
    while IFS= read -r arg; do
        set -- "$@" "$arg"
    done < "$ENTRYPOINT_ARGS_FILE"
    exec "$@"
}

run_mcp() {
    set -- sidemantic server mcp
    if [ -n "$SIDEMANTIC_CONNECTION" ]; then
        set -- "$@" --connection "$SIDEMANTIC_CONNECTION"
    fi
    if [ -n "$SIDEMANTIC_DB" ]; then
        set -- "$@" --db "$SIDEMANTIC_DB"
    fi
    if [ -n "$SIDEMANTIC_DEMO" ]; then
        set -- "$@" --demo
    fi
    while IFS= read -r arg; do
        set -- "$@" "$arg"
    done < "$ENTRYPOINT_ARGS_FILE"
    exec "$@"
}

run_api() {
    set -- sidemantic server api --host 0.0.0.0
    if [ -n "$SIDEMANTIC_CONNECTION" ]; then
        set -- "$@" --connection "$SIDEMANTIC_CONNECTION"
    fi
    if [ -n "$SIDEMANTIC_DB" ]; then
        set -- "$@" --db "$SIDEMANTIC_DB"
    fi
    if [ -n "$SIDEMANTIC_API_TOKEN" ]; then
        write_secret_file "$SIDEMANTIC_API_TOKEN"
        set -- "$@" --auth-token-file "$SECRET_FILE"
    fi
    if [ -n "$SIDEMANTIC_API_PORT" ]; then
        set -- "$@" --port "$SIDEMANTIC_API_PORT"
    fi
    if [ -n "$SIDEMANTIC_MAX_REQUEST_BODY_BYTES" ]; then
        set -- "$@" --max-request-body-bytes "$SIDEMANTIC_MAX_REQUEST_BODY_BYTES"
    fi
    if [ -n "$SIDEMANTIC_CORS_ORIGINS" ]; then
        OLD_IFS=$IFS
        IFS=','
        for ORIGIN in $SIDEMANTIC_CORS_ORIGINS; do
            set -- "$@" --cors-origin "$ORIGIN"
        done
        IFS=$OLD_IFS
    fi
    if [ -n "$SIDEMANTIC_DEMO" ]; then
        set -- "$@" --demo
    fi
    while IFS= read -r arg; do
        set -- "$@" "$arg"
    done < "$ENTRYPOINT_ARGS_FILE"
    exec "$@"
}

run_both() {
    set -- sidemantic server postgres --host 0.0.0.0
    if [ -n "$SIDEMANTIC_CONNECTION" ]; then
        set -- "$@" --connection "$SIDEMANTIC_CONNECTION"
    fi
    if [ -n "$SIDEMANTIC_DB" ]; then
        set -- "$@" --db "$SIDEMANTIC_DB"
    fi
    if [ -n "$SIDEMANTIC_USERNAME" ]; then
        set -- "$@" --username "$SIDEMANTIC_USERNAME"
    fi
    if [ -n "$SIDEMANTIC_PASSWORD" ]; then
        write_secret_file "$SIDEMANTIC_PASSWORD"
        set -- "$@" --password-file "$SECRET_FILE"
    fi
    if [ -n "$SIDEMANTIC_PORT" ]; then
        set -- "$@" --port "$SIDEMANTIC_PORT"
    fi
    if [ -n "$SIDEMANTIC_DEMO" ]; then
        set -- "$@" --demo
    fi
    "$@" &
    SERVE_PID=$!
    trap 'kill "$SERVE_PID" 2>/dev/null; rm -f "$ENTRYPOINT_ARGS_FILE"' EXIT

    set -- sidemantic server mcp
    if [ -n "$SIDEMANTIC_CONNECTION" ]; then
        set -- "$@" --connection "$SIDEMANTIC_CONNECTION"
    fi
    if [ -n "$SIDEMANTIC_DB" ]; then
        set -- "$@" --db "$SIDEMANTIC_DB"
    fi
    if [ -n "$SIDEMANTIC_DEMO" ]; then
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
        echo "Unknown SIDEMANTIC_MODE: $MODE (use serve, mcp, api, or both)" >&2
        exit 1
        ;;
esac
