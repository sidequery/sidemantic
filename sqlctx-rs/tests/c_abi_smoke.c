#include "sqlctx.h"

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void fail(const char *message) {
    fprintf(stderr, "%s\n", message);
    exit(1);
}

static void expect_success(char *error) {
    if (error != NULL) {
        fprintf(stderr, "unexpected sqlctx error: %s\n", error);
        sqlctx_free(error);
        exit(1);
    }
}

static void expect_error(char *error, const char *fragment) {
    if (error == NULL) {
        fail("expected sqlctx error, got success");
    }
    if (strstr(error, fragment) == NULL) {
        fprintf(stderr, "expected error containing '%s', got '%s'\n", fragment, error);
        sqlctx_free(error);
        exit(1);
    }
    sqlctx_free(error);
}

static void expect_rewrite_contains(SqlctxRewriteResult result, const char *fragment) {
    if (result.error != NULL) {
        fprintf(stderr, "unexpected rewrite error: %s\n", result.error);
        sqlctx_free_result(result);
        exit(1);
    }
    if (!result.was_rewritten) {
        sqlctx_free_result(result);
        fail("expected query to be rewritten");
    }
    if (result.sql == NULL || strstr(result.sql, fragment) == NULL) {
        fprintf(stderr, "expected rewritten SQL containing '%s', got '%s'\n", fragment,
                result.sql == NULL ? "<null>" : result.sql);
        sqlctx_free_result(result);
        exit(1);
    }
    sqlctx_free_result(result);
}

static void expect_passthrough(SqlctxRewriteResult result, const char *sql) {
    if (result.error != NULL) {
        fprintf(stderr, "unexpected rewrite error: %s\n", result.error);
        sqlctx_free_result(result);
        exit(1);
    }
    if (result.was_rewritten) {
        sqlctx_free_result(result);
        fail("expected passthrough rewrite result");
    }
    if (result.sql == NULL || strcmp(result.sql, sql) != 0) {
        fprintf(stderr, "expected passthrough SQL '%s', got '%s'\n", sql,
                result.sql == NULL ? "<null>" : result.sql);
        sqlctx_free_result(result);
        exit(1);
    }
    sqlctx_free_result(result);
}

int main(void) {
    const char *context_a = "c-abi:a";
    const char *context_b = "c-abi:b";
    const char *yaml_a =
        "models:\n"
        "  - name: orders\n"
        "    table: orders_a\n"
        "    primary_key: order_id\n"
        "    metrics:\n"
        "      - name: revenue\n"
        "        agg: sum\n"
        "        sql: amount\n";
    const char *yaml_b =
        "models:\n"
        "  - name: orders\n"
        "    table: orders_b\n"
        "    primary_key: order_id\n"
        "    metrics:\n"
        "      - name: order_count\n"
        "        agg: count\n";

    sqlctx_clear_for_context(context_a);
    sqlctx_clear_for_context(context_b);

    expect_success(sqlctx_load_yaml_for_context(context_a, yaml_a));
    expect_success(sqlctx_load_yaml_for_context(context_b, yaml_b));

    char *models = sqlctx_list_models_for_context(context_a);
    if (models == NULL || strstr(models, "orders") == NULL) {
        sqlctx_free(models);
        fail("expected context A model list to include orders");
    }
    sqlctx_free(models);

    if (!sqlctx_is_model_for_context(context_a, "orders")) {
        fail("expected context A orders model");
    }

    expect_rewrite_contains(
        sqlctx_rewrite_for_context(context_a, "SELECT orders.revenue FROM orders"),
        "orders_a");
    expect_rewrite_contains(
        sqlctx_rewrite_for_context(context_b, "SELECT orders.order_count FROM orders"),
        "orders_b");

    sqlctx_clear_for_context(context_a);
    expect_passthrough(
        sqlctx_rewrite_for_context(context_a, "SELECT orders.revenue FROM orders"),
        "SELECT orders.revenue FROM orders");
    expect_rewrite_contains(
        sqlctx_rewrite_for_context(context_b, "SELECT orders.order_count FROM orders"),
        "orders_b");

    sqlctx_clear();
    expect_success(sqlctx_load_yaml(yaml_a));
    expect_rewrite_contains(sqlctx_rewrite("SELECT orders.revenue FROM orders"), "orders_a");

    sqlctx_clear_for_context("c-abi:memory");
    expect_success(sqlctx_define_for_context(
        "c-abi:memory", "MODEL (name events, table events, primary_key event_id);", NULL,
        false));
    expect_success(sqlctx_add_definition_for_context(
        "c-abi:memory", "METRIC event_count AS COUNT(*)", NULL, false));
    expect_rewrite_contains(
        sqlctx_rewrite_for_context("c-abi:memory", "SELECT events.event_count FROM events"),
        "COUNT");

    expect_error(sqlctx_load_yaml_for_context(context_a, NULL), "null yaml pointer");

    SqlctxRewriteResult null_sql = sqlctx_rewrite_for_context(context_a, NULL);
    if (null_sql.error == NULL || strstr(null_sql.error, "null sql pointer") == NULL) {
        sqlctx_free_result(null_sql);
        fail("expected null SQL rewrite error");
    }
    sqlctx_free_result(null_sql);

    sqlctx_clear_for_context(context_b);
    sqlctx_clear_for_context("c-abi:memory");
    sqlctx_clear();

    return 0;
}
