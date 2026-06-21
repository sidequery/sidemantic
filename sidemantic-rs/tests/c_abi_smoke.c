#include "sidemantic.h"

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
        fprintf(stderr, "unexpected sidemantic error: %s\n", error);
        sidemantic_free(error);
        exit(1);
    }
}

static void expect_error(char *error, const char *fragment) {
    if (error == NULL) {
        fail("expected sidemantic error, got success");
    }
    if (strstr(error, fragment) == NULL) {
        fprintf(stderr, "expected error containing '%s', got '%s'\n", fragment, error);
        sidemantic_free(error);
        exit(1);
    }
    sidemantic_free(error);
}

static void expect_rewrite_contains(SidemanticRewriteResult result, const char *fragment) {
    if (result.error != NULL) {
        fprintf(stderr, "unexpected rewrite error: %s\n", result.error);
        sidemantic_free_result(result);
        exit(1);
    }
    if (!result.was_rewritten) {
        sidemantic_free_result(result);
        fail("expected query to be rewritten");
    }
    if (result.sql == NULL || strstr(result.sql, fragment) == NULL) {
        fprintf(stderr, "expected rewritten SQL containing '%s', got '%s'\n", fragment,
                result.sql == NULL ? "<null>" : result.sql);
        sidemantic_free_result(result);
        exit(1);
    }
    sidemantic_free_result(result);
}

static void expect_passthrough(SidemanticRewriteResult result, const char *sql) {
    if (result.error != NULL) {
        fprintf(stderr, "unexpected rewrite error: %s\n", result.error);
        sidemantic_free_result(result);
        exit(1);
    }
    if (result.was_rewritten) {
        sidemantic_free_result(result);
        fail("expected passthrough rewrite result");
    }
    if (result.sql == NULL || strcmp(result.sql, sql) != 0) {
        fprintf(stderr, "expected passthrough SQL '%s', got '%s'\n", sql,
                result.sql == NULL ? "<null>" : result.sql);
        sidemantic_free_result(result);
        exit(1);
    }
    sidemantic_free_result(result);
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

    sidemantic_clear_for_context(context_a);
    sidemantic_clear_for_context(context_b);

    expect_success(sidemantic_load_yaml_for_context(context_a, yaml_a));
    expect_success(sidemantic_load_yaml_for_context(context_b, yaml_b));

    char *models = sidemantic_list_models_for_context(context_a);
    if (models == NULL || strstr(models, "orders") == NULL) {
        sidemantic_free(models);
        fail("expected context A model list to include orders");
    }
    sidemantic_free(models);

    if (!sidemantic_is_model_for_context(context_a, "orders")) {
        fail("expected context A orders model");
    }

    expect_rewrite_contains(
        sidemantic_rewrite_for_context(context_a, "SELECT orders.revenue FROM orders"),
        "orders_a");
    expect_rewrite_contains(
        sidemantic_rewrite_for_context(context_b, "SELECT orders.order_count FROM orders"),
        "orders_b");

    sidemantic_clear_for_context(context_a);
    expect_passthrough(
        sidemantic_rewrite_for_context(context_a, "SELECT orders.revenue FROM orders"),
        "SELECT orders.revenue FROM orders");
    expect_rewrite_contains(
        sidemantic_rewrite_for_context(context_b, "SELECT orders.order_count FROM orders"),
        "orders_b");

    sidemantic_clear();
    expect_success(sidemantic_load_yaml(yaml_a));
    expect_rewrite_contains(sidemantic_rewrite("SELECT orders.revenue FROM orders"), "orders_a");

    sidemantic_clear_for_context("c-abi:memory");
    expect_success(sidemantic_define_for_context(
        "c-abi:memory", "MODEL (name events, table events, primary_key event_id);", NULL,
        false));
    expect_success(sidemantic_add_definition_for_context(
        "c-abi:memory", "METRIC event_count AS COUNT(*)", NULL, false));
    expect_rewrite_contains(
        sidemantic_rewrite_for_context("c-abi:memory", "SELECT events.event_count FROM events"),
        "COUNT");

    expect_error(sidemantic_load_yaml_for_context(context_a, NULL), "null yaml pointer");

    SidemanticRewriteResult null_sql = sidemantic_rewrite_for_context(context_a, NULL);
    if (null_sql.error == NULL || strstr(null_sql.error, "null sql pointer") == NULL) {
        sidemantic_free_result(null_sql);
        fail("expected null SQL rewrite error");
    }
    sidemantic_free_result(null_sql);

    sidemantic_clear_for_context(context_b);
    sidemantic_clear_for_context("c-abi:memory");
    sidemantic_clear();

    return 0;
}
