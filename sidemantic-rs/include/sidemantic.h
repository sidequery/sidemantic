/*
 * Sidemantic C API
 *
 * FFI bindings for the sidemantic semantic layer library.
 */

#ifndef SIDEMANTIC_H
#define SIDEMANTIC_H

#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Result from rewrite operation */
typedef struct {
    char *sql;          /* Rewritten SQL (NULL if error) */
    char *error;        /* Error message (NULL if success) */
    bool was_rewritten; /* Whether the query was rewritten (false = passthrough) */
} SidemanticRewriteResult;

/*
 * Load semantic models from YAML string.
 *
 * Returns NULL on success, error message on failure.
 * Caller must free the returned string with sidemantic_free().
 */
char *sidemantic_load_yaml(const char *yaml);

/*
 * Clear all loaded semantic models.
 */
void sidemantic_clear(void);

/*
 * Check if a table name is a registered semantic model.
 */
bool sidemantic_is_model(const char *table_name);

/*
 * Get list of registered model names (comma-separated).
 *
 * Caller must free the returned string with sidemantic_free().
 */
char *sidemantic_list_models(void);

/*
 * Rewrite a SQL query using semantic definitions.
 *
 * Returns a SidemanticRewriteResult struct.
 * Caller must free with sidemantic_free_result().
 */
SidemanticRewriteResult sidemantic_rewrite(const char *sql);

/*
 * Free a string returned by sidemantic functions.
 */
void sidemantic_free(char *ptr);

/*
 * Free a SidemanticRewriteResult.
 */
void sidemantic_free_result(SidemanticRewriteResult result);

#ifdef __cplusplus
}
#endif

#endif /* SIDEMANTIC_H */
