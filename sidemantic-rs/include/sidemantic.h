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
char *sidemantic_load_yaml_for_context(const char *context, const char *yaml);

/*
 * Load semantic models from a file or directory path.
 *
 * If path is a directory, loads all .yaml/.yml files in it.
 * Returns NULL on success, error message on failure.
 * Caller must free the returned string with sidemantic_free().
 */
char *sidemantic_load_file(const char *path);
char *sidemantic_load_file_for_context(const char *context, const char *path);

/*
 * Clear all loaded semantic models.
 */
void sidemantic_clear(void);
void sidemantic_clear_for_context(const char *context);

/*
 * Define a semantic model from SQL definition format.
 *
 * Parses the definition, saves to file, and loads into current session.
 * If `replace` is true, removes any existing model with the same name from the file.
 *
 * db_path: Path to the database file (NULL for in-memory/session-local).
 *   - If db_path is "foo.duckdb", definitions are saved to "foo.sidemantic.sql"
 *   - If db_path is NULL or ":memory:", definitions are not persisted
 *
 * Returns NULL on success, error message on failure.
 * Caller must free the returned string with sidemantic_free().
 */
char *sidemantic_define(const char *definition_sql, const char *db_path, bool replace);
char *sidemantic_define_for_context(const char *context, const char *definition_sql, const char *db_path, bool replace);

/*
 * Auto-load definitions from file if it exists.
 *
 * Called on extension load to restore previously saved definitions.
 * Looks for the definitions file based on db_path (same logic as sidemantic_define).
 *
 * Returns NULL on success (including when file doesn't exist), error message on failure.
 * Caller must free the returned string with sidemantic_free().
 */
char *sidemantic_autoload(const char *db_path);
char *sidemantic_autoload_for_context(const char *context, const char *db_path);

/*
 * Add a metric/dimension/segment to a model.
 *
 * Supports syntaxes:
 *   - "METRIC (name foo, ...)" - adds to active model
 *   - "METRIC model.foo (...)" - adds to specified model
 *   - "METRIC foo AS SUM(x)" - adds to active model
 *   - "METRIC model.foo AS SUM(x)" - adds to specified model
 *
 * definition_sql: The definition (e.g., "METRIC revenue AS SUM(amount)")
 * db_path: Path to database file for persistence (NULL for in-memory)
 * is_replace: If true, replace existing metric/dimension with same name
 *
 * Returns NULL on success, error message on failure.
 * Caller must free the returned string with sidemantic_free().
 */
char *sidemantic_add_definition(const char *definition_sql, const char *db_path, bool is_replace);
char *sidemantic_add_definition_for_context(const char *context, const char *definition_sql, const char *db_path, bool is_replace);

/*
 * Set the active model for subsequent METRIC/DIMENSION/SEGMENT additions.
 *
 * model_name: Name of an existing model to use as the active model.
 *
 * Returns NULL on success, error message on failure.
 * Caller must free the returned string with sidemantic_free().
 */
char *sidemantic_use(const char *model_name);
char *sidemantic_use_for_context(const char *context, const char *model_name);

/*
 * Check if a table name is a registered semantic model.
 */
bool sidemantic_is_model(const char *table_name);
bool sidemantic_is_model_for_context(const char *context, const char *table_name);

/*
 * Get list of registered model names (comma-separated).
 *
 * Caller must free the returned string with sidemantic_free().
 */
char *sidemantic_list_models(void);
char *sidemantic_list_models_for_context(const char *context);

/*
 * Rewrite a SQL query using semantic definitions.
 *
 * Returns a SidemanticRewriteResult struct.
 * Caller must free with sidemantic_free_result().
 */
SidemanticRewriteResult sidemantic_rewrite(const char *sql);
SidemanticRewriteResult sidemantic_rewrite_for_context(const char *context, const char *sql);

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
