/*
 * Sidemantic C API
 *
 * FFI bindings for the sidemantic semantic layer library.
 */

#ifndef SIDEMANTIC_H
#define SIDEMANTIC_H

#include <stdbool.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Result from rewrite operation */
typedef struct {
    char *sql;          /* Rewritten SQL (NULL if error) */
    char *error;        /* Error message (NULL if success) */
    bool was_rewritten; /* Whether the query was rewritten (false = passthrough) */
} SidemanticRewriteResult;

/* Stateless catalog operations. Snapshot and active_model may be NULL or empty.
 * All other inputs must be NUL-terminated UTF-8 strings. Operations are model,
 * item, use, yaml, file, and legacy_sql. No context registry or sidecar is changed.
 * Publish a successful snapshot in the host transaction; active_model belongs to
 * the calling session. Free every result with sidemantic_free_snapshot_result.
 */
typedef struct {
    char *snapshot;
    char *active_model;
    char *error;
} SidemanticSnapshotResult;

SidemanticSnapshotResult sidemantic_snapshot_apply(const char *snapshot, const char *active_model,
                                                  const char *operation, const char *content, bool replace);
void sidemantic_free_snapshot_result(SidemanticSnapshotResult result);

/* Import host-authorized UTF-8 contents without accessing files or environment.
 * Directory imports retain cross-file inheritance and relationship inference.
 * Inputs are borrowed for the call; free the returned snapshot normally.
 */
typedef struct {
    const char *path;
    const char *content;
} SidemanticSource;

SidemanticSnapshotResult sidemantic_snapshot_load_sources(const char *snapshot, const SidemanticSource *sources,
                                                         size_t count, bool directory);

/* Direct rewrite after the host has identified semantic field references.
 * Returns compiler errors instead of falling back to ordinary SQL.
 */
SidemanticRewriteResult sidemantic_snapshot_rewrite(const char *snapshot, const char *sql);

typedef struct {
    char *name;
    char **fields;
    size_t field_count;
} SidemanticModelInfo;

typedef struct {
    SidemanticModelInfo *models;
    size_t count;
    char *error;
} SidemanticModelList;

/* Model names and semantic fields, sorted and owned by the returned result. */
SidemanticModelList sidemantic_snapshot_list_models(const char *snapshot);
void sidemantic_free_model_list(SidemanticModelList result);

/* Rich catalog metadata. Optional strings are NULL when not declared. The
 * semantic_type describes a metric/dimension; data_type is an authored logical
 * type, not a guess about the physical result. definition is complete JSON.
 */
typedef struct {
    char *kind;
    char *model_name;
    char *name;
    char *qualified_name;
    char *label;
    char *description;
    char *semantic_type;
    char *data_type;
    char *sql;
    char *aggregation;
    char *target_model;
    char *relationship_type;
    char *granularity;
    bool is_public;
    char *definition;
} SidemanticCatalogEntry;

typedef struct {
    SidemanticCatalogEntry *entries;
    size_t count;
    char *error;
} SidemanticCatalogEntries;

/* Empty kind/model selects all. Non-NULL metric is a JSON array of model and
 * metric identifiers and selects compatible dimensions. Results own all memory.
 */
SidemanticCatalogEntries sidemantic_snapshot_catalog(const char *snapshot, const char *kind,
                                                     const char *model, const char *metric);
void sidemantic_free_catalog_entries(SidemanticCatalogEntries result);

/* Export returns a lossless versioned snapshot for the 'import' operation. Free
 * the text result
 * with sidemantic_free_result(); the text is in the result's sql member.
 */
SidemanticRewriteResult sidemantic_snapshot_export(const char *snapshot);

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
 * Stateless, policy-aware SemanticInput v1 entrypoints. All arguments must be
 * non-NULL, NUL-terminated UTF-8 strings. Context is query context JSON (use
 * "{}" for no caller attributes). Neither function changes loaded YAML models.
 * Free every returned result with sidemantic_free_result(). On success sql is
 * non-NULL and was_rewritten is true; on failure error is non-NULL.
 */
SidemanticRewriteResult sidemantic_compile_semantic_input(const char *input_json, const char *query_json);
SidemanticRewriteResult sidemantic_rewrite_semantic_input(const char *input_json, const char *sql, const char *context_json);

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
