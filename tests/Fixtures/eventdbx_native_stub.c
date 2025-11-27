#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct DbxHandle {
    char *config_json;
} DbxHandle;

static char *duplicate_string(const char *value) {
    if (value == NULL) {
        return NULL;
    }
    size_t length = strlen(value);
    char *copy = (char *)malloc(length + 1);
    if (copy != NULL) {
        memcpy(copy, value, length);
        copy[length] = '\0';
    }
    return copy;
}

static char *build_json(const char *format, ...) {
    va_list args;
    va_start(args, format);
    int size = vsnprintf(NULL, 0, format, args);
    va_end(args);

    if (size < 0) {
        return NULL;
    }

    char *buffer = (char *)malloc((size_t)size + 1);
    if (buffer == NULL) {
        return NULL;
    }

    va_start(args, format);
    vsnprintf(buffer, (size_t)size + 1, format, args);
    va_end(args);

    return buffer;
}

static bool has_marker(const char *value, const char *marker) {
    return value != NULL && strcmp(value, marker) == 0;
}

static bool should_return_null(const char *first, const char *second) {
    return has_marker(first, "no-data") || has_marker(second, "no-data");
}

static bool should_return_bad_json(const char *first, const char *second) {
    return has_marker(first, "bad-json") || has_marker(second, "bad-json");
}

static bool should_error(const char *first, const char *second, char **error_out) {
    if (has_marker(first, "native-error") || has_marker(second, "native-error")) {
        *error_out = duplicate_string("native error from stub library");
        return true;
    }
    return false;
}

void dbx_string_free(char *ptr) {
    if (ptr != NULL) {
        free(ptr);
    }
}

DbxHandle *dbx_client_new(const char *config_json, char **error_out) {
    if (config_json != NULL && strstr(config_json, "config-error") != NULL) {
        *error_out = duplicate_string("config failure from stub library");
        return NULL;
    }

    *error_out = NULL;
    DbxHandle *handle = (DbxHandle *)malloc(sizeof(DbxHandle));
    if (handle == NULL) {
        return NULL;
    }
    handle->config_json = duplicate_string(config_json == NULL ? "" : config_json);
    return handle;
}

void dbx_client_free(DbxHandle *handle) {
    if (handle != NULL) {
        free(handle->config_json);
        free(handle);
    }
}

char *dbx_list_aggregates(DbxHandle *handle, const char *aggregate_type, const char *options_json, char **error_out) {
    if (should_error(aggregate_type, NULL, error_out)) {
        return NULL;
    }
    if (should_return_null(aggregate_type, NULL)) {
        *error_out = NULL;
        return NULL;
    }
    if (should_return_bad_json(aggregate_type, NULL)) {
        *error_out = NULL;
        return duplicate_string("{\"function\":\"dbx_list_aggregates\",\"invalid\":}");
    }

    *error_out = NULL;
    const char *options = options_json != NULL ? options_json : "null";
    return build_json("{\"function\":\"dbx_list_aggregates\",\"aggregate_type\":\"%s\",\"options\":%s}", aggregate_type, options);
}

char *dbx_get_aggregate(DbxHandle *handle, const char *aggregate_type, const char *aggregate_id, char **error_out) {
    if (should_error(aggregate_type, aggregate_id, error_out)) {
        return NULL;
    }
    if (should_return_null(aggregate_type, aggregate_id)) {
        *error_out = NULL;
        return NULL;
    }
    if (should_return_bad_json(aggregate_type, aggregate_id)) {
        *error_out = NULL;
        return duplicate_string("{\"function\":\"dbx_get_aggregate\",\"broken\": [}");
    }

    *error_out = NULL;
    return build_json("{\"function\":\"dbx_get_aggregate\",\"aggregate_type\":\"%s\",\"aggregate_id\":\"%s\"}", aggregate_type, aggregate_id);
}

char *dbx_select_aggregate(DbxHandle *handle, const char *aggregate_type, const char *aggregate_id, const char *fields_json, char **error_out) {
    if (should_error(aggregate_type, aggregate_id, error_out)) {
        return NULL;
    }
    if (should_return_null(aggregate_type, aggregate_id)) {
        *error_out = NULL;
        return NULL;
    }
    if (should_return_bad_json(aggregate_type, aggregate_id)) {
        *error_out = NULL;
        return duplicate_string("{\"function\":\"dbx_select_aggregate\",\"broken\": [}");
    }

    *error_out = NULL;
    const char *fields = fields_json != NULL ? fields_json : "null";
    return build_json("{\"function\":\"dbx_select_aggregate\",\"aggregate_type\":\"%s\",\"aggregate_id\":\"%s\",\"fields\":%s}", aggregate_type, aggregate_id, fields);
}

char *dbx_list_events(DbxHandle *handle, const char *aggregate_type, const char *aggregate_id, const char *options_json, char **error_out) {
    if (should_error(aggregate_type, aggregate_id, error_out)) {
        return NULL;
    }
    if (should_return_null(aggregate_type, aggregate_id)) {
        *error_out = NULL;
        return NULL;
    }
    if (should_return_bad_json(aggregate_type, aggregate_id)) {
        *error_out = NULL;
        return duplicate_string("{\"function\":\"dbx_list_events\",\"broken\": [}");
    }

    *error_out = NULL;
    const char *options = options_json != NULL ? options_json : "null";
    return build_json("{\"function\":\"dbx_list_events\",\"aggregate_type\":\"%s\",\"aggregate_id\":\"%s\",\"options\":%s}", aggregate_type, aggregate_id, options);
}

char *dbx_append_event(DbxHandle *handle, const char *aggregate_type, const char *aggregate_id, const char *event_type, const char *options_json, char **error_out) {
    if (should_error(aggregate_type, aggregate_id, error_out)) {
        return NULL;
    }
    if (should_return_null(aggregate_type, aggregate_id)) {
        *error_out = NULL;
        return NULL;
    }
    if (should_return_bad_json(aggregate_type, aggregate_id)) {
        *error_out = NULL;
        return duplicate_string("{\"function\":\"dbx_append_event\",\"broken\": [}");
    }

    *error_out = NULL;
    const char *options = options_json != NULL ? options_json : "null";
    return build_json("{\"function\":\"dbx_append_event\",\"aggregate_type\":\"%s\",\"aggregate_id\":\"%s\",\"event_type\":\"%s\",\"options\":%s}", aggregate_type, aggregate_id, event_type, options);
}

char *dbx_create_aggregate(DbxHandle *handle, const char *aggregate_type, const char *aggregate_id, const char *event_type, const char *options_json, char **error_out) {
    if (should_error(aggregate_type, aggregate_id, error_out)) {
        return NULL;
    }
    if (should_return_null(aggregate_type, aggregate_id)) {
        *error_out = NULL;
        return NULL;
    }
    if (should_return_bad_json(aggregate_type, aggregate_id)) {
        *error_out = NULL;
        return duplicate_string("{\"function\":\"dbx_create_aggregate\",\"broken\": [}");
    }

    *error_out = NULL;
    const char *options = options_json != NULL ? options_json : "null";
    return build_json("{\"function\":\"dbx_create_aggregate\",\"aggregate_type\":\"%s\",\"aggregate_id\":\"%s\",\"event_type\":\"%s\",\"options\":%s}", aggregate_type, aggregate_id, event_type, options);
}

char *dbx_patch_event(DbxHandle *handle, const char *aggregate_type, const char *aggregate_id, const char *event_type, const char *patch_json, const char *options_json, char **error_out) {
    if (should_error(aggregate_type, aggregate_id, error_out)) {
        return NULL;
    }
    if (should_return_null(aggregate_type, aggregate_id)) {
        *error_out = NULL;
        return NULL;
    }
    if (should_return_bad_json(aggregate_type, aggregate_id)) {
        *error_out = NULL;
        return duplicate_string("{\"function\":\"dbx_patch_event\",\"broken\": [}");
    }

    *error_out = NULL;
    const char *patch = patch_json != NULL ? patch_json : "null";
    const char *options = options_json != NULL ? options_json : "null";
    return build_json("{\"function\":\"dbx_patch_event\",\"aggregate_type\":\"%s\",\"aggregate_id\":\"%s\",\"event_type\":\"%s\",\"patch\":%s,\"options\":%s}", aggregate_type, aggregate_id, event_type, patch, options);
}

char *dbx_set_archive(DbxHandle *handle, const char *aggregate_type, const char *aggregate_id, bool archived, const char *options_json, char **error_out) {
    if (should_error(aggregate_type, aggregate_id, error_out)) {
        return NULL;
    }
    if (should_return_null(aggregate_type, aggregate_id)) {
        *error_out = NULL;
        return NULL;
    }
    if (should_return_bad_json(aggregate_type, aggregate_id)) {
        *error_out = NULL;
        return duplicate_string("{\"function\":\"dbx_set_archive\",\"broken\": [}");
    }

    *error_out = NULL;
    const char *options = options_json != NULL ? options_json : "null";
    const char *archived_value = archived ? "true" : "false";
    return build_json("{\"function\":\"dbx_set_archive\",\"aggregate_type\":\"%s\",\"aggregate_id\":\"%s\",\"archived\":%s,\"options\":%s}", aggregate_type, aggregate_id, archived_value, options);
}

char *dbx_verify_aggregate(DbxHandle *handle, const char *aggregate_type, const char *aggregate_id, char **error_out) {
    if (should_error(aggregate_type, aggregate_id, error_out)) {
        return NULL;
    }
    if (should_return_null(aggregate_type, aggregate_id)) {
        *error_out = NULL;
        return NULL;
    }
    if (should_return_bad_json(aggregate_type, aggregate_id)) {
        *error_out = NULL;
        return duplicate_string("{\"function\":\"dbx_verify_aggregate\",\"broken\": [}");
    }

    *error_out = NULL;
    return build_json("{\"function\":\"dbx_verify_aggregate\",\"aggregate_type\":\"%s\",\"aggregate_id\":\"%s\"}", aggregate_type, aggregate_id);
}

char *dbx_create_snapshot(DbxHandle *handle, const char *aggregate_type, const char *aggregate_id, const char *options_json, char **error_out) {
    if (should_error(aggregate_type, aggregate_id, error_out)) {
        return NULL;
    }
    if (should_return_null(aggregate_type, aggregate_id)) {
        *error_out = NULL;
        return NULL;
    }
    if (should_return_bad_json(aggregate_type, aggregate_id)) {
        *error_out = NULL;
        return duplicate_string("{\"function\":\"dbx_create_snapshot\",\"broken\": [}");
    }

    *error_out = NULL;
    const char *options = options_json != NULL ? options_json : "null";
    return build_json("{\"function\":\"dbx_create_snapshot\",\"aggregate_type\":\"%s\",\"aggregate_id\":\"%s\",\"options\":%s}", aggregate_type, aggregate_id, options);
}

char *dbx_list_snapshots(DbxHandle *handle, const char *options_json, char **error_out) {
    if (should_error(options_json, NULL, error_out)) {
        return NULL;
    }
    if (should_return_null(options_json, NULL)) {
        *error_out = NULL;
        return NULL;
    }
    if (should_return_bad_json(options_json, NULL)) {
        *error_out = NULL;
        return duplicate_string("{\"function\":\"dbx_list_snapshots\",\"broken\": [}");
    }

    *error_out = NULL;
    const char *options = options_json != NULL ? options_json : "null";
    return build_json("{\"function\":\"dbx_list_snapshots\",\"options\":%s}", options);
}

char *dbx_get_snapshot(DbxHandle *handle, uint64_t snapshot_id, const char *options_json, char **error_out) {
    (void)handle;
    if (should_error(options_json, NULL, error_out)) {
        return NULL;
    }
    if (should_return_null(options_json, NULL)) {
        *error_out = NULL;
        return NULL;
    }
    if (should_return_bad_json(options_json, NULL)) {
        *error_out = NULL;
        return duplicate_string("{\"function\":\"dbx_get_snapshot\",\"broken\": [}");
    }

    *error_out = NULL;
    const char *options = options_json != NULL ? options_json : "null";
    return build_json("{\"function\":\"dbx_get_snapshot\",\"snapshot_id\":%llu,\"options\":%s}", (unsigned long long)snapshot_id, options);
}
