use std::{
    ffi::{CStr, CString},
    os::raw::c_char,
    time::Duration,
};

use eventdbx_client::{
    AggregateSort, AggregateSortField, AppendEventRequest, ClientConfig, CreateAggregateRequest,
    EventDbxClient, ListAggregatesOptions, ListEventsOptions, PatchEventRequest, PublishTarget,
    SelectAggregateRequest, SetAggregateArchiveRequest,
};
use serde::Deserialize;
use serde_json::{Map, Value};
use tokio::runtime::Runtime;

struct DbxHandle {
    runtime: Runtime,
    client: EventDbxClient,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ConfigInput {
    ip: Option<String>,
    host: Option<String>,
    port: Option<u16>,
    token: Option<String>,
    tenant_id: Option<String>,
    tenant: Option<String>,
    tenant_id_env: Option<String>,
    no_noise: Option<bool>,
    connect_timeout_ms: Option<u64>,
    request_timeout_ms: Option<u64>,
    protocol_version: Option<u16>,
}

fn default_host(cfg: &ConfigInput) -> String {
    cfg.host
        .clone()
        .or_else(|| cfg.ip.clone())
        .or_else(|| std::env::var("EVENTDBX_HOST").ok())
        .unwrap_or_else(|| "127.0.0.1".to_string())
}

fn default_token(cfg: &ConfigInput) -> Option<String> {
    cfg.token
        .clone()
        .or_else(|| std::env::var("EVENTDBX_TOKEN").ok())
}

fn default_tenant(cfg: &ConfigInput) -> String {
    cfg.tenant_id
        .clone()
        .or_else(|| cfg.tenant.clone())
        .or_else(|| cfg.tenant_id_env.clone())
        .or_else(|| std::env::var("EVENTDBX_TENANT_ID").ok())
        .unwrap_or_else(|| "default".to_string())
}

fn set_error(out: *mut *mut c_char, msg: impl Into<String>) {
    if out.is_null() {
        return;
    }
    let cstr = CString::new(msg.into()).unwrap_or_else(|_| CString::new("ffi error").unwrap());
    unsafe {
        *out = cstr.into_raw();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_sort_accepts_known_fields() {
        let sorts = parse_sort(Some(&Value::String(
            "aggregate_type:asc,updated_at:desc,unknown:asc".to_string(),
        )));
        assert_eq!(sorts.len(), 2);
        assert!(matches!(sorts[0].field, AggregateSortField::AggregateType));
        assert!(!sorts[0].descending);
        assert!(matches!(sorts[1].field, AggregateSortField::UpdatedAt));
        assert!(sorts[1].descending);
    }

    #[test]
    fn parse_sort_ignores_empty_and_invalid() {
        let sorts = parse_sort(Some(&Value::String(" , , ".to_string())));
        assert!(sorts.is_empty());
    }

    #[test]
    fn publish_targets_parse_minimal_fields() {
        let value = serde_json::json!([
            { "plugin": "search", "mode": "event-only", "priority": "high" },
            { "plugin": "analytics" }
        ]);
        let targets = parse_publish_targets(Some(&value));
        assert_eq!(targets.len(), 2);
        assert_eq!(targets[0].plugin, "search");
        assert_eq!(targets[0].mode.as_deref(), Some("event-only"));
        assert_eq!(targets[0].priority.as_deref(), Some("high"));
        assert_eq!(targets[1].plugin, "analytics");
        assert!(targets[1].mode.is_none());
        assert!(targets[1].priority.is_none());
    }

    #[test]
    fn payload_options_defaults_to_empty_payload() {
        let (payload, note, metadata, token, publish_targets) =
            parse_payload_options(Value::Object(Map::new()));
        assert_eq!(payload, Value::Null);
        assert!(note.is_none());
        assert!(metadata.is_none());
        assert!(token.is_none());
        assert!(publish_targets.is_empty());
    }

    #[test]
    fn payload_options_reads_fields() {
        let input = serde_json::json!({
            "payload": { "name": "Ada" },
            "metadata": { "@source": "test" },
            "note": "demo",
            "token": "abc",
            "publishTargets": [{ "plugin": "search", "mode": "all" }]
        });
        let (payload, note, metadata, token, publish_targets) = parse_payload_options(input);
        assert_eq!(payload, serde_json::json!({ "name": "Ada" }));
        assert_eq!(metadata, Some(serde_json::json!({ "@source": "test" })));
        assert_eq!(note.as_deref(), Some("demo"));
        assert_eq!(token.as_deref(), Some("abc"));
        assert_eq!(publish_targets.len(), 1);
        assert_eq!(publish_targets[0].plugin, "search");
        assert_eq!(publish_targets[0].mode.as_deref(), Some("all"));
    }
}
fn clear_error(out: *mut *mut c_char) {
    if out.is_null() {
        return;
    }
    unsafe {
        *out = std::ptr::null_mut();
    }
}

fn string_from_ptr(ptr: *const c_char) -> Result<String, String> {
    if ptr.is_null() {
        return Ok(String::new());
    }
    unsafe { CStr::from_ptr(ptr) }
        .to_str()
        .map(|s| s.to_string())
        .map_err(|e| format!("invalid utf-8: {e}"))
}

fn parse_json(ptr: *const c_char) -> Result<Value, String> {
    if ptr.is_null() {
        return Ok(Value::Null);
    }
    let text = string_from_ptr(ptr)?;
    if text.trim().is_empty() {
        return Ok(Value::Null);
    }
    serde_json::from_str(&text).map_err(|e| format!("invalid json: {e}"))
}

fn to_cstring(json: Value) -> Result<*mut c_char, String> {
    serde_json::to_string(&json)
        .map_err(|e| format!("failed to serialize json: {e}"))
        .and_then(|s| CString::new(s).map_err(|e| format!("failed to build c string: {e}")))
        .map(|c| c.into_raw())
}

fn parse_sort(value: Option<&Value>) -> Vec<AggregateSort> {
    let mut sorts = Vec::new();
    let Some(Value::String(text)) = value else {
        return sorts;
    };
    for part in text.split(',').map(|p| p.trim()).filter(|p| !p.is_empty()) {
        let mut iter = part.splitn(2, ':');
        let field_raw = iter.next().unwrap_or_default().trim().to_lowercase();
        let order_raw = iter.next().unwrap_or("asc").trim().to_lowercase();
        let field = match field_raw.as_str() {
            "aggregate_type" | "type" => AggregateSortField::AggregateType,
            "aggregate_id" | "id" => AggregateSortField::AggregateId,
            "archived" => AggregateSortField::Archived,
            "created_at" => AggregateSortField::CreatedAt,
            "updated_at" => AggregateSortField::UpdatedAt,
            _ => continue,
        };
        let descending = matches!(order_raw.as_str(), "desc" | "descending");
        sorts.push(AggregateSort { field, descending });
    }
    sorts
}

fn parse_publish_targets(value: Option<&Value>) -> Vec<PublishTarget> {
    let mut targets = Vec::new();
    if let Some(Value::Array(items)) = value {
        for item in items {
            if let Some(plugin) = item.get("plugin").and_then(Value::as_str) {
                let mut target = PublishTarget::new(plugin.to_string());
                if let Some(mode) = item.get("mode").and_then(Value::as_str) {
                    target.mode = Some(mode.to_string());
                }
                if let Some(priority) = item.get("priority").and_then(Value::as_str) {
                    target.priority = Some(priority.to_string());
                }
                targets.push(target);
            }
        }
    }
    targets
}

#[no_mangle]
pub extern "C" fn dbx_string_free(ptr: *mut c_char) {
    if ptr.is_null() {
        return;
    }
    unsafe {
        let _ = CString::from_raw(ptr);
    }
}

#[no_mangle]
pub extern "C" fn dbx_client_new(
    config_json: *const c_char,
    error_out: *mut *mut c_char,
) -> *mut DbxHandle {
    clear_error(error_out);

    let config_value = match parse_json(config_json) {
        Ok(Value::Object(map)) => map,
        Ok(Value::Null) => Map::new(),
        Ok(_) => {
            set_error(error_out, "config must be a JSON object");
            return std::ptr::null_mut();
        }
        Err(err) => {
            set_error(error_out, err);
            return std::ptr::null_mut();
        }
    };

    let cfg: ConfigInput = match serde_json::from_value(Value::Object(config_value)) {
        Ok(cfg) => cfg,
        Err(err) => {
            set_error(error_out, format!("invalid config: {err}"));
            return std::ptr::null_mut();
        }
    };

    let host = default_host(&cfg);
    let token = match default_token(&cfg) {
        Some(token) => token,
        None => {
            set_error(error_out, "token is required");
            return std::ptr::null_mut();
        }
    };
    let mut client_cfg = ClientConfig::new(host, token);
    if let Some(port) = cfg.port {
        client_cfg = client_cfg.with_port(port);
    }
    if let Some(protocol) = cfg.protocol_version {
        client_cfg = client_cfg.with_protocol_version(protocol);
    }
    if let Some(connect) = cfg.connect_timeout_ms {
        client_cfg = client_cfg.with_connect_timeout(Duration::from_millis(connect));
    }
    client_cfg = client_cfg.with_request_timeout(cfg.request_timeout_ms.map(Duration::from_millis));
    client_cfg = client_cfg.with_tenant(default_tenant(&cfg));
    if let Some(no_noise) = cfg.no_noise {
        client_cfg = client_cfg.with_noise(!no_noise);
    }

    let runtime = match Runtime::new() {
        Ok(rt) => rt,
        Err(err) => {
            set_error(error_out, format!("failed to create runtime: {err}"));
            return std::ptr::null_mut();
        }
    };

    let client = match runtime.block_on(EventDbxClient::connect(client_cfg)) {
        Ok(client) => client,
        Err(err) => {
            set_error(error_out, format!("failed to connect: {err}"));
            return std::ptr::null_mut();
        }
    };

    Box::into_raw(Box::new(DbxHandle { runtime, client }))
}

#[no_mangle]
pub extern "C" fn dbx_client_free(handle: *mut DbxHandle) {
    if handle.is_null() {
        return;
    }
    unsafe {
        drop(Box::from_raw(handle));
    }
}

#[no_mangle]
pub extern "C" fn dbx_list_aggregates(
    handle: *mut DbxHandle,
    aggregate_type: *const c_char,
    options_json: *const c_char,
    error_out: *mut *mut c_char,
) -> *mut c_char {
    clear_error(error_out);
    if handle.is_null() {
        set_error(error_out, "handle is null");
        return std::ptr::null_mut();
    }
    let agg_type = match string_from_ptr(aggregate_type) {
        Ok(s) if s.is_empty() => None,
        Ok(s) => Some(s),
        Err(err) => {
            set_error(error_out, err);
            return std::ptr::null_mut();
        }
    };
    let opts_value = match parse_json(options_json) {
        Ok(v) => v,
        Err(err) => {
            set_error(error_out, err);
            return std::ptr::null_mut();
        }
    };
    let mut opts = ListAggregatesOptions::default();
    if let Some(map) = opts_value.as_object() {
        if let Some(cursor) = map.get("cursor").and_then(Value::as_str) {
            opts.cursor = Some(cursor.to_string());
        }
        if let Some(take) = map.get("take").and_then(Value::as_u64) {
            opts.take = Some(take);
        }
        if let Some(filter) = map.get("filter").and_then(Value::as_str) {
            opts.filter = Some(filter.to_string());
        }
        opts.include_archived = map
            .get("includeArchived")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        opts.archived_only = map
            .get("archivedOnly")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        opts.token = map.get("token").and_then(Value::as_str).map(|s| s.to_string());
        opts.sort = parse_sort(map.get("sort"));
    }
    if let Some(agg_type) = agg_type {
        if opts.filter.is_none() {
            opts.filter = Some(format!("aggregate_type = \"{agg_type}\""));
        }
    }

    let client = unsafe { &mut *handle };
    let response = match client
        .runtime
        .block_on(client.client.list_aggregates(opts))
    {
        Ok(resp) => resp,
        Err(err) => {
            set_error(error_out, err.to_string());
            return std::ptr::null_mut();
        }
    };

    let payload = Value::Object(
        [
            ("items".to_string(), response.aggregates),
            (
                "nextCursor".to_string(),
                response
                    .next_cursor
                    .map(Value::String)
                    .unwrap_or(Value::Null),
            ),
        ]
        .into_iter()
        .collect(),
    );

    match to_cstring(payload) {
        Ok(ptr) => ptr,
        Err(err) => {
            set_error(error_out, err);
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "C" fn dbx_get_aggregate(
    handle: *mut DbxHandle,
    aggregate_type: *const c_char,
    aggregate_id: *const c_char,
    error_out: *mut *mut c_char,
) -> *mut c_char {
    clear_error(error_out);
    if handle.is_null() {
        set_error(error_out, "handle is null");
        return std::ptr::null_mut();
    }
    let agg_type = match string_from_ptr(aggregate_type) {
        Ok(s) => s,
        Err(err) => {
            set_error(error_out, err);
            return std::ptr::null_mut();
        }
    };
    let agg_id = match string_from_ptr(aggregate_id) {
        Ok(s) => s,
        Err(err) => {
            set_error(error_out, err);
            return std::ptr::null_mut();
        }
    };

    let client = unsafe { &mut *handle };
    let response = match client.runtime.block_on(client.client.get_aggregate(&agg_type, &agg_id)) {
        Ok(resp) => resp,
        Err(err) => {
            set_error(error_out, err.to_string());
            return std::ptr::null_mut();
        }
    };

    let payload = Value::Object(
        [
            ("found".to_string(), Value::Bool(response.found)),
            (
                "aggregate".to_string(),
                response.aggregate.unwrap_or(Value::Null),
            ),
        ]
        .into_iter()
        .collect(),
    );

    match to_cstring(payload) {
        Ok(ptr) => ptr,
        Err(err) => {
            set_error(error_out, err);
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "C" fn dbx_select_aggregate(
    handle: *mut DbxHandle,
    aggregate_type: *const c_char,
    aggregate_id: *const c_char,
    fields_json: *const c_char,
    error_out: *mut *mut c_char,
) -> *mut c_char {
    clear_error(error_out);
    if handle.is_null() {
        set_error(error_out, "handle is null");
        return std::ptr::null_mut();
    }
    let agg_type = match string_from_ptr(aggregate_type) {
        Ok(s) => s,
        Err(err) => {
            set_error(error_out, err);
            return std::ptr::null_mut();
        }
    };
    let agg_id = match string_from_ptr(aggregate_id) {
        Ok(s) => s,
        Err(err) => {
            set_error(error_out, err);
            return std::ptr::null_mut();
        }
    };
    let fields_value = match parse_json(fields_json) {
        Ok(v) => v,
        Err(err) => {
            set_error(error_out, err);
            return std::ptr::null_mut();
        }
    };
    let fields: Vec<String> = match fields_value {
        Value::Array(items) => items
            .into_iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect(),
        Value::Null => Vec::new(),
        _ => {
            set_error(error_out, "fields must be an array of strings");
            return std::ptr::null_mut();
        }
    };

    let request = SelectAggregateRequest::new(agg_type, agg_id, fields);
    let client = unsafe { &mut *handle };
    let response = match client
        .runtime
        .block_on(client.client.select_aggregate(request))
    {
        Ok(resp) => resp,
        Err(err) => {
            set_error(error_out, err.to_string());
            return std::ptr::null_mut();
        }
    };

    let payload = Value::Object(
        [
            ("found".to_string(), Value::Bool(response.found)),
            (
                "selection".to_string(),
                response.selection.unwrap_or(Value::Null),
            ),
        ]
        .into_iter()
        .collect(),
    );

    match to_cstring(payload) {
        Ok(ptr) => ptr,
        Err(err) => {
            set_error(error_out, err);
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "C" fn dbx_list_events(
    handle: *mut DbxHandle,
    aggregate_type: *const c_char,
    aggregate_id: *const c_char,
    options_json: *const c_char,
    error_out: *mut *mut c_char,
) -> *mut c_char {
    clear_error(error_out);
    if handle.is_null() {
        set_error(error_out, "handle is null");
        return std::ptr::null_mut();
    }
    let agg_type = match string_from_ptr(aggregate_type) {
        Ok(s) => s,
        Err(err) => {
            set_error(error_out, err);
            return std::ptr::null_mut();
        }
    };
    let agg_id = match string_from_ptr(aggregate_id) {
        Ok(s) => s,
        Err(err) => {
            set_error(error_out, err);
            return std::ptr::null_mut();
        }
    };
    let opts_value = match parse_json(options_json) {
        Ok(v) => v,
        Err(err) => {
            set_error(error_out, err);
            return std::ptr::null_mut();
        }
    };
    let mut opts = ListEventsOptions::default();
    if let Some(map) = opts_value.as_object() {
        if let Some(cursor) = map.get("cursor").and_then(Value::as_str) {
            opts.cursor = Some(cursor.to_string());
        }
        if let Some(take) = map.get("take").and_then(Value::as_u64) {
            opts.take = Some(take);
        }
        if let Some(filter) = map.get("filter").and_then(Value::as_str) {
            opts.filter = Some(filter.to_string());
        }
        opts.token = map.get("token").and_then(Value::as_str).map(|s| s.to_string());
    }

    let client = unsafe { &mut *handle };
    let response = match client
        .runtime
        .block_on(client.client.list_events(&agg_type, &agg_id, opts))
    {
        Ok(resp) => resp,
        Err(err) => {
            set_error(error_out, err.to_string());
            return std::ptr::null_mut();
        }
    };

    let payload = Value::Object(
        [
            ("items".to_string(), response.events),
            (
                "nextCursor".to_string(),
                response
                    .next_cursor
                    .map(Value::String)
                    .unwrap_or(Value::Null),
            ),
        ]
        .into_iter()
        .collect(),
    );
    match to_cstring(payload) {
        Ok(ptr) => ptr,
        Err(err) => {
            set_error(error_out, err);
            std::ptr::null_mut()
        }
    }
}

fn parse_payload_options(
    opts_value: Value,
) -> (Value, Option<String>, Option<Value>, Option<String>, Vec<PublishTarget>) {
    let mut payload = Value::Null;
    let mut metadata = None;
    let mut note = None;
    let mut token = None;
    let mut publish_targets = Vec::new();

    if let Some(map) = opts_value.as_object() {
        if let Some(p) = map.get("payload") {
            payload = p.clone();
        }
        metadata = map.get("metadata").cloned();
        note = map.get("note").and_then(Value::as_str).map(|s| s.to_string());
        token = map.get("token").and_then(Value::as_str).map(|s| s.to_string());
        publish_targets = parse_publish_targets(map.get("publishTargets"));
    }

    (payload, note, metadata, token, publish_targets)
}

#[no_mangle]
pub extern "C" fn dbx_append_event(
    handle: *mut DbxHandle,
    aggregate_type: *const c_char,
    aggregate_id: *const c_char,
    event_type: *const c_char,
    options_json: *const c_char,
    error_out: *mut *mut c_char,
) -> *mut c_char {
    clear_error(error_out);
    if handle.is_null() {
        set_error(error_out, "handle is null");
        return std::ptr::null_mut();
    }
    let agg_type = match string_from_ptr(aggregate_type) {
        Ok(s) => s,
        Err(err) => {
            set_error(error_out, err);
            return std::ptr::null_mut();
        }
    };
    let agg_id = match string_from_ptr(aggregate_id) {
        Ok(s) => s,
        Err(err) => {
            set_error(error_out, err);
            return std::ptr::null_mut();
        }
    };
    let evt_type = match string_from_ptr(event_type) {
        Ok(s) => s,
        Err(err) => {
            set_error(error_out, err);
            return std::ptr::null_mut();
        }
    };
    let opts_value = match parse_json(options_json) {
        Ok(v) => v,
        Err(err) => {
            set_error(error_out, err);
            return std::ptr::null_mut();
        }
    };

    let (payload, note, metadata, token, publish_targets) = parse_payload_options(opts_value);
    let payload = match payload {
        Value::Null => Value::Object(Map::new()),
        other => other,
    };

    let mut request = AppendEventRequest::new(agg_type, agg_id, evt_type, payload);
    request.note = note;
    request.metadata = metadata;
    request.token = token;
    request.publish_targets = publish_targets;

    let client = unsafe { &mut *handle };
    let response = match client.runtime.block_on(client.client.append_event(request)) {
        Ok(resp) => resp,
        Err(err) => {
            set_error(error_out, err.to_string());
            return std::ptr::null_mut();
        }
    };

    let payload = Value::Object(
        [("event".to_string(), response.event)]
            .into_iter()
            .collect(),
    );
    match to_cstring(payload) {
        Ok(ptr) => ptr,
        Err(err) => {
            set_error(error_out, err);
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "C" fn dbx_create_aggregate(
    handle: *mut DbxHandle,
    aggregate_type: *const c_char,
    aggregate_id: *const c_char,
    event_type: *const c_char,
    options_json: *const c_char,
    error_out: *mut *mut c_char,
) -> *mut c_char {
    clear_error(error_out);
    if handle.is_null() {
        set_error(error_out, "handle is null");
        return std::ptr::null_mut();
    }
    let agg_type = match string_from_ptr(aggregate_type) {
        Ok(s) => s,
        Err(err) => {
            set_error(error_out, err);
            return std::ptr::null_mut();
        }
    };
    let agg_id = match string_from_ptr(aggregate_id) {
        Ok(s) => s,
        Err(err) => {
            set_error(error_out, err);
            return std::ptr::null_mut();
        }
    };
    let evt_type = match string_from_ptr(event_type) {
        Ok(s) => s,
        Err(err) => {
            set_error(error_out, err);
            return std::ptr::null_mut();
        }
    };
    let opts_value = match parse_json(options_json) {
        Ok(v) => v,
        Err(err) => {
            set_error(error_out, err);
            return std::ptr::null_mut();
        }
    };

    let (payload, note, metadata, token, publish_targets) = parse_payload_options(opts_value);
    let payload = match payload {
        Value::Null => Value::Object(Map::new()),
        other => other,
    };

    let mut request = CreateAggregateRequest::new(agg_type, agg_id, evt_type, payload);
    request.note = note;
    request.metadata = metadata;
    request.token = token;
    request.publish_targets = publish_targets;

    let client = unsafe { &mut *handle };
    let response = match client.runtime.block_on(client.client.create_aggregate(request)) {
        Ok(resp) => resp,
        Err(err) => {
            set_error(error_out, err.to_string());
            return std::ptr::null_mut();
        }
    };

    let payload = Value::Object(
        [("aggregate".to_string(), response.aggregate)]
            .into_iter()
            .collect(),
    );
    match to_cstring(payload) {
        Ok(ptr) => ptr,
        Err(err) => {
            set_error(error_out, err);
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "C" fn dbx_patch_event(
    handle: *mut DbxHandle,
    aggregate_type: *const c_char,
    aggregate_id: *const c_char,
    event_type: *const c_char,
    patch_json: *const c_char,
    options_json: *const c_char,
    error_out: *mut *mut c_char,
) -> *mut c_char {
    clear_error(error_out);
    if handle.is_null() {
        set_error(error_out, "handle is null");
        return std::ptr::null_mut();
    }
    let agg_type = match string_from_ptr(aggregate_type) {
        Ok(s) => s,
        Err(err) => {
            set_error(error_out, err);
            return std::ptr::null_mut();
        }
    };
    let agg_id = match string_from_ptr(aggregate_id) {
        Ok(s) => s,
        Err(err) => {
            set_error(error_out, err);
            return std::ptr::null_mut();
        }
    };
    let evt_type = match string_from_ptr(event_type) {
        Ok(s) => s,
        Err(err) => {
            set_error(error_out, err);
            return std::ptr::null_mut();
        }
    };

    let patch_value = match parse_json(patch_json) {
        Ok(v) => v,
        Err(err) => {
            set_error(error_out, err);
            return std::ptr::null_mut();
        }
    };

    let opts_value = match parse_json(options_json) {
        Ok(v) => v,
        Err(err) => {
            set_error(error_out, err);
            return std::ptr::null_mut();
        }
    };
    let (_, note, metadata, token, publish_targets) = parse_payload_options(opts_value);

    let mut request = PatchEventRequest::new(agg_type, agg_id, evt_type, patch_value);
    request.note = note;
    request.metadata = metadata;
    request.token = token;
    request.publish_targets = publish_targets;

    let client = unsafe { &mut *handle };
    let response = match client.runtime.block_on(client.client.patch_event(request)) {
        Ok(resp) => resp,
        Err(err) => {
            set_error(error_out, err.to_string());
            return std::ptr::null_mut();
        }
    };

    let payload = Value::Object(
        [("event".to_string(), response.event)]
            .into_iter()
            .collect(),
    );
    match to_cstring(payload) {
        Ok(ptr) => ptr,
        Err(err) => {
            set_error(error_out, err);
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "C" fn dbx_set_archive(
    handle: *mut DbxHandle,
    aggregate_type: *const c_char,
    aggregate_id: *const c_char,
    archived: bool,
    options_json: *const c_char,
    error_out: *mut *mut c_char,
) -> *mut c_char {
    clear_error(error_out);
    if handle.is_null() {
        set_error(error_out, "handle is null");
        return std::ptr::null_mut();
    }
    let agg_type = match string_from_ptr(aggregate_type) {
        Ok(s) => s,
        Err(err) => {
            set_error(error_out, err);
            return std::ptr::null_mut();
        }
    };
    let agg_id = match string_from_ptr(aggregate_id) {
        Ok(s) => s,
        Err(err) => {
            set_error(error_out, err);
            return std::ptr::null_mut();
        }
    };

    let opts_value = match parse_json(options_json) {
        Ok(v) => v,
        Err(err) => {
            set_error(error_out, err);
            return std::ptr::null_mut();
        }
    };
    let mut request = SetAggregateArchiveRequest::new(agg_type, agg_id, archived);
    if let Some(map) = opts_value.as_object() {
        request.note = map.get("note").and_then(Value::as_str).map(|s| s.to_string());
        request.token = map.get("token").and_then(Value::as_str).map(|s| s.to_string());
    }

    let client = unsafe { &mut *handle };
    let response = match client
        .runtime
        .block_on(client.client.set_aggregate_archive(request))
    {
        Ok(resp) => resp,
        Err(err) => {
            set_error(error_out, err.to_string());
            return std::ptr::null_mut();
        }
    };

    let payload = Value::Object(
        [("aggregate".to_string(), response.aggregate)]
            .into_iter()
            .collect(),
    );
    match to_cstring(payload) {
        Ok(ptr) => ptr,
        Err(err) => {
            set_error(error_out, err);
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "C" fn dbx_verify_aggregate(
    handle: *mut DbxHandle,
    aggregate_type: *const c_char,
    aggregate_id: *const c_char,
    error_out: *mut *mut c_char,
) -> *mut c_char {
    clear_error(error_out);
    if handle.is_null() {
        set_error(error_out, "handle is null");
        return std::ptr::null_mut();
    }
    let agg_type = match string_from_ptr(aggregate_type) {
        Ok(s) => s,
        Err(err) => {
            set_error(error_out, err);
            return std::ptr::null_mut();
        }
    };
    let agg_id = match string_from_ptr(aggregate_id) {
        Ok(s) => s,
        Err(err) => {
            set_error(error_out, err);
            return std::ptr::null_mut();
        }
    };

    let client = unsafe { &mut *handle };
    let response = match client
        .runtime
        .block_on(client.client.verify_aggregate(&agg_type, &agg_id))
    {
        Ok(resp) => resp,
        Err(err) => {
            set_error(error_out, err.to_string());
            return std::ptr::null_mut();
        }
    };

    let payload = Value::Object(
        [("merkleRoot".to_string(), Value::String(response.merkle_root))]
            .into_iter()
            .collect(),
    );

    match to_cstring(payload) {
        Ok(ptr) => ptr,
        Err(err) => {
            set_error(error_out, err);
            std::ptr::null_mut()
        }
    }
}
