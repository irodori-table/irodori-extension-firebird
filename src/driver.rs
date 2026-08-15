use irodori_connector_abi::{option_string, push_sensitive, request_containers};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use firebird_wire::{ConnectConfig, Connection as FbConn, Value as FbValue, WireCrypt};
use serde_json::{json, Map, Value};

use crate::abi::{self, IrodoriConnectorBuffer};
use crate::{ABI_VERSION, CONFIG_JSON, DRIVER_LINKED, ENGINE, MANIFEST_JSON};

static CONNECTIONS: OnceLock<Mutex<HashMap<String, FirebirdConnection>>> = OnceLock::new();

#[derive(Clone)]
struct FirebirdConnection {
    conn: Arc<Mutex<Option<FbConn>>>,
    config: FirebirdConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FirebirdConfig {
    target: String,
    user: String,
    password: String,
    role: Option<String>,
    charset: String,
    wire_crypt: WireCrypt,
    redaction_values: Vec<String>,
}

#[derive(Default)]
struct ObjectMeta {
    kind: String,
    columns: Vec<Value>,
    indexes: Vec<Value>,
    primary_key: Vec<String>,
    foreign_keys: Vec<Value>,
}

type QueryRows = Vec<Vec<Value>>;
type QueryOutput = (Vec<String>, QueryRows, bool);

fn connections() -> &'static Mutex<HashMap<String, FirebirdConnection>> {
    CONNECTIONS.get_or_init(|| Mutex::new(HashMap::new()))
}

pub fn call_json(request: IrodoriConnectorBuffer) -> IrodoriConnectorBuffer {
    let request = match abi::parse_request(request) {
        Ok(request) => request,
        Err(response) => return response,
    };
    let method = match abi::request_method(request.as_ref()) {
        Ok(method) => method,
        Err(response) => return response,
    };

    match method {
        "health" | "ping" => abi::ok(Map::from_iter([
            ("engine".to_string(), Value::String(ENGINE.to_string())),
            ("abiVersion".to_string(), json!(ABI_VERSION)),
            ("driverLinked".to_string(), Value::Bool(DRIVER_LINKED)),
        ])),
        "describe" | "capabilities" => abi::ok(Map::from_iter([
            ("engine".to_string(), Value::String(ENGINE.to_string())),
            ("abiVersion".to_string(), json!(ABI_VERSION)),
            ("driverLinked".to_string(), Value::Bool(DRIVER_LINKED)),
            (
                "manifest".to_string(),
                serde_json::from_str(MANIFEST_JSON).unwrap_or(Value::Null),
            ),
            (
                "config".to_string(),
                serde_json::from_str(CONFIG_JSON).unwrap_or(Value::Null),
            ),
        ])),
        "manifest" => abi::owned_buffer(MANIFEST_JSON.to_string()),
        "config" => abi::owned_buffer(CONFIG_JSON.to_string()),
        "connect" => connect(request.as_ref().expect("connect has request")),
        "query" => query(request.as_ref().expect("query has request")),
        "metadata" => metadata(request.as_ref().expect("metadata has request")),
        "close" => close(request.as_ref().expect("close has request")),
        other => abi::error(
            "connector.unknownMethod",
            format!("unknown connector method: {other}"),
        ),
    }
}

fn connect(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = abi::connection_id(Some(request));
    let (driver_config, connector_config) = match FirebirdConfig::from_request(request) {
        Ok(config) => config,
        Err(err) => return abi::error("connector.invalidRequest", err),
    };
    let conn = match FbConn::connect(&driver_config) {
        Ok(conn) => conn,
        Err(err) => {
            return abi::error(
                "connector.connectFailed",
                connector_config.redact(&err.to_string()),
            )
        }
    };
    let connection = FirebirdConnection {
        conn: Arc::new(Mutex::new(Some(conn))),
        config: connector_config,
    };
    let server_version = run_scalar(
        &connection,
        "select rdb$get_context('SYSTEM', 'ENGINE_VERSION') from rdb$database",
    )
    .unwrap_or_else(|_| "Firebird".to_string());
    let mut guard = match connections().lock() {
        Ok(guard) => guard,
        Err(_) => {
            return abi::error(
                "connector.statePoisoned",
                "Connector connection state is poisoned.",
            )
        }
    };
    let response = Map::from_iter([
        ("engine".to_string(), Value::String(ENGINE.to_string())),
        (
            "connectionId".to_string(),
            Value::String(connection_id.clone()),
        ),
        ("driverLinked".to_string(), Value::Bool(DRIVER_LINKED)),
        (
            "database".to_string(),
            Value::String(connection.config.target.clone()),
        ),
        (
            "user".to_string(),
            Value::String(connection.config.user.clone()),
        ),
        ("serverVersion".to_string(), Value::String(server_version)),
    ]);
    guard.insert(connection_id, connection);
    abi::ok(response)
}

fn query(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = abi::connection_id(Some(request));
    let Some(sql) = abi::string_field(request, "sql")
        .or_else(|| abi::string_field(request, "query"))
        .or_else(|| abi::string_field(request, "statement"))
    else {
        return abi::error(
            "connector.invalidRequest",
            "query requires a string sql, query, or statement field.",
        );
    };
    let connection = match connection(&connection_id) {
        Ok(connection) => connection,
        Err(response) => return response,
    };
    match run_query(&connection, sql, abi::max_rows(request)) {
        Ok((columns, rows, truncated)) => abi::ok(Map::from_iter([
            ("connectionId".to_string(), Value::String(connection_id)),
            (
                "columns".to_string(),
                Value::Array(columns.into_iter().map(Value::String).collect()),
            ),
            (
                "rows".to_string(),
                Value::Array(rows.into_iter().map(Value::Array).collect()),
            ),
            ("truncated".to_string(), Value::Bool(truncated)),
        ])),
        Err(err) => abi::error("connector.queryFailed", connection.config.redact(&err)),
    }
}

fn metadata(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = abi::connection_id(Some(request));
    let connection = match connection(&connection_id) {
        Ok(connection) => connection,
        Err(response) => return response,
    };
    match load_metadata(&connection) {
        Ok(metadata) => abi::ok(Map::from_iter([
            ("connectionId".to_string(), Value::String(connection_id)),
            ("metadata".to_string(), metadata),
        ])),
        Err(err) => abi::error("connector.metadataFailed", connection.config.redact(&err)),
    }
}

fn close(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = abi::connection_id(Some(request));
    let connection = match connections().lock() {
        Ok(mut guard) => guard.remove(&connection_id),
        Err(_) => {
            return abi::error(
                "connector.statePoisoned",
                "Connector connection state is poisoned.",
            )
        }
    };
    if let Some(connection) = connection.as_ref() {
        if let Ok(mut guard) = connection.conn.lock() {
            if let Some(conn) = guard.take() {
                let _ = conn.close();
            }
        }
    }
    abi::ok(Map::from_iter([
        ("connectionId".to_string(), Value::String(connection_id)),
        ("closed".to_string(), Value::Bool(connection.is_some())),
    ]))
}

impl FirebirdConfig {
    fn from_request(request: &Value) -> Result<(ConnectConfig, Self), String> {
        let url = option_string(request, &["url", "connectionString", "dsn"]);
        let parsed = url.as_deref().and_then(parse_firebird_url);
        let host = option_string(request, &["host"])
            .or_else(|| parsed.as_ref().map(|parsed| parsed.host.clone()))
            .unwrap_or_else(|| "localhost".to_string());
        let port = option_u16(request, &["port"])
            .or_else(|| parsed.as_ref().and_then(|parsed| parsed.port))
            .unwrap_or(3050);
        let database = option_string(request, &["database", "db"])
            .or_else(|| parsed.as_ref().map(|parsed| parsed.database.clone()))
            .ok_or_else(|| "Firebird requires database.".to_string())?;
        let user = option_string(request, &["user", "username"])
            .or_else(|| parsed.as_ref().and_then(|parsed| parsed.user.clone()))
            .unwrap_or_else(|| "SYSDBA".to_string());
        let password = option_string(request, &["password"])
            .or_else(|| parsed.as_ref().and_then(|parsed| parsed.password.clone()))
            .unwrap_or_default();
        let role = option_string(request, &["role"]);
        let charset = option_string(request, &["charset"]).unwrap_or_else(|| "UTF8".to_string());
        let wire_crypt = match option_string(request, &["wireCrypt", "wire_crypt"])
            .unwrap_or_else(|| "enabled".to_string())
            .to_ascii_lowercase()
            .as_str()
        {
            "disabled" | "false" | "off" => WireCrypt::Disabled,
            "required" | "require" | "true" => WireCrypt::Required,
            _ => WireCrypt::Enabled,
        };
        let mut driver_config = ConnectConfig::new()
            .host(host.clone())
            .port(port)
            .database(database.clone())
            .user(user.clone())
            .password(password.clone())
            .charset(charset.clone())
            .wire_crypt(wire_crypt)
            .connect_timeout(Duration::from_secs(
                option_u16(request, &["connectTimeoutSeconds"]).unwrap_or(15) as u64,
            ));
        if let Some(role) = role.clone() {
            driver_config = driver_config.role(role);
        }
        let mut redaction_values = Vec::new();
        push_sensitive(&mut redaction_values, Some(&password));
        Ok((
            driver_config,
            Self {
                target: format!("{host}:{port}/{database}"),
                user,
                password,
                role,
                charset,
                wire_crypt,
                redaction_values,
            },
        ))
    }

    fn redact(&self, message: &str) -> String {
        self.redaction_values
            .iter()
            .fold(message.to_string(), |message, secret| {
                if secret.is_empty() {
                    message
                } else {
                    message.replace(secret, "****")
                }
            })
    }
}

fn run_scalar(connection: &FirebirdConnection, sql: &str) -> Result<String, String> {
    let (_, rows, _) = run_query(connection, sql, 1)?;
    Ok(rows
        .first()
        .and_then(|row| row.first())
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string())
}

fn run_query(
    connection: &FirebirdConnection,
    sql: &str,
    cap: usize,
) -> Result<QueryOutput, String> {
    let mut guard = connection
        .conn
        .lock()
        .map_err(|_| "Firebird connection state is poisoned.".to_string())?;
    let conn = guard
        .as_mut()
        .ok_or_else(|| "Firebird connection is closed.".to_string())?;
    let tx = conn
        .begin()
        .map_err(|err| format!("Firebird transaction begin failed: {err}"))?;
    let result = (|| {
        let mut stmt = conn
            .prepare(&tx, sql)
            .map_err(|err| format!("Firebird prepare failed: {err}"))?;
        stmt.set_fetch_size(cap.clamp(1, 10_000) as i32);
        stmt.execute(conn, &tx, &[])
            .map_err(|err| format!("Firebird execute failed: {err}"))?;
        let columns = stmt
            .columns()
            .iter()
            .map(|column| column.name().to_string())
            .collect::<Vec<_>>();
        let mut rows = Vec::new();
        while rows.len() < cap {
            let Some(row) = stmt
                .fetch(conn)
                .map_err(|err| format!("Firebird fetch failed: {err}"))?
            else {
                break;
            };
            rows.push(row.into_iter().map(firebird_value_to_json).collect());
        }
        let truncated = if rows.len() == cap {
            stmt.fetch(conn)
                .map_err(|err| format!("Firebird fetch failed: {err}"))?
                .is_some()
        } else {
            false
        };
        stmt.drop_statement(conn)
            .map_err(|err| format!("Firebird statement close failed: {err}"))?;
        Ok((columns, rows, truncated))
    })();
    match result {
        Ok(output) => {
            tx.commit(conn)
                .map_err(|err| format!("Firebird commit failed: {err}"))?;
            Ok(output)
        }
        Err(err) => {
            let _ = tx.rollback(conn);
            Err(err)
        }
    }
}

fn load_metadata(connection: &FirebirdConnection) -> Result<Value, String> {
    let (_, object_rows, _) = run_query(
        connection,
        r#"
        select trim(r.rdb$relation_name) as relation_name,
               case when r.rdb$view_blr is null then 'table' else 'view' end as object_type
        from rdb$relations r
        where coalesce(r.rdb$system_flag, 0) = 0
        order by 1
        "#,
        100_000,
    )?;
    let mut objects = BTreeMap::<String, ObjectMeta>::new();
    for row in object_rows {
        let name = json_string(row.first()).unwrap_or_default();
        if name.is_empty() {
            continue;
        }
        objects.entry(name).or_insert_with(|| ObjectMeta {
            kind: json_string(row.get(1)).unwrap_or_else(|| "table".to_string()),
            ..Default::default()
        });
    }

    let (_, column_rows, _) = run_query(
        connection,
        r#"
        select trim(rf.rdb$relation_name) as relation_name,
               trim(rf.rdb$field_name) as field_name,
               f.rdb$field_type,
               f.rdb$field_sub_type,
               f.rdb$field_length,
               f.rdb$field_scale,
               rf.rdb$null_flag,
               rf.rdb$field_position + 1
        from rdb$relation_fields rf
        join rdb$fields f on f.rdb$field_name = rf.rdb$field_source
        join rdb$relations r on r.rdb$relation_name = rf.rdb$relation_name
        where coalesce(r.rdb$system_flag, 0) = 0
        order by rf.rdb$relation_name, rf.rdb$field_position
        "#,
        100_000,
    )?;
    for row in column_rows {
        let table = json_string(row.first()).unwrap_or_default();
        let Some(object) = objects.get_mut(&table) else {
            continue;
        };
        object.columns.push(json!({
            "name": json_string(row.get(1)).unwrap_or_default(),
            "dataType": firebird_type_name(
                json_i64(row.get(2)).unwrap_or_default(),
                json_i64(row.get(3)).unwrap_or_default(),
                json_i64(row.get(5)).unwrap_or_default()
            ),
            "length": json_i64(row.get(4)),
            "scale": json_i64(row.get(5)),
            "nullable": row.get(6).is_none_or(Value::is_null),
            "ordinal": json_i64(row.get(7)).unwrap_or((object.columns.len() + 1) as i64)
        }));
    }

    let (_, index_rows, _) = run_query(
        connection,
        r#"
        select trim(i.rdb$relation_name),
               trim(i.rdb$index_name),
               i.rdb$unique_flag,
               trim(s.rdb$field_name)
        from rdb$indices i
        join rdb$index_segments s on s.rdb$index_name = i.rdb$index_name
        where coalesce(i.rdb$system_flag, 0) = 0
        order by i.rdb$relation_name, i.rdb$index_name, s.rdb$field_position
        "#,
        100_000,
    )?;
    let mut index_map = BTreeMap::<(String, String), usize>::new();
    for row in index_rows {
        let table = json_string(row.first()).unwrap_or_default();
        let name = json_string(row.get(1)).unwrap_or_default();
        let Some(object) = objects.get_mut(&table) else {
            continue;
        };
        let index = *index_map.entry((table, name.clone())).or_insert_with(|| {
            object.indexes.push(json!({
                "name": name,
                "columns": [],
                "unique": json_i64(row.get(2)).unwrap_or_default() == 1
            }));
            object.indexes.len() - 1
        });
        if let Some(index) = object.indexes.get_mut(index).and_then(Value::as_object_mut) {
            push_json_string(
                index,
                "columns",
                json_string(row.get(3)).unwrap_or_default(),
            );
        }
    }

    let (_, pk_rows, _) = run_query(
        connection,
        r#"
        select trim(rc.rdb$relation_name), trim(s.rdb$field_name)
        from rdb$relation_constraints rc
        join rdb$index_segments s on s.rdb$index_name = rc.rdb$index_name
        where rc.rdb$constraint_type = 'PRIMARY KEY'
        order by rc.rdb$relation_name, s.rdb$field_position
        "#,
        100_000,
    )?;
    for row in pk_rows {
        let table = json_string(row.first()).unwrap_or_default();
        let column = json_string(row.get(1)).unwrap_or_default();
        if let Some(object) = objects.get_mut(&table) {
            object.primary_key.push(column);
        }
    }

    let (_, fk_rows, _) = run_query(
        connection,
        r#"
        select trim(rc.rdb$relation_name),
               trim(rc.rdb$constraint_name),
               trim(seg.rdb$field_name),
               trim(refc.rdb$relation_name),
               trim(refseg.rdb$field_name)
        from rdb$relation_constraints rc
        join rdb$ref_constraints rfc on rfc.rdb$constraint_name = rc.rdb$constraint_name
        join rdb$relation_constraints refc on refc.rdb$constraint_name = rfc.rdb$const_name_uq
        join rdb$index_segments seg on seg.rdb$index_name = rc.rdb$index_name
        join rdb$index_segments refseg on refseg.rdb$index_name = refc.rdb$index_name
             and refseg.rdb$field_position = seg.rdb$field_position
        where rc.rdb$constraint_type = 'FOREIGN KEY'
        order by rc.rdb$relation_name, rc.rdb$constraint_name, seg.rdb$field_position
        "#,
        100_000,
    )?;
    let mut fk_map = BTreeMap::<(String, String), usize>::new();
    for row in fk_rows {
        let table = json_string(row.first()).unwrap_or_default();
        let name = json_string(row.get(1)).unwrap_or_default();
        let Some(object) = objects.get_mut(&table) else {
            continue;
        };
        let index = *fk_map.entry((table, name.clone())).or_insert_with(|| {
            object.foreign_keys.push(json!({
                "name": name,
                "columns": [],
                "referencesSchema": null,
                "referencesTable": json_string(row.get(3)).unwrap_or_default(),
                "referencesColumns": []
            }));
            object.foreign_keys.len() - 1
        });
        if let Some(foreign_key) = object
            .foreign_keys
            .get_mut(index)
            .and_then(Value::as_object_mut)
        {
            push_json_string(
                foreign_key,
                "columns",
                json_string(row.get(2)).unwrap_or_default(),
            );
            push_json_string(
                foreign_key,
                "referencesColumns",
                json_string(row.get(4)).unwrap_or_default(),
            );
        }
    }

    Ok(json!({
        "schemas": [{
            "name": "PUBLIC",
            "objects": objects
                .into_iter()
                .map(|(name, object)| json!({
                    "schema": "PUBLIC",
                    "name": name,
                    "kind": object.kind,
                    "columns": object.columns,
                    "indexes": object.indexes,
                    "primaryKey": object.primary_key,
                    "foreignKeys": object.foreign_keys
                }))
                .collect::<Vec<_>>()
        }]
    }))
}

fn firebird_value_to_json(value: FbValue) -> Value {
    match value {
        FbValue::Null => Value::Null,
        FbValue::Bool(value) => Value::Bool(value),
        FbValue::Short(value) => json!(value),
        FbValue::Int(value) => json!(value),
        FbValue::BigInt(value) => json!(value),
        FbValue::Float(value) => json!(value),
        FbValue::Double(value) => json!(value),
        FbValue::Text(value) => Value::String(value),
        FbValue::Bytes(value) => Value::String(format!("0x{}", hex_encode(&value))),
        FbValue::Blob(value) => json!({"blobId": value.to_string()}),
        FbValue::Array(value) => json!({"arrayId": value.to_string()}),
        FbValue::Date(_) => value
            .as_civil_date()
            .map(|date| format!("{:04}-{:02}-{:02}", date.year, date.month, date.day))
            .map(Value::String)
            .unwrap_or_else(|| Value::String(format!("{value:?}"))),
        FbValue::Time(_) => value
            .as_civil_time()
            .map(|time| {
                format!(
                    "{:02}:{:02}:{:02}.{:04}",
                    time.hour, time.minute, time.second, time.frac
                )
            })
            .map(Value::String)
            .unwrap_or_else(|| Value::String(format!("{value:?}"))),
        FbValue::Timestamp(_, _) => value
            .as_civil_timestamp()
            .map(|timestamp| {
                format!(
                    "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:04}",
                    timestamp.date.year,
                    timestamp.date.month,
                    timestamp.date.day,
                    timestamp.time.hour,
                    timestamp.time.minute,
                    timestamp.time.second,
                    timestamp.time.frac
                )
            })
            .map(Value::String)
            .unwrap_or_else(|| Value::String(format!("{value:?}"))),
        FbValue::Int128(value) => Value::String(value.to_string()),
        FbValue::DecFloat(value) => Value::String(format!("{value:?}")),
        FbValue::TimeTz(value) => Value::String(format!("{value:?}")),
        FbValue::TimestampTz(value) => Value::String(format!("{value:?}")),
    }
}

fn firebird_type_name(field_type: i64, sub_type: i64, scale: i64) -> String {
    let base = match field_type {
        7 => "SMALLINT",
        8 => "INTEGER",
        10 => "FLOAT",
        12 => "DATE",
        13 => "TIME",
        14 => "CHAR",
        16 if sub_type == 1 || scale < 0 => "NUMERIC",
        16 if sub_type == 2 => "DECIMAL",
        16 => "BIGINT",
        23 => "BOOLEAN",
        24 => "DECFLOAT(16)",
        25 => "DECFLOAT(34)",
        26 => "INT128",
        27 => "DOUBLE PRECISION",
        28 => "TIME WITH TIME ZONE",
        29 => "TIMESTAMP WITH TIME ZONE",
        35 => "TIMESTAMP",
        37 => "VARCHAR",
        40 => "CSTRING",
        261 => "BLOB",
        _ => "UNKNOWN",
    };
    if scale < 0 && matches!(field_type, 7 | 8 | 16 | 26) {
        format!("{base} scale {scale}")
    } else {
        base.to_string()
    }
}

#[derive(Debug, Clone)]
struct ParsedFirebirdUrl {
    host: String,
    port: Option<u16>,
    database: String,
    user: Option<String>,
    password: Option<String>,
}

fn parse_firebird_url(value: &str) -> Option<ParsedFirebirdUrl> {
    let rest = value
        .strip_prefix("firebird://")
        .or_else(|| value.strip_prefix("fb://"))?;
    let (authority, path) = rest.split_once('/').unwrap_or((rest, ""));
    let (userinfo, hostport) = authority
        .rsplit_once('@')
        .map(|(userinfo, hostport)| (Some(userinfo), hostport))
        .unwrap_or((None, authority));
    let (host, port) = hostport
        .rsplit_once(':')
        .map(|(host, port)| (host.to_string(), port.parse::<u16>().ok()))
        .unwrap_or((hostport.to_string(), None));
    let (user, password) = userinfo
        .map(|userinfo| {
            userinfo
                .split_once(':')
                .map(|(user, password)| {
                    (Some(percent_decode(user)), Some(percent_decode(password)))
                })
                .unwrap_or((Some(percent_decode(userinfo)), None))
        })
        .unwrap_or((None, None));
    Some(ParsedFirebirdUrl {
        host,
        port,
        database: percent_decode(path.trim_start_matches('/')),
        user,
        password,
    })
}

fn percent_decode(value: &str) -> String {
    let mut out = String::new();
    let mut chars = value.chars();
    while let Some(ch) = chars.next() {
        if ch == '%' {
            let h1 = chars.next().unwrap_or('0');
            let h2 = chars.next().unwrap_or('0');
            if let Ok(byte) = u8::from_str_radix(&format!("{h1}{h2}"), 16) {
                out.push(byte as char);
            }
        } else if ch == '+' {
            out.push(' ');
        } else {
            out.push(ch);
        }
    }
    out
}

fn json_string(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::Null => None,
        Value::String(value) => Some(value.trim().to_string()),
        Value::Number(value) => Some(value.to_string()),
        Value::Bool(value) => Some(value.to_string()),
        other => Some(other.to_string()),
    }
}

fn json_i64(value: Option<&Value>) -> Option<i64> {
    value.and_then(|value| {
        value
            .as_i64()
            .or_else(|| value.as_str()?.trim().parse::<i64>().ok())
    })
}

fn push_json_string(object: &mut Map<String, Value>, key: &str, value: String) {
    if let Some(values) = object.get_mut(key).and_then(Value::as_array_mut) {
        values.push(Value::String(value));
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn connection(connection_id: &str) -> Result<FirebirdConnection, IrodoriConnectorBuffer> {
    let guard = connections().lock().map_err(|_| {
        abi::error(
            "connector.statePoisoned",
            "Connector connection state is poisoned.",
        )
    })?;
    guard.get(connection_id).cloned().ok_or_else(|| {
        abi::error(
            "connector.connectionNotFound",
            format!("no open connection: {connection_id}"),
        )
    })
}

fn option_u16(request: &Value, fields: &[&str]) -> Option<u16> {
    request_containers(request)
        .into_iter()
        .find_map(|container| {
            fields.iter().find_map(|field| {
                container
                    .get(*field)
                    .and_then(|value| value.as_u64().or_else(|| value.as_str()?.parse().ok()))
                    .and_then(|value| u16::try_from(value).ok())
            })
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_firebird_url() {
        let parsed =
            parse_firebird_url("firebird://sysdba:masterkey@localhost:3050/employee").unwrap();
        assert_eq!(parsed.host, "localhost");
        assert_eq!(parsed.port, Some(3050));
        assert_eq!(parsed.database, "employee");
        assert_eq!(parsed.user.as_deref(), Some("sysdba"));
        assert_eq!(parsed.password.as_deref(), Some("masterkey"));
    }

    #[test]
    fn maps_basic_values() {
        assert_eq!(firebird_value_to_json(FbValue::Int(42)), json!(42));
        assert_eq!(
            firebird_value_to_json(FbValue::Text("abc".to_string())),
            json!("abc")
        );
    }
}
