# Loglugger: Formal Specification

## 1. Overview

Loglugger is a two-component system for collecting log records from systemd journald and persisting them to YDB (Yandex Database). The architecture consists of:

- **Client**: Reads from journald, optionally filters by service name, batches records, and sends them to the server via HTTP.
- **Server**: Receives batches, validates position continuity, and writes to YDB using BulkUpsert.

The system implements a **position-tracking protocol** to ensure exactly-once delivery semantics and ordered processing of log records per client.

---

## 2. Terminology

| Term | Definition |
|------|------------|
| **Position** | An opaque cursor representing a point in the journald stream. Implementation-specific (e.g., monotonic timestamp + offset, or cursor string). |
| **Current position** | The position at which the client started reading the batch. |
| **Next position** | The position immediately after the last record in the batch; used as the new expected position after successful processing. |
| **Expected position** | The position the server expects the client to send as "current" in the next batch. |
| **Reset** | A special signal indicating the client cannot resume from the expected position; the server accepts the batch without position validation. |

---

## 3. Architecture

```
┌─────────────────┐                    ┌─────────────────┐
│     Client      │                    │     Server      │
│  ┌───────────┐  │                    │  ┌───────────┐  │
│  │ journald  │  │                    │  │  HTTP API │  │
│  │  reader   │  │                    │  │           │  │
│  └─────┬─────┘  │                    │  └─────┬─────┘  │
│        │        │   HTTP POST        │        │        │
│  ┌─────▼─────┐  │  (JSON batches)    │  ┌─────▼─────┐  │
│  │  message  │  │──────────────────► │  │  field    │  │
│  │  parser   │  │                    │  │  mapping  │  │
│  │  (regex)  │  │  ◄─────────────────│  └─────┬─────┘  │
│  └─────┬─────┘  │   JSON response    │        │        │
│        │        │                    │  ┌─────▼─────┐  │
│  ┌─────▼─────┐  │                    │  │  YDB      │  │
│  │  position │  │                    │  │BulkUpsert │  │
│  │  tracker  │  │                    │  └───────────┘  │
└─────────────────┘                    └─────────────────┘
```

---

## 4. Client Specification

### 4.1 Responsibilities

1. Read log records from systemd journald.
2. Optionally filter records by service name using a configurable mask (e.g., glob, regex, or prefix).
3. Send raw MESSAGE field in the protocol payload.
4. Batch records for efficient transmission.
5. Send batches to one or more server endpoints via HTTP in JSON format.
6. Maintain and validate position continuity with the server.
7. Handle position mismatch and reset scenarios.

### 4.2 Journald Integration

- **Library**: Use `github.com/coreos/go-systemd/v22/sdjournal` or equivalent for journald access.
- **Read mode**: Sequential read from the journal, respecting cursor/position semantics.
- **Filtering**: Apply service name filter before batching. The filter mask may be:
  - Exact match: `_SYSTEMD_UNIT=foo.service`
  - Prefix match: `_SYSTEMD_UNIT=foo*.service`
  - Regex (if supported): configurable pattern
- **Fields to extract**: At minimum, `MESSAGE`, `PRIORITY`, `SYSLOG_IDENTIFIER`, `_SYSTEMD_UNIT`, `__REALTIME_TIMESTAMP`, `__MONOTONIC_TIMESTAMP`, and other standard journal fields as needed for the target schema.

### 4.3 Position Handling

- **Source of truth**: The server-side position store is the source of truth for the current expected position per `client_id`.
- **Startup lookup**: On startup, the client requests the current expected position from the server using a dedicated position lookup endpoint (see §5.2.1).
- **Initial state**: If the server has no stored position for the client, the client starts from head and sends `reset: true` in the first batch.
- **Normal operation**: After a successful batch, the server stores `next_position` as the new expected position. The client does not maintain a separate persistent local position store.
- **Read from position**: When the server returns a stored position, the client attempts to read journald starting from that position.
- **Reset condition**: The client sends `reset: true` when:
  - The server has no stored position for the client.
  - Journal was rotated or truncated; the server-provided position is no longer valid.
  - The server returned `expected_position` due to mismatch and the client cannot resume from that position (e.g., journal history was lost).
  - Configuration change that invalidates position (e.g., filter mask change).

### 4.3.1 Journal Corruption Handling

- **Corruption signal**: If journald iteration returns `EBADMSG` / "bad message", the client treats this as journal corruption rather than a transient transport error.
- **Default behavior**: By default, the client must log the corruption and stop immediately. The error message should mention that recovery is available via an explicit opt-in configuration switch.
- **Opt-in recovery**: When `journal_recovery` is enabled, the client may attempt best-effort recovery from corruption. This mode is disabled by default because it may skip records.
- **Recovery strategy**: A recovery attempt should:
  - Reopen the journal with the same namespace and exact-match filters.
  - First try to resume from the last known good cursor.
  - If that still fails, try to seek to a point just after the timestamp of the last known good entry.
- **Recovery warning**: When recovery is enabled and corruption is detected, the client must log a warning that some data loss is possible.
- **Recovery result**:
  - If recovery succeeds, the client resumes reading and must send the next batch with `reset: true`, because continuity relative to the previous server-side position may have been broken.
  - If recovery still fails, the client must stop and report that recovery is not possible.

### 4.4 Message Payload

The client sends raw `message` to the server. Parsing is performed on the server side and is configured via server settings.

### 4.5 Batching

- **Record count limit**: `batch_size` defines the maximum number of records in a normal batch.
- **Uncompressed payload limit**: The client must not send more than **10 MB** of log data (uncompressed) in one request.
  - Log-data size is calculated from record content fields (`message`, `fields` values, and selected metadata string fields), before gzip compression.
  - If adding another record would exceed 10 MB, the client flushes the current batch and sends the next records in subsequent requests.
  - If a single record itself is larger than 10 MB, the client still sends it as a single-record batch (exception to the normal cap), rather than dropping it.
- **Flush triggers**: Batch is sent when record-count limit is reached, uncompressed payload limit is reached, timeout expires, or on graceful shutdown.

### 4.6 HTTP Client

- **Methods**:
  - `GET` for startup position lookup.
  - `POST` for batch submission.
- **Content-Type**: `application/json` for batch submission.
- **Endpoints**:
  - Position lookup: `GET /v1/positions?client_id=<client_id>`
  - Batch submit: `POST /v1/batches`
- **Transport**: HTTPS with TLS. The client verifies the server certificate using a configurable trust store (see §9.1). For mTLS, the client presents its own certificate.
- **Multiple servers**: The client may be configured with multiple server base URLs. Endpoint selection is **sticky**: the client keeps using the current endpoint while requests succeed, and switches to the next endpoint only after a transient failure (network error or 5xx) during retry. As servers are stateless, they are expected to be connected to the same backing database.
- **Retries**: Implement endless retry with exponential backoff for transient failures (5xx, network errors) so batches are not dropped during prolonged outages. Do not retry on 4xx (except possibly 409 with position mismatch—see server spec).
- **Timeout**: Configurable request timeout.
- **Client identification**: Each client instance should have a unique identifier (e.g., hostname + instance ID) for server-side position tracking.

---

## 5. Server Specification

### 5.1 Responsibilities

1. Accept HTTP requests with batches of log records.
2. Validate position continuity per client.
3. Optionally parse `message` using a server-side regex and enrich records with parsed groups.
4. Map source fields to destination table columns using the configured field mapping.
5. Persist batches to the configured backend (`mock` for testing, or `ydb` for the actual usage).
6. Return appropriate responses including position information or errors.
7. Process requests from different clients concurrently.

### 5.2 HTTP API

#### 5.2.1 Endpoints

```
GET /v1/positions?client_id=<client_id>

POST /v1/batches
Content-Type: application/json
```

**Transport**: The server listens over TLS (HTTPS). It requires and verifies client certificates (mTLS) and validates client certificate subject fields (see §9).

#### 5.2.2 Position Lookup Response

**Success Response (200 OK, position found)**

```json
{
  "status": "ok",
  "current_position": "string"
}
```

**Success Response (200 OK, no stored position)**

```json
{
  "status": "not_found"
}
```

**Error Response (4xx/5xx)**

```json
{
  "status": "error",
  "message": "string"
}
```

#### 5.2.3 Request Body Schema

```json
{
  "client_id": "string",           // Required. Unique identifier for the client.
  "reset": false,                  // Optional. Default: false. If true, skip position validation.
  "current_position": "string",    // Required if reset is false. Position at batch start.
  "next_position": "string",       // Required. Position after last record in batch.
  "records": [                     // Required. Array of log records.
    {
      "message": "string",         // Raw message; present when parsing disabled or regex did not match.
      "priority": "int",
      "syslog_identifier": "string",
      "systemd_unit": "string",
      "realtime_timestamp": "int64",
      "monotonic_timestamp": "uint64",
      "fields": {}                 // Optional. Additional journal fields.
    }
  ]
}
```

**Payload rule**: Each record includes raw `message`. Parsing-related fields are not part of the client-server protocol.

#### 5.2.4 Success Response (200 OK)

```json
{
  "status": "ok",
  "next_position": "string"        // Echo of accepted next_position for client to store.
}
```

#### 5.2.5 Position Mismatch Response (409 Conflict)

```json
{
  "status": "position_mismatch",
  "expected_position": "string"    // Position the server expects; client should resume from here.
}
```

#### 5.2.6 Error Response (4xx/5xx)

```json
{
  "status": "error",
  "message": "string"              // Human-readable error description.
}
```

### 5.3 Position Validation Logic

```
IF request.reset == true:
  SKIP position check
  WRITE log records first
  IF record write failed:
    RETURN error
    DO NOT update expected_position
  STORE request.next_position as expected_position for client_id
  RETURN 200 with next_position

IF client_id has no stored expected_position:
  REJECT with 400 "missing current_position or reset required"

IF request.current_position != stored expected_position for client_id:
  REJECT with 409, return expected_position

ACCEPT batch
WRITE log records first
IF record write failed:
  RETURN error
  DO NOT update expected_position
STORE request.next_position as expected_position for client_id
RETURN 200 with next_position
```

**Durability requirement**: The server **must write log records before updating the stored position**. This ordering is required to avoid the risk of losing records by advancing the position past data that was not successfully persisted.

**Concurrency requirement**: The server **must support concurrent processing of requests from different clients**. Implementations must avoid global serialization of all batch requests, and must preserve per-client position safety under concurrent load.

### 5.4 YDB Integration

- **Operation**: BulkUpsert.
- **Table schema**: Defined separately; must include columns for all required log fields plus metadata (e.g., `client_id`, `received_at`).
- **Idempotency**: BulkUpsert is naturally idempotent for the same key. Design primary key to avoid duplicates (e.g., `client_id`, `position`, or `client_id` + `log_timestamp_us` + `message_hash`).
- **Recommended uniqueness fields**:
  - `log_timestamp_us`: record timestamp as `Timestamp64` (microsecond precision).
  - `message_hash`: `Uint64` from `CityHash64` over the full record payload.
- **Batching**: Map incoming records to table rows. Add server-side metadata (timestamp, client_id) before upsert.
- **Write ordering**: Successful record persistence must happen before position advancement. If record persistence fails, the server must not update `expected_position`.
- **Library**: Use `github.com/ydb-platform/ydb-go-sdk/v3` or equivalent.
- **Authentication modes**: YDB connection auth is configurable and supports:
  - `anonymous` (default)
  - `static` (login/password)
  - `service-account-key` (service account key file)
  - `metadata` (instance metadata credentials; optional metadata URL override)

### 5.5 Field Mapping (Source → Destination)

The server uses a **configurable mapping** between source fields (from the client payload) and destination table columns. This allows the schema to evolve independently of the client and supports different log formats.

- **Mapping schema**: A list of mappings, each specifying:
  - **Source**: Field path in the incoming record. May be:
    - Top-level: `message`, `priority`, `syslog_identifier`, `systemd_unit`, `realtime_timestamp`, `monotonic_timestamp`
    - Computed by mapper: `log_timestamp_us`, `message_cityhash64`
    - Parsed: `parsed.P_DTTM`, `parsed.P_SERVICE`, `parsed.P_LEVEL`, `parsed.P_MESSAGE`
    - Nested in `fields`: `fields.CODE_FILE`, `fields.CODE_LINE`
  - **Destination**: YDB table column name.
  - **Transform** (optional): Function to apply (e.g., parse string to `Timestamp64`, integer conversion, default value if missing).

- **Example mapping**:

```yaml
field_mapping:
  - source: client_id
    destination: client_id
  - source: log_timestamp_us
    destination: log_timestamp_us
    transform: timestamp64_us
  - source: message_cityhash64
    destination: message_hash
    transform: uint64
  - source: parsed.P_DTTM
    destination: ts_orig
    transform: timestamp64
  - source: parsed.P_SERVICE
    destination: service_name
  - source: parsed.P_LEVEL
    destination: log_level
  - source: parsed.P_MESSAGE
    destination: log_message
  - source: syslog_identifier
    destination: syslog_id
```

- **Resolution order**: When building a row, the server checks `parsed` first for parsed fields; if the record has `message` instead of `parsed`, the mapping for `parsed.*` yields no value (or a configured default).
- **Missing source**: If a mapped source field is absent, the destination column may be left NULL or filled with a default (configurable per mapping).
- **Unmapped columns**: Destination columns not in the mapping may be set from server metadata (e.g., `received_at` = now) or left NULL.
- **Time conversion option**: Server config option `convert_time_to_local_tz` controls parsing of timezone-less values in `timestamp64` transform:
  - `false` (default): parse timezone-less values as UTC.
  - `true`: parse timezone-less values in OS local timezone (`time.Local`) before saving. This is useful for local-time log formats but dangerous when hosts are configured inconsistently.

### 5.6 Server-Side Message Parsing (Optional)

Before mapping, the server may parse incoming `message` and `_SYSTEMD_UNIT` using configurable regexes with named capture groups:

- `message_regex`: regex pattern with named groups (e.g., `(?P<P_DTTM>...)`).
- `systemd_unit_regex`: regex pattern with named groups applied to `systemd_unit` (e.g., `(?P<P_UNIT_NAME>...)`).
- `message_regex_no_match`: behavior when regex does not match:
  - `send_raw` (default): keep the record and map raw `message`.
  - `skip`: drop the record from the batch before write.

Parsed groups from both regexes are merged and available to field mapping as `parsed.<GROUP_NAME>` but are not included in the protocol payload.

### 5.7 Position Storage

- **Backend**: Persistent store keyed by `client_id`. In the current design, this is a dedicated YDB table or an in-memory store for tests/local development.
- **Value**: `expected_position` string.
- **Update**: Update only after successful record write. If the record write fails, do not modify the stored position.
- **Retention**: Consider TTL or cleanup for inactive clients.

---

## 6. Data Models

### 6.1 Log Record (Client → Server)

Each record contains raw `message`. Parsing results are not sent over protocol.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| message | string | Yes | Raw log message from journald |
| priority | int | No | Syslog priority (0–7) |
| syslog_identifier | string | No | Syslog identifier |
| systemd_unit | string | No | systemd unit name |
| realtime_timestamp | int64 | No | Microseconds since epoch |
| monotonic_timestamp | uint64 | No | Monotonic clock value |
| fields | map[string]string | No | Additional journal fields |

### 6.2 Position Format

- **Recommendation**: Use journald cursor string when available, or a composite of `(monotonic_timestamp, offset)` encoded as a string (e.g., `base64` or `monotonic:offset`).
- **Opaque**: The server treats position as an opaque string; no parsing required for validation (equality check only).

---

## 7. Golang Best Practices

### 7.1 Project Structure

```
loglugger/
├── cmd/
│   ├── client/
│   │   └── main.go
│   └── server/
│       └── main.go
├── internal/
│   ├── client/
│   │   ├── journal.go      # journald reader
│   │   ├── parser.go       # message regex parser
│   │   ├── batcher.go      # batching logic
│   │   └── sender.go       # HTTP sender
│   ├── server/
│   │   ├── handler.go      # HTTP handler
│   │   ├── position.go     # position store
│   │   └── ydb.go          # YDB BulkUpsert
│   └── models/
│       ├── batch.go        # request/response types
│       └── record.go       # log record type
├── pkg/
│   └── ...                 # Public APIs if any
├── go.mod
├── go.sum
└── SPECIFICATION.md
```

### 7.2 Design Principles

- **Context propagation**: Use `context.Context` for cancellation and timeouts in all I/O operations.
- **Interfaces**: Define interfaces for journal reader, position store, and YDB writer to enable testing and swapping implementations.
- **Configuration**: Use `github.com/spf13/viper` or struct-based config with env/flag overrides; avoid hardcoded values.
- **Logging**: Use structured logging (`slog` or `zap`); avoid `log.Printf` in production code.
- **Error handling**: Use `fmt.Errorf` with `%w` for error wrapping; check `errors.Is`/`errors.As` where appropriate.
- **Graceful shutdown**: Handle `SIGINT`/`SIGTERM`; drain in-flight batches before exit.
- **Metrics**: Expose Prometheus metrics (optional but recommended): batches sent/received, records count, position mismatches, errors.

### 7.3 Concurrency

- **Client**: Single goroutine for journal read + batch send, or producer-consumer with bounded channel.
- **Server**: Stateless HTTP handlers; position store and YDB client must be safe for concurrent access (use sync primitives or transactional backend).

### 7.4 Testing

- **Unit tests**: Mock journal, HTTP, and YDB dependencies via interfaces.
- **Integration tests**: Optional; require journald and YDB availability.
- **Table-driven tests**: Use for handler validation logic (position mismatch, reset, etc.).

---

## 8. Configuration Reference

### 8.1 Client

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| server_urls | string/list | — | One or more server base URLs (must use `https://`); list or comma-separated string |
| client_id | string | hostname | Unique client identifier |
| service_mask | string | "" | Filter mask for `_SYSTEMD_UNIT` (empty = no filter) |
| journal_recovery | bool | false | Enable best-effort recovery from journal corruption (`EBADMSG` / "bad message"); may skip records and therefore may lose data |
| **Message parsing** | | | |
| batch_size | int | 50000 | Max records per batch (also constrained by the fixed 10 MB uncompressed log-data limit per request) |
| batch_timeout | duration | 5s | Max time before flushing partial batch |
| http_timeout | duration | 30s | HTTP request timeout |
| retry_delay | duration | 1s | Base delay for exponential backoff |
| **TLS** | | | |
| tls_ca_file | string | — | Path to PEM file with CA certs for server verification |
| tls_cert_file | string | — | Path to client certificate (PEM) for mTLS |
| tls_key_file | string | — | Path to client private key (PEM) for mTLS |
| tls_use_system_pool | bool | false | If true, add system CA pool to trust store (in addition to `tls_ca_file`) |

**TLS trust store**: `tls_ca_file` must be set (or `tls_use_system_pool` must be true) for server verification. For mTLS, both `tls_cert_file` and `tls_key_file` are required.

### 8.2 Server

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| config_file | string | — | Path to server YAML/JSON configuration file passed via `-config` |
| listen_addr | string | :27312 | HTTPS listen address |
| writer_backend | string | mock | Output backend (`mock`, `ydb`) |
| ydb_endpoint | string | — | YDB endpoint |
| ydb_database | string | — | YDB database path |
| ydb_table | string | logs | Target table name |
| ydb_auth_mode | string | anonymous | YDB auth mode (`anonymous`, `static`, `service-account-key`, `metadata`) |
| ydb_auth_login | string | — | Login for `static` auth mode |
| ydb_auth_password | string | — | Password for `static` auth mode |
| ydb_auth_sa_key_file | string | — | Path to service account key file for `service-account-key` auth mode |
| ydb_auth_metadata_url | string | — | Optional metadata endpoint URL override for `metadata` auth mode |
| ydb_ca_path | string | — | Optional path to PEM file with CA certificates for YDB TLS verification |
| position_table | string | loglugger_positions | YDB table used to store expected position per client |
| **Field mapping** | | | |
| field_mapping_file | string | — | Path to YAML/JSON file with source→destination field mappings |
| message_regex | string | `^(?:(?P<P_DTTM>[^ ]+)\s+)?:(?P<P_SERVICE>[^ ]+)\s+(?P<P_LEVEL>[^ ]+):\s+(?P<P_MESSAGE>.*)$` | Regex with named groups for server-side MESSAGE parsing |
| systemd_unit_regex | string | "" | Regex with named groups for server-side `_SYSTEMD_UNIT` parsing. Empty = parsing disabled |
| message_regex_no_match | string | send_raw | Server behavior when regex does not match: `send_raw` or `skip` |
| convert_time_to_local_tz | bool | false | Parse timezone-less `timestamp64` values in OS local timezone before writing (dangerous if timezone config differs across hosts) |
| **TLS** | | | |
| tls_cert_file | string | — | Path to server certificate (PEM) |
| tls_key_file | string | — | Path to server private key (PEM) |
| tls_ca_file | string | — | Path to PEM file with CA certs for client verification |
| tls_client_subject_cn | list | — | Required CN value(s) in client certificate subject |
| tls_client_subject_o | list | — | Required O value(s) in client certificate subject |
| tls_client_subject_ou | list | — | Required OU value(s) in client certificate subject |

**Server startup**: Most server settings are read from `config_file` (`-config` CLI flag). `listen_addr` may be overridden with `-listen` for quick local overrides.

**Backend coupling**: `writer_backend` also selects the position-store backend:
- `mock` -> in-memory position store
- `ydb` -> YDB position store using `position_table`

**TLS**: `tls_cert_file` and `tls_key_file` are required for HTTPS. For mTLS, `tls_ca_file` is required. Subject checks (`tls_client_subject_*`) are optional; if any are set, all configured attributes must match.

---

## 9. TLS Mutual Authentication

The client and server communicate over TLS with mutual certificate authentication (mTLS). Both sides verify the peer's certificate before establishing the connection.

### 9.1 Client-Side: Server Certificate Verification

The client **must** verify that the server's certificate is trusted before sending any data.

- **Trust store**: The client uses a configurable trust store to validate the server certificate chain. The trust store contains the CA certificates (or intermediate CAs) that signed the server certificate.
- **Configurable location**: The trust store path is a configuration parameter and points to a PEM file containing one or more CA certificates.
- **Default**: If not configured, the client MAY fall back to the system default trust store (e.g., `crypto/x509.SystemCertPool()`), but this should be explicitly documented and preferably disabled in production for stricter control.
- **Implementation**: Configure `tls.Config.RootCAs` with a `x509.CertPool` populated from the configured trust store. Do not use `InsecureSkipVerify`.

### 9.2 Server-Side: Client Certificate Verification

The server **must** verify that the client's certificate is trusted and that it contains the required subject field values.

- **Trust store**: The server uses a configurable trust store (CA certificates) in PEM file format to validate the client certificate chain.
- **Client auth mode**: The server requires and verifies client certificates (`tls.RequireAndVerifyClientCert`). Connections without a valid client certificate are rejected.
- **Subject field validation**: In addition to chain verification, the server checks that the client certificate's **Subject** contains specific required values. The required subject attributes are configurable. Typical attributes:
  - `CN` (Common Name): e.g., client hostname or identifier.
  - `O` (Organization): e.g., department or team.
  - `OU` (Organizational Unit): e.g., environment (prod, staging).
- **Validation logic**: For each configured subject attribute, the server extracts the corresponding value from the client certificate's Subject and compares it against the allowed/required value(s). Comparison may be:
  - Exact match: `CN` must equal `host-01-prod`.
  - List match: `O` must be one of `["team-a", "team-b"]`.
  - Pattern match: `OU` must match regex `^prod-.*`.
- **Rejection**: If the client certificate is untrusted or fails subject validation, the server terminates the TLS handshake with an appropriate alert (e.g., `bad_certificate` or `certificate_unknown`). No HTTP request is processed.

### 9.3 Subject Field Configuration (Server)

The server configuration defines which subject attributes are required and their expected values:

| Attribute | Config key example | Format | Description |
|-----------|-------------------|--------|-------------|
| CN | `tls_client_subject_cn` | list | Required Common Name(s) |
| O | `tls_client_subject_o` | list | Required Organization(s) |
| OU | `tls_client_subject_ou` | list | Required Organizational Unit(s) |

Each configured attribute is provided as a list. The certificate subject value must match at least one configured entry for that attribute. All configured attributes must be present and match.

### 9.4 Golang Implementation Notes

- **Client**: Use `x509.SystemCertPool()` as base (optional) or create new `x509.CertPool()`, then append certs from file via `AppendCertsFromPEM()`.
- **Server**: Use `tls.Config.ClientCAs` for the trust pool and `ClientAuth: tls.RequireAndVerifyClientCert`. For subject validation, implement a custom `tls.Config.VerifyConnection` callback (Go 1.15+) or a `GetConfigForClient` hook that inspects `ConnectionState.PeerCertificates[0].Subject` after the handshake. Note: `VerifyConnection` runs during the handshake; for subject checks, ensure the peer certificate is already verified by the default chain verification before applying custom logic.
- **Certificate parsing**: Use `x509.ParseCertificate()` for raw certs; access `cert.Subject` (type `pkix.Name`) for `CommonName`, `Organization`, `OrganizationalUnit`, etc.

---

## 10. Failure Modes and Recovery

| Scenario | Client Behavior | Server Behavior |
|----------|-----------------|-----------------|
| Network partition | Retry with backoff; buffer batches in memory (bounded) | N/A |
| Server restart | On startup, fetch current position from `GET /v1/positions`; then continue or reset | Position stored on server persists |
| Journal rotation | If server-provided cursor cannot be used, send reset and restart from head | Accept with reset; update expected position |
| Journal corruption (`EBADMSG`) with recovery disabled | Log corruption, mention recovery option, and stop | Position stored on server remains unchanged |
| Journal corruption (`EBADMSG`) with recovery enabled | Warn about possible data loss; try reopen/reseek recovery; on success resume with `reset: true`; on failure stop | Accept next successful recovery batch with reset; otherwise position stored on server remains unchanged |
| YDB unavailable | Client retries; server returns 5xx | Fail batch; do not update position |
| Duplicate batch (retry) | Client may retry same batch | Idempotent BulkUpsert; position already updated—reject with 409 if current_position no longer matches |

---

## 11. Appendix: Example Batch Request

**Example batch request**:

```json
{
  "client_id": "host-01-prod",
  "reset": false,
  "current_position": "s=12345;o=67890;",
  "next_position": "s=12345;o=68190;",
  "records": [
    {
      "message": "2025-03-13T10:00:00 :nginx INFO: Server started",
      "priority": 6,
      "syslog_identifier": "nginx",
      "systemd_unit": "nginx.service",
      "realtime_timestamp": 1710345600000000,
      "monotonic_timestamp": 12345678,
      "fields": {}
    }
  ]
}
```
