# Loglugger: Formal Specification

## Document Information

| Field | Value |
|-------|-------|
| Version | 1.2 |
| Date | 2025-03-13 |
| Status | Draft |

---

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
│  │  message  │  │──────────────────►│  │  field    │  │
│  │  parser   │  │                    │  │  mapping  │  │
│  │  (regex)  │  │  ◄─────────────────│  └─────┬─────┘  │
│  └─────┬─────┘  │   JSON response    │        │        │
│        │        │                    │  ┌─────▼─────┐  │
│  ┌─────▼─────┐  │                    │  │  YDB     │  │
│  │  position │  │                    │  │BulkUpsert│  │
│  │  tracker  │  │                    │  └──────────┘  │
└─────────────────┘                    └─────────────────┘
```

---

## 4. Client Specification

### 4.1 Responsibilities

1. Read log records from systemd journald.
2. Optionally filter records by service name using a configurable mask (e.g., glob, regex, or prefix).
3. Optionally parse the MESSAGE field with a regex; extract named groups and send them instead of the raw message.
4. Batch records for efficient transmission.
5. Send batches to the server via HTTP in JSON format.
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

- **Initial state**: On first run, the client has no stored position. It sends `reset: true` in the first batch.
- **Normal operation**: After a successful response, the client stores the `next_position` from the batch as the expected starting position for the next read.
- **Read from position**: The client attempts to read journald starting from the expected position.
- **Reset condition**: The client sends `reset: true` when:
  - First run (no stored position).
  - Journal was rotated or truncated; expected position is no longer valid.
  - Server returned `expected_position` due to mismatch; client cannot resume from that position (e.g., journal history was lost).
  - Configuration change that invalidates position (e.g., filter mask change).

### 4.4 Message Parsing (Optional)

Before batching, the client may parse the journal `MESSAGE` field using a configurable regular expression with **named capture groups**. The parsed fields replace the raw message in the payload sent to the server.

- **Regex**: Configurable pattern. Use Go's named group syntax: `(?P<Name>subexpr)`.
- **Example**: `^(?P<P_DTTM>[^ ]*) :(?P<P_SERVICE>[^ ]*) (?P<P_LEVEL>[^ ]*): (?P<P_MESSAGE>.*)$` — extracts `P_DTTM`, `P_SERVICE`, `P_LEVEL`, `P_MESSAGE` from messages like `2025-03-13T10:00:00 :nginx INFO: Server started`.
- **Output**: When the regex matches, the client sends the named groups as a `parsed` map (e.g., `{"P_DTTM": "...", "P_SERVICE": "...", "P_LEVEL": "...", "P_MESSAGE": "..."}`) instead of the original `message`. Journal metadata (priority, timestamps, etc.) is still sent.
- **No match**: If the regex does not match, the client either:
  - **Option A** (default): Send the record with `message` set to the raw value and `parsed` omitted or empty.
  - **Option B** (configurable): Skip the record (do not include in batch).
- **Compilation**: The regex is compiled once at startup. Invalid regex causes client startup failure.
- **Golang**: Use `regexp.MustCompile()` or `regexp.Compile()`; `FindStringSubmatch()` returns slice where index 0 is full match, and `SubexpNames()` gives group names. Build the `parsed` map from `SubexpIndex(name)` for each named group.

### 4.5 Batching

- **Batch size**: Configurable (e.g., max records per batch, max bytes, or max time window).
- **Flush triggers**: Batch is sent when size limit is reached, timeout expires, or on graceful shutdown.

### 4.6 HTTP Client

- **Method**: POST.
- **Content-Type**: `application/json`.
- **Endpoint**: Configurable base URL + path (e.g., `/v1/batches`).
- **Transport**: HTTPS with TLS. The client verifies the server certificate using a configurable trust store (see §9.1). For mTLS, the client presents its own certificate.
- **Retries**: Implement retry with exponential backoff for transient failures (5xx, network errors). Do not retry on 4xx (except possibly 409 with position mismatch—see server spec).
- **Timeout**: Configurable request timeout.
- **Client identification**: Each client instance should have a unique identifier (e.g., hostname + instance ID) for server-side position tracking.

---

## 5. Server Specification

### 5.1 Responsibilities

1. Accept HTTP requests with batches of log records.
2. Validate position continuity per client.
3. Map source fields to destination table columns using the configured field mapping.
4. Persist batches to YDB via BulkUpsert.
5. Return appropriate responses including position information or errors.

### 5.2 HTTP API

#### 5.2.1 Endpoint

```
POST /v1/batches
Content-Type: application/json
```

**Transport**: The server listens over TLS (HTTPS). It requires and verifies client certificates (mTLS) and validates client certificate subject fields (see §9).

#### 5.2.2 Request Body Schema

```json
{
  "client_id": "string",           // Required. Unique identifier for the client.
  "reset": false,                  // Optional. Default: false. If true, skip position validation.
  "current_position": "string",    // Required if reset is false. Position at batch start.
  "next_position": "string",       // Required. Position after last record in batch.
  "records": [                     // Required. Array of log records.
    {
      "message": "string",         // Raw message; present when parsing disabled or regex did not match.
      "parsed": {                  // Parsed fields from regex; present when regex matched. Replaces message.
        "P_DTTM": "string",
        "P_SERVICE": "string",
        "P_LEVEL": "string",
        "P_MESSAGE": "string"
      },
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

**Payload rule**: Each record contains either `message` (raw) or `parsed` (extracted fields), not both. When the client's regex matches, `parsed` is sent; otherwise `message` is sent.

#### 5.2.3 Success Response (200 OK)

```json
{
  "status": "ok",
  "next_position": "string"        // Echo of accepted next_position for client to store.
}
```

#### 5.2.4 Position Mismatch Response (409 Conflict)

```json
{
  "status": "position_mismatch",
  "expected_position": "string"    // Position the server expects; client should resume from here.
}
```

#### 5.2.5 Error Response (4xx/5xx)

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
  ACCEPT batch
  STORE request.next_position as expected_position for client_id
  RETURN 200 with next_position

IF client_id has no stored expected_position:
  REJECT with 400 "missing current_position or reset required"

IF request.current_position != stored expected_position for client_id:
  REJECT with 409, return expected_position

ACCEPT batch
STORE request.next_position as expected_position for client_id
RETURN 200 with next_position
```

### 5.4 YDB Integration

- **Operation**: BulkUpsert.
- **Table schema**: Defined separately; must include columns for all required log fields plus metadata (e.g., `client_id`, `received_at`).
- **Idempotency**: BulkUpsert is naturally idempotent for the same key. Design primary key to avoid duplicates (e.g., `client_id`, `position`, or `realtime_timestamp` + `monotonic_timestamp` + `client_id`).
- **Batching**: Map incoming records to table rows. Add server-side metadata (timestamp, client_id) before upsert.
- **Library**: Use `github.com/ydb-platform/ydb-go-sdk/v3` or equivalent.

### 5.5 Field Mapping (Source → Destination)

The server uses a **configurable mapping** between source fields (from the client payload) and destination table columns. This allows the schema to evolve independently of the client and supports different log formats.

- **Mapping schema**: A list of mappings, each specifying:
  - **Source**: Field path in the incoming record. May be:
    - Top-level: `message`, `priority`, `syslog_identifier`, `systemd_unit`, `realtime_timestamp`, `monotonic_timestamp`
    - Parsed: `parsed.P_DTTM`, `parsed.P_SERVICE`, `parsed.P_LEVEL`, `parsed.P_MESSAGE`
    - Nested in `fields`: `fields.CODE_FILE`, `fields.CODE_LINE`
  - **Destination**: YDB table column name.
  - **Transform** (optional): Function to apply (e.g., parse timestamp string to int64, default value if missing).

- **Example mapping**:

```yaml
field_mapping:
  - source: parsed.P_DTTM
    destination: log_dttm
  - source: parsed.P_SERVICE
    destination: service_name
  - source: parsed.P_LEVEL
    destination: log_level
  - source: parsed.P_MESSAGE
    destination: log_message
  - source: syslog_identifier
    destination: syslog_id
  - source: realtime_timestamp
    destination: ts_epoch_us
  - source: client_id
    destination: client_id
```

- **Resolution order**: When building a row, the server checks `parsed` first for parsed fields; if the record has `message` instead of `parsed`, the mapping for `parsed.*` yields no value (or a configured default).
- **Missing source**: If a mapped source field is absent, the destination column may be left NULL or filled with a default (configurable per mapping).
- **Unmapped columns**: Destination columns not in the mapping may be set from server metadata (e.g., `received_at` = now) or left NULL.

### 5.6 Position Storage

- **Backend**: Persistent store (e.g., YDB table, Redis, or file) keyed by `client_id`.
- **Value**: `expected_position` string.
- **Update**: Atomic update on successful batch processing.
- **Retention**: Consider TTL or cleanup for inactive clients.

---

## 6. Data Models

### 6.1 Log Record (Client → Server)

Each record contains either `message` (raw) or `parsed` (regex-extracted fields), depending on whether the client's message regex matched.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| message | string | Conditional | Raw log message; present when parsing disabled or regex did not match |
| parsed | map[string]string | Conditional | Named capture groups from regex; present when regex matched. Keys are group names (e.g., `P_DTTM`, `P_SERVICE`, `P_LEVEL`, `P_MESSAGE`) |
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
| server_url | string | — | Base URL of the server (must use `https://`) |
| client_id | string | hostname | Unique client identifier |
| service_mask | string | "" | Filter mask for `_SYSTEMD_UNIT` (empty = no filter) |
| **Message parsing** | | | |
| message_regex | string | "" | Regex with named groups to parse MESSAGE. Empty = no parsing, send raw message |
| message_regex_no_match | string | send_raw | Behavior when regex does not match: `send_raw` (default) or `skip` |
| batch_size | int | 1000 | Max records per batch |
| batch_timeout | duration | 5s | Max time before flushing partial batch |
| position_file | string | — | Path to store position (optional) |
| http_timeout | duration | 30s | HTTP request timeout |
| retry_max | int | 5 | Max retries on transient failure |
| retry_base_delay | duration | 1s | Base delay for exponential backoff |
| **TLS** | | | |
| tls_ca_file | string | — | Path to PEM file with CA certs for server verification |
| tls_ca_path | string | — | Path to directory with PEM CA certs (alternative to `tls_ca_file`) |
| tls_cert_file | string | — | Path to client certificate (PEM) for mTLS |
| tls_key_file | string | — | Path to client private key (PEM) for mTLS |
| tls_use_system_pool | bool | false | If true, add system CA pool to trust store (in addition to `tls_ca_*`) |

**TLS trust store**: Either `tls_ca_file` or `tls_ca_path` must be set (or `tls_use_system_pool` true) for server verification. For mTLS, both `tls_cert_file` and `tls_key_file` are required.

### 8.2 Server

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| listen_addr | string | :8080 | HTTP listen address (use `:8443` for TLS) |
| ydb_endpoint | string | — | YDB endpoint |
| ydb_database | string | — | YDB database path |
| ydb_table | string | logs | Target table name |
| position_store | string | ydb | Backend for position storage (ydb, memory) |
| **Field mapping** | | | |
| field_mapping_file | string | — | Path to YAML/JSON file with source→destination field mappings |
| field_mapping | list | — | Inline mapping (alternative to file). Format: `[{source, destination}]` |
| **TLS** | | | |
| tls_cert_file | string | — | Path to server certificate (PEM) |
| tls_key_file | string | — | Path to server private key (PEM) |
| tls_ca_file | string | — | Path to PEM file with CA certs for client verification |
| tls_ca_path | string | — | Path to directory with PEM CA certs (alternative to `tls_ca_file`) |
| tls_client_subject_cn | string/list | — | Required CN value(s) in client certificate subject |
| tls_client_subject_o | string/list | — | Required O value(s) in client certificate subject |
| tls_client_subject_ou | string/list | — | Required OU value(s) in client certificate subject |

**TLS**: `tls_cert_file` and `tls_key_file` are required for HTTPS. For mTLS, `tls_ca_file` or `tls_ca_path` is required. Subject checks (`tls_client_subject_*`) are optional; if any are set, all configured attributes must match.

---

## 9. TLS Mutual Authentication

The client and server communicate over TLS with mutual certificate authentication (mTLS). Both sides verify the peer's certificate before establishing the connection.

### 9.1 Client-Side: Server Certificate Verification

The client **must** verify that the server's certificate is trusted before sending any data.

- **Trust store**: The client uses a configurable trust store to validate the server certificate chain. The trust store contains the CA certificates (or intermediate CAs) that signed the server certificate.
- **Configurable location**: The trust store path is a configuration parameter. Supported formats:
  - **PEM file**: Path to a file containing one or more PEM-encoded CA certificates.
  - **Directory**: Path to a directory containing PEM files (e.g., hashed certificate names per OpenSSL conventions). The client loads all certificates from the directory.
- **Default**: If not configured, the client MAY fall back to the system default trust store (e.g., `crypto/x509.SystemCertPool()`), but this should be explicitly documented and preferably disabled in production for stricter control.
- **Implementation**: Configure `tls.Config.RootCAs` with a `x509.CertPool` populated from the configured trust store. Do not use `InsecureSkipVerify`.

### 9.2 Server-Side: Client Certificate Verification

The server **must** verify that the client's certificate is trusted and that it contains the required subject field values.

- **Trust store**: The server uses a configurable trust store (CA certificates) to validate the client certificate chain. Format: same as client (PEM file or directory).
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
| CN | `tls.client_subject.cn` | string or list | Required Common Name(s) |
| O | `tls.client_subject.o` | string or list | Required Organization(s) |
| OU | `tls.client_subject.ou` | string or list | Required Organizational Unit(s) |
| (custom) | `tls.client_subject.<oid>` | string or list | Custom OID values |

If multiple values are provided (list), the certificate subject value must match at least one. All configured attributes must be present and match.

### 9.4 Golang Implementation Notes

- **Client**: Use `x509.SystemCertPool()` as base (optional) or create new `x509.CertPool()`, then append certs from file via `AppendCertsFromPEM()` or from directory by reading and parsing each PEM file.
- **Server**: Use `tls.Config.ClientCAs` for the trust pool and `ClientAuth: tls.RequireAndVerifyClientCert`. For subject validation, implement a custom `tls.Config.VerifyConnection` callback (Go 1.15+) or a `GetConfigForClient` hook that inspects `ConnectionState.PeerCertificates[0].Subject` after the handshake. Note: `VerifyConnection` runs during the handshake; for subject checks, ensure the peer certificate is already verified by the default chain verification before applying custom logic.
- **Certificate parsing**: Use `x509.ParseCertificate()` for raw certs; access `cert.Subject` (type `pkix.Name`) for `CommonName`, `Organization`, `OrganizationalUnit`, etc.

---

## 10. Failure Modes and Recovery

| Scenario | Client Behavior | Server Behavior |
|----------|-----------------|-----------------|
| Network partition | Retry with backoff; buffer batches in memory (bounded) | N/A |
| Server restart | Retry; position stored on server persists | Reject until client sends matching position or reset |
| Journal rotation | Send reset; restart from head or saved cursor | Accept with reset; update expected position |
| YDB unavailable | Client retries; server returns 503 | Fail batch; do not update position |
| Duplicate batch (retry) | Client may retry same batch | Idempotent BulkUpsert; position already updated—reject with 409 if current_position no longer matches |

---

## 11. Appendix: Example Batch Request

**With parsed fields** (regex matched):

```json
{
  "client_id": "host-01-prod",
  "reset": false,
  "current_position": "s=12345;o=67890;",
  "next_position": "s=12345;o=68190;",
  "records": [
    {
      "parsed": {
        "P_DTTM": "2025-03-13T10:00:00",
        "P_SERVICE": "nginx",
        "P_LEVEL": "INFO",
        "P_MESSAGE": "Server started"
      },
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

**With raw message** (parsing disabled or regex did not match):

```json
{
  "client_id": "host-01-prod",
  "reset": false,
  "current_position": "s=12345;o=67890;",
  "next_position": "s=12345;o=68190;",
  "records": [
    {
      "message": "Server started",
      "priority": 6,
      "syslog_identifier": "nginx",
      "systemd_unit": "nginx.service",
      "realtime_timestamp": 1710345600000000,
      "monotonic_timestamp": 12345678,
      "fields": {
        "CODE_FILE": "src/core/nginx.c",
        "CODE_LINE": "1234"
      }
    }
  ]
}
```

---

## 12. Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2025-03-13 | — | Initial specification |
| 1.1 | 2025-03-13 | — | TLS mTLS authentication: client trust store, server client cert + subject validation |
| 1.2 | 2025-03-13 | — | Client message regex parsing; server field mapping (source→destination) |
