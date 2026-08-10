# Loglugger: Formal Specification

## 1. Overview

Loglugger is a multi-component system for collecting log records from systemd journald, persisting them to YDB (Yandex Database), and exporting stored records for downstream processing. The architecture consists of:

- **Client**: Reads from journald, optionally filters by service name, batches records, and sends them to the server via HTTP.
- **Server**: Receives batches, validates position continuity, and writes to YDB using BulkUpsert.
- **Extractor tool**: Reads records from YDB with query-time filters and writes them to TSV files with optional zstd compression.

The system implements a **position-tracking protocol** to ensure the at-least-once delivery semantics and ordered processing of log records per client.

All timestamps transferred or persisted by Loglugger as typed timestamp values are UTC. In particular, Unix-epoch timestamps are interpreted as UTC, and timezone-less values passed through the `timestamp64` transform are parsed as UTC before storage.

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
│  └───────────┘  │                    │                 │
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
- **Client sequence number**: The client adds `seqno` to every outgoing record. `seqno` is monotonically increasing for the lifetime of the process; the first value equals client startup time in milliseconds since Unix epoch.

### 4.3 Position Handling

- **Source of truth**: The server-side position store is the source of truth for the current expected position per `client_id`.
- **Startup lookup**: On startup, the client requests the current expected position from the server using a dedicated position lookup endpoint (see §5.2.1).
- **Initial state**: If the server has no stored position for the client, the client starts from head and sends `reset: true` in the first batch.
- **Normal operation**: After a successful batch, the server stores `next_position` as the new expected position. The client does not maintain a separate persistent local position store.
- **Read from position**: When the server returns a stored position, the client attempts to read journald starting from that position.
- **Cursor exactness**: Resuming from a journald cursor **must** verify that the sought entry still exists, using `sd_journal_test_cursor` (or equivalent) after `sd_journal_seek_cursor` and the required positioning `sd_journal_next`. `sd_journal_seek_cursor` alone is not sufficient: when the entry has been vacuumed or rotated away, journald still returns success and lands on the nearest following entry. Treating that soft match as an exact resume would skip the nearest surviving record on the next `Next()` and violate at-least-once delivery. Cursor strings must not be compared with ordinary string equality; the same entry may have multiple cursor representations.
- **Stale cursor handling**: If the stored cursor is missing or does not match the entry selected after seek, the client must treat the position as no longer valid: fail the seek, send `reset: true`, and restart from head (or otherwise follow the reset path below). It must not continue sequential reading from the nearest neighbor while keeping the stale protocol position.
- **Reset condition**: The client sends `reset: true` when:
  - The server has no stored position for the client.
  - Journal was rotated or truncated; the server-provided position is no longer valid (including when `sd_journal_test_cursor` rejects the stored cursor).
  - The server returned `expected_position` due to mismatch and the client cannot resume from that position (e.g., journal history was lost).
  - Configuration change that invalidates position (e.g., filter mask change).
- **Position mismatch / stream restart**: When the server returns HTTP `409` with `position_mismatch`, the client reseeks to `expected_position` or resets to head. Because batching may retain unsent records after a partial flush (JSON-size or record-count split), the client **must discard** any remaining in-memory batch buffer and unfinished multiline state before continuing. Leftover records still carry pre-mismatch `current_position` values and must not be sent after the stream restart (doing so can repeat `409` responses or attach stale records to a `reset: true` batch). If both the seek to `expected_position` and the fallback seek to head fail, the client **must fail-stop**; it must not continue reading or send a `reset: true` batch from an unrepositioned journal stream.

### 4.3.1 Journal Corruption Handling

- **Corruption signal**: If journald iteration returns `EBADMSG` / "bad message", the client treats this as journal corruption rather than a transient transport error.
- **Default behavior**: By default, the client must log the corruption and stop immediately. The error message should mention that recovery is available via an explicit opt-in configuration switch.
- **Opt-in recovery**: When `journal_recovery` is enabled, the client may attempt best-effort recovery from corruption. This mode is disabled by default because it may skip records.
- **Recovery strategy**: A recovery attempt should:
  - Reopen the journal with the same namespace and exact-match filters.
  - First try to resume from the last known good cursor, verifying exact cursor match (`sd_journal_test_cursor`) the same way as normal position seek. A soft nearest-neighbor match is a failure of this step.
  - If that still fails, try to seek to a point just after the timestamp of the last known good entry.
- **Recovery warning**: When recovery is enabled and corruption is detected, the client must log a warning that some data loss is possible.
- **Recovery result**:
  - If recovery succeeds, the client resumes reading and must send the next batch with `reset: true`, because continuity relative to the previous server-side position may have been broken.
  - The client **must retain** any unsent in-memory batch buffer and unfinished multiline state across successful recovery, and must ship that retained data (with `reset: true` on the next send). Recovery resumes after the last successfully read journal cursor (`last`), so discarded buffers would never be re-read from journald and would violate at-least-once delivery. This differs from a §4.3 position-mismatch stream restart, where the journal is repositioned to a server `expected_position` (or head) and leftover pre-mismatch buffers must be discarded because they carry stale protocol positions.
  - If recovery still fails, the client must stop and report that recovery is not possible.

### 4.3.2 Persistent Journal Read Failures

- **Non-corruption I/O errors**: Journal read failures that are not `EBADMSG` / "bad message" and not caused by client cancellation (for example local journald I/O or wait failures) are treated as potentially transient.
- **Bounded retry**: The client may retry such errors with a short delay for a limited window (on the order of ~15 seconds).
- **Fail-stop**: If the same class of error persists beyond that window, the client must flush any already-buffered batch when possible and stop. Endless retry without exit or escalation is not allowed: the process would appear healthy while log shipping is stalled.
- **Supervisor restart**: Stopping allows an external process supervisor to restart the client and/or raise an alert. On restart, the client resumes from the server-stored position (§4.3).

### 4.4 Message Payload

The client sends raw `message` to the server and may additionally send `parsed` groups extracted by client-side regex parsing (`message_regex`, `systemd_unit_regex`).

When `message_regex_no_match` is `skip`, records that do not match `message_regex` are discarded client-side and **must not** advance the acknowledged protocol position used for `current_position` / `next_position`. Position continuity (§4.3 / §5.3) is defined over the stream of sent records, the same way journal-level filters (`service_mask`, empty `MESSAGE`) preserve continuity relative to the last sent record.

When `message_regex` is configured, the client also supports multiline merge:

- Start with the current journal message text.
- Append subsequent message texts (joined with newline) while they do not match `message_regex`.
- Stop merging when one of the following happens:
  - the next message matches `message_regex` (it starts a new independent message);
  - the next message does not arrive within `multiline_timeout` (default `1s`);
  - the number of merged source messages reaches `multiline_max_messages` (default `1000`).

### 4.5 Batching

- **Record count limit**: `batch_size` defines the maximum number of records in a normal batch.
- **Uncompressed JSON limit**: The client must not send more than **15 MiB** of uncompressed JSON body in one request.
  - Size is the exact byte length of the `POST /v1/batches` JSON document (envelope + `records` array), measured before gzip compression.
  - The client marshals each record to JSON once when it is added to the batch and tracks the running request size incrementally; records are not re-serialized for size checks or for the final request body.
  - If adding another record would exceed 15 MiB, the client flushes the current batch and sends the next records in subsequent requests.
  - If a single record cannot fit under the 15 MiB limit together with the request envelope, the client must fail-stop rather than send an oversized request or drop the record silently.
- **Flush triggers**: Batch is sent when record-count limit is reached, uncompressed JSON limit is reached, timeout expires, or on graceful shutdown.
- **Compatibility with server limits**: The default client JSON cap (15 MiB) is chosen to stay below the default server limits of 16 MiB compressed and 32 MiB decompressed, including gzip overhead on poorly compressible payloads.

### 4.6 HTTP Client

- **Methods**:
  - `GET` for startup position lookup.
  - `POST` for batch submission.
- **Content-Type**: `application/json` for batch submission.
- **Content-Encoding**: `gzip` for batch submission. The client **must** gzip-compress the JSON request body and set `Content-Encoding: gzip` on `POST /v1/batches` (see §4.5 for the related uncompressed JSON limit).
- **Endpoints**:
  - Position lookup: `GET /v1/positions?client_id=<client_id>`
  - Batch submit: `POST /v1/batches`
- **Transport**: HTTPS with TLS. The client verifies the server certificate using a configurable trust store (see §9.1). For mTLS, the client presents its own certificate.
- **Multiple servers**: The client may be configured with multiple server base URLs. Endpoint selection is **sticky** between requests: after a successful request the client keeps using that endpoint, and on a transient failure (network error or 5xx) it advances to the next configured endpoint for the following retry attempt. In addition, when two or more endpoints are configured, the client periodically **reshuffles** the endpoint list every 30–60 minutes (randomized interval) so long-lived clients redistribute load across healthy servers even without failures; the next request after a due reshuffle may therefore use a different endpoint than the previous successful one. As servers are stateless, they are expected to be connected to the same backing database.
- **Retries**: Implement endless retry with exponential backoff for transient failures (5xx, network errors) so batches are not dropped during prolonged outages. Do not retry on 4xx (except possibly 409 with position mismatch—see server spec). On non-retryable 4xx after a batch has been flushed from the in-memory buffer, the client must stop rather than continue reading journald; otherwise the flushed batch would be skipped (especially dangerous when the next send uses `reset: true`).
- **Timeout**: Configurable request timeout.
- **Client identification**: Each client instance must be configured with a distinct `client_id` (e.g., hostname + instance ID) for server-side position tracking. Uniqueness is an operational/configuration prerequisite; see §5.3.1.

---

## 5. Server Specification

### 5.1 Responsibilities

1. Accept HTTP requests with batches of log records.
2. Validate position continuity per client.
3. Map source fields to destination table columns using the configured field mapping.
4. Persist batches to the configured backend (`mock` for testing, or `ydb` for the actual usage).
5. Return appropriate responses including position information or errors.
6. Process requests from different clients concurrently.

### 5.2 HTTP API

#### 5.2.1 Endpoints

```
GET /v1/positions?client_id=<client_id>

POST /v1/batches
Content-Type: application/json
Content-Encoding: gzip
```

**Batch body encoding**: Clients **must** send the JSON batch body gzip-compressed with `Content-Encoding: gzip` (§4.6). The server **must** accept `gzip`. For interoperability with ad-hoc clients and proxies, the server also accepts an uncompressed body when `Content-Encoding` is omitted or set to `identity`; any other `Content-Encoding` value is rejected.

**Request size limits**: The server rejects oversized batch bodies with HTTP 413:
- `max_compressed_body_bytes` (default **16 MiB**): maximum raw HTTP body size before decoding `Content-Encoding`.
- `max_decompressed_body_bytes` (default **32 MiB**): maximum JSON payload size after decompression.

**HTTP server timeouts**: The server sets `http.Server` timeouts to bound stalled TLS/HTTP exchanges (Slowloris-style connection exhaustion). All values are configurable and must be greater than zero:
- `read_header_timeout` (default **10s**): TLS handshake + request headers.
- `read_timeout` (default **60s**): entire request including body.
- `write_timeout` (default **60s**): handler processing + response write.
- `idle_timeout` (default **120s**): keep-alive idle connection lifetime.

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
  "client_id": "string",           // Required. Client identifier; uniqueness is an operational prerequisite (see §5.3.1), not enforced by the server.
  "reset": false,                  // Optional. Default: false. If true, skip position validation.
  "current_position": "string",    // Required if reset is false. Position at batch start.
  "next_position": "string",       // Required. Position after last record in batch.
  "records": [                     // Required. Array of log records.
    {
      "message": "string",         // Raw message; present when parsing disabled or regex did not match.
      "parsed": {"KEY":"VALUE"},   // Optional named groups produced by client-side regex parsing.
      "seqno": "int64",            // Monotonic sequence number generated by client process.
      "priority": "int",
      "syslog_identifier": "string",
      "systemd_unit": "string",
      "realtime_ts": "int64",
      "fields": {}                 // Optional. Additional journal fields.
    }
  ]
}
```

**Payload rule**: Each record includes raw `message`. When client-side parsing is enabled, parsed named groups are sent in `parsed` and may be used by mapping rules.

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

**Concurrency requirement**: The server **must support concurrent processing of requests from different clients**. Implementations must avoid global serialization of all batch requests, and must preserve per-client position safety under concurrent load when each `client_id` has a single active sender (the intended deployment model).

### 5.3.1 `client_id` Uniqueness (Out of Scope)

Ensuring uniqueness of `client_id` across client instances is **not considered and not solved** by this protocol or by the server in the current version. The server does not detect, reject, or coordinate multiple senders that share the same `client_id`. Operators are responsible for assigning a distinct `client_id` to each client instance.

The position-tracking protocol assumes **at most one active sender per `client_id`**. If that assumption is violated—whether by misconfiguration, accidental reuse of the same `client_id`, a duplicate deployment, a buggy client that issues parallel batch submits for one `client_id`, or an external replay/proxy that duplicates in-flight requests—the following problems are **inevitable** and are not treated as defects relative to this specification:

1. **Position races**: Concurrent `POST /v1/batches` for the same `client_id` may both pass the position check before either updates the stored expected position.
2. **Writes visible with `409 position_mismatch`**: Because records are written before the position update (§5.3 durability requirement), a request that later loses the position update may already have persisted rows, yet still return `409 position_mismatch`.
3. **Interleaved position streams**: Independent journal readers sharing one `client_id` advance a single shared expected position, producing non-contiguous / mutually invalid `current_position` values from each sender’s point of view.
4. **Duplicate or overwritten rows**: Retries or competing batches may insert duplicates (if the table primary key does not cover the colliding rows) or silently overwrite via idempotent BulkUpsert (if it does)—exactly-once delivery is not guaranteed under shared `client_id`.
5. **Lost progress for a competing sender**: When one sender successfully updates the shared expected position, other senders using the same `client_id` observe mismatches and must reset or reseek, potentially skipping or re-sending data.

Mitigation of these outcomes (for example global or per-client request locking, transactional write+position commit, or server-side uniqueness enforcement) is **out of scope** for the current version.

### 5.4 YDB Integration

- **Operation**: BulkUpsert.
- **Table schema**: Defined separately; must include columns for all required log fields plus metadata (e.g., `client_id`, `received_at`).
- **Idempotency**: BulkUpsert is naturally idempotent for the same key. Design primary key to avoid duplicates (e.g., `client_id`, `position`, or `client_id` + `log_timestamp_us` + `message_hash`).
- **Recommended uniqueness fields**:
  - `log_timestamp_us`: record timestamp as `Timestamp64` (microsecond precision).
  - `message_hash`: `Uint64` from `CityHash64` over the full record payload.
- **Batching**: Map incoming records to table rows. Add server-side metadata (timestamp, client_id) before upsert.
- **Timezone rule**: Typed timestamps transferred to YDB are stored in UTC.
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
    - Top-level: `message`, `seqno`, `priority`, `syslog_identifier`, `systemd_unit`, `realtime_ts`
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
  - source: realtime_ts
    destination: ts_orig
    transform: timestamp64_us
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
- **Transform / mapping failure**: If a present source value cannot be converted by the configured transform (or another per-record mapping step fails), the server **rejects the whole batch with HTTP 400** and does **not** update `expected_position`. This is a non-retryable client error: endless 5xx retry would poison the send loop and block later logs. The client must fail-stop (see §4.6 / §10); fixing the mapping or the offending record is an operational concern.
- **Unmapped columns**: Destination columns not in the mapping may be set from server metadata (e.g., `received_at` = now) or left NULL.
- **Timestamp parsing**:
  - `timestamp64_us` interprets numeric input as Unix microseconds in UTC.
  - `timestamp64` interprets timezone-less input as UTC.
  - `timestamp64` preserves the instant encoded by timezone-aware input; persisted typed timestamp values are stored in UTC.

### 5.6 Parsed Fields from Client (Optional)

When client-side parsing is enabled, records may include `parsed` fields extracted from `message` and/or `systemd_unit` using named capture groups.

- The server does not execute regex parsing.
- The server treats `parsed` as optional input data.
- Field mapping may reference parsed groups using `parsed.<GROUP_NAME>`.

### 5.7 Position Storage

- **Backend**: Persistent store keyed by `client_id`. In the current design, this is a dedicated YDB table or an in-memory store for tests/local development.
- **Value**: `expected_position` string.
- **Update**: Update only after successful record write. If the record write fails, do not modify the stored position.
- **Retention**: Consider TTL or cleanup for inactive clients.

### 5.8 Database Extraction Tool

Loglugger must include a dedicated extraction tool that reads records from the target YDB table and writes them to a set of output files.

- **Goal**: Export historical records from database storage into filesystem files for transfer or offline analysis.
- **Source**: The tool reads from the configured target logs table (same schema family used by server writes).
- **Output files**: Extraction output is split into a series of files; when the current file reaches the configured size limit, the tool switches to the next file.

#### 5.8.1 Query and Filters

- **Mandatory filter**: Time interval filter is required for every extraction run.
- **Time semantics**: The interval is applied to the table timestamp field used for event time (configured column, typically `ts_orig`), with inclusive lower bound and exclusive upper bound.
- **Optional field-list filters**: The tool supports filtering by a list of values in a specified table field, for example `dbname`, `level`, `service`, `unit`, `hostname`.
- **SQL construction**: Filters are applied by adding parameters to the `SELECT` statement used to fetch records.
  - Time interval parameters are always added.
  - Field-list filters are added only when provided.

#### 5.8.2 File Format and Escaping

- **Format**: Output format is tab-separated values (TSV).
- **Text escaping**: During extraction, text field values are escaped so that TAB/CR/LF do not break TSV structure. Before writing a cell, replace:
  - TAB (`U+0009`) with the two-character sequence `\t`
  - LF (`U+000A`) with the two-character sequence `\n`
  - CR (`U+000D`) with the two-character sequence `\r`
- **Row layout**: Each selected record is written as one TSV line with `\n` line terminator. Columns are separated by a literal TAB.

#### 5.8.3 Compression and File Rotation

- **Compression**: The tool supports optional zstd compression for output files.
- **Rotation unit**: File rotation uses output-file size limit:
  - for uncompressed output, measured on plain TSV bytes written;
  - for compressed output, measured on compressed bytes written.
- **Default size limits**:
  - Uncompressed output: `200 MiB`
  - Compressed output (zstd enabled): `10 MiB`
- **Output naming**: Files are named `<output-prefix>_<NNNNNN>.tsv` (or `.tsv.zst` when compression is enabled).
- **Prefix cleanup before write**: Before each extraction attempt the tool deletes existing files in the output directory that match that naming pattern for the configured `output-prefix`. This makes a full attempt restart-safe (no duplicate rows after a retry) and allows re-invoking the tool with the same parameters after a failed run. Other files in the directory are left untouched. Individual output files are still created with `O_EXCL` so concurrent writers cannot clobber the same path.
- **Retries**: Automatic YDB SDK retries are disabled around the file-writing scan callback (the callback is not idempotent). The tool instead performs an explicit outer retry loop for transient YDB failures: log the failure, delete matching prefix files again, and restart the scan from the beginning. Non-retryable errors and exhausted attempts fail the process.

#### 5.8.4 Database connection specification

The tool should support reading the database connection parameters from the configuration files for the server component.

#### 5.8.5 Filtering parameters specification

The tool should allow to specify the extraction filters through the command line parameters.

Time interval parameters are required, other filters are optional.

---

## 6. Data Models

### 6.1 Log Record (Client → Server)

Each record contains raw `message`. Parsing results may also be sent by client in `parsed`.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| message | string | Yes | Raw log message from journald |
| parsed | map[string]string | No | Named groups extracted by optional client-side regex parsing |
| seqno | int64 | No | Monotonic client-side sequence number; Loglugger client always sends it and starts from startup epoch milliseconds |
| priority | int | No | Syslog priority (0–7) |
| syslog_identifier | string | No | Syslog identifier |
| systemd_unit | string | No | systemd unit name |
| realtime_ts | int64 | No | Microseconds since epoch |
| fields | map[string]string | No | Additional journal fields |

### 6.2 Position Format

- **Recommendation**: Use journald cursor string when available.
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
| server_url | string | — | Single server base URL (comma-separated values are allowed) |
| client_id | string | hostname | Client identifier; must be unique per client instance (not enforced by server; see §5.3.1) |
| service_mask | string | "" | Filter mask for `_SYSTEMD_UNIT` (empty = no filter) |
| journal_recovery | bool | false | Enable best-effort recovery from journal corruption (`EBADMSG` / "bad message"); may skip records and therefore may lose data |
| message_regex | string | "" | Regex with named groups for client-side `message` parsing |
| systemd_unit_regex | string | "" | Regex with named groups for client-side `systemd_unit` parsing |
| message_regex_no_match | string | send_raw | Client behavior when message regex does not match: `send_raw` or `skip` |
| multiline_timeout | duration | 1s | Multiline merge timeout; used only when `message_regex` is set |
| multiline_max_messages | int | 1000 | Max number of source messages merged into one output message; used only when `message_regex` is set |
| batch_size | int | 50000 | Max records per batch (also constrained by the fixed 15 MiB uncompressed JSON limit per request) |
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
| max_compressed_body_bytes | int | 16777216 | Max raw HTTP body size before decoding `Content-Encoding` (16 MiB) |
| max_decompressed_body_bytes | int | 33554432 | Max JSON payload size after decompression (32 MiB) |
| read_header_timeout | duration | 10s | TLS handshake + request header read timeout |
| read_timeout | duration | 60s | Full request read timeout (including body) |
| write_timeout | duration | 60s | Handler processing + response write timeout |
| idle_timeout | duration | 120s | Keep-alive idle connection timeout |
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
| Journal rotation / vacuumed cursor | If `sd_journal_seek_cursor` + `sd_journal_test_cursor` cannot confirm the server-provided cursor, treat it as invalid: send reset and restart from head (do not resume from the nearest neighbor) | Accept with reset; update expected position |
| Journal corruption (`EBADMSG`) with recovery disabled | Log corruption, mention recovery option, and stop | Position stored on server remains unchanged |
| Journal corruption (`EBADMSG`) with recovery enabled | Warn about possible data loss; try reopen/reseek recovery; on success retain any already-buffered unsent records/multiline state, resume with `reset: true`, and send that retained data before relying on post-recovery reads; on failure stop | Accept next successful recovery batch with reset; otherwise position stored on server remains unchanged |
| Persistent journal read I/O errors (non-corruption, e.g. local journald failures) | Retry briefly (~15s) with short delay; if errors persist, flush any buffered batch and stop so a supervisor can restart/alert; do not retry forever while appearing healthy | Position stored on server remains unchanged until a batch is accepted |
| YDB unavailable | Client retries; server returns 5xx | Fail batch; do not update position |
| Field mapping / transform failure for a record in the batch | Do not retry; fail-stop so the flushed batch is not skipped; on restart, resume from the server-stored position (still pointing at the rejected batch until mapping or data is fixed) | Reject batch with HTTP 400; do not update position |
| Non-retryable HTTP 4xx on batch submit (e.g. 400/401/403/404/413/422) | Do not retry; stop the process after logging the error so the already-flushed batch is not skipped; on restart, resume from the server-stored position | Reject batch; do not update position |
| HTTP `409` position mismatch, but journal reseek to `expected_position` and fallback seek to head both fail | Fail-stop; do not discard buffers and continue from an unrepositioned stream; on restart, fetch server position and resume or reset | Return `409` with `expected_position`; position unchanged by the rejected batch |
| Single journal record whose JSON exceeds the 15 MiB request limit | Fail-stop with an error; do not send an oversized request and do not skip the record silently | N/A (no request is sent) |
| Duplicate batch (retry) | Client may retry same batch | Idempotent BulkUpsert; position already updated—reject with 409 if current_position no longer matches |
| Duplicate / shared `client_id` (misconfiguration or concurrent senders) | Not supported; competing senders will observe intermittent `409`, resets, and possible data duplication or gaps | No uniqueness enforcement; position races and write-before-409 outcomes as described in §5.3.1 are expected |

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
      "seqno": 1742203200123,
      "priority": 6,
      "syslog_identifier": "nginx",
      "systemd_unit": "nginx.service",
      "realtime_ts": 1710345600000000,
      "fields": {}
    }
  ]
}
```
