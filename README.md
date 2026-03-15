# Loglugger

A two-component system for collecting records from systemd journald and storing them in a target backend (YDB or a mock backend for testing).

See [SPECIFICATION.md](SPECIFICATION.md) for the formal specification.

## Components

- **Client** (`cmd/client`): Reads records from journald, optionally parses messages with a regular expression, groups them into batches, and sends them to the server over HTTP.
- **Server** (`cmd/server`): Accepts batches, validates position continuity, maps fields, and writes them to storage (`MockWriter` by default).

## Build

```bash
sudo apt-get install -y libsystemd-dev  # or your operating system equivalent
./build.sh
```

The client requires Linux for journald support. On macOS and Windows, the client fails at startup with "journald is only supported on Linux".

After building, use `./bin/loglugger-server` and `./bin/loglugger-client` in the run commands below.

## Run

**Server** (TLS + mTLS required):
```bash
./bin/loglugger-server -config examples/config/server.yaml
```

Server configuration:

- Most server settings are loaded from a YAML/JSON file passed with `-config`.
- See `examples/config/server.yaml` for all supported keys.
- Optional CLI override: `-listen :27312` (overrides `listen_addr` from the configuration file).
- The client fetches its startup position from `GET /v1/positions?client_id=...` and does not keep a local position file.
- Request-size protection is configurable:
  - `max_compressed_body_bytes`: maximum raw HTTP request body size before decoding `Content-Encoding`.
  - `max_decompressed_body_bytes`: maximum decoded JSON payload size after decompression.
  - Defaults: `8388608` (8 MiB) and `33554432` (32 MiB).

**Client** (Linux only):
```bash
./bin/loglugger-client \
  -server https://localhost:27312 \
  -client-id myhost \
  -tls-ca-file certs/ca.crt \
  -tls-cert-file certs/client.crt \
  -tls-key-file certs/client.key
```

Useful client flags:

- `-service-mask nginx.service` uses exact systemd unit matching.
- `-service-mask 'nginx*.service'` uses glob matching.
- `-service-mask 'regex:^nginx-(api|worker)\\.service$'` uses regex matching.
- `-server https://a:27312,https://b:27312` configures multiple endpoints; the client keeps using the current endpoint while requests succeed and switches to the next one only after a transient failure (`5xx` or network error).
- `-tls-ca-file` and `-tls-use-system-pool` control the client trust store.
- Client batches are additionally limited to 10 MB of uncompressed log data per request.
- If a single record exceeds 10 MB, it is sent as a single-record request and is not dropped.

### Example Mapping Files

- `examples/mappings/basic.yaml` is a baseline mapping file for local runs with the mock backend or YDB.
- `examples/mappings/ydb.json` follows the same approach in JSON and is convenient when configuring the YDB writer.
- The mapper supports computed source fields:
  - `log_timestamp_us`: microsecond record timestamp that maps to YDB `Timestamp64` via `transform: timestamp64_us`.
  - `message_cityhash64`: `CityHash64` over the full record payload, typically mapped to `message_hash` (`Uint64`) to preserve uniqueness.
- YDB-specific examples for schema and mapping are provided in `examples/ydbd/field_mapping.yaml` and `examples/ydbd/target_table.sql`.

### Local mTLS Setup

The following `openssl` commands generate a local CA, together with server and client certificates, for the sample mTLS setup.

```bash
mkdir -p certs

openssl ecparam -name prime256v1 -genkey -noout -out certs/ca.key
openssl req -x509 -new -key certs/ca.key -sha256 -days 3650 \
  -subj "/CN=loglugger-local-ca" \
  -out certs/ca.crt

openssl ecparam -name prime256v1 -genkey -noout -out certs/server.key
openssl req -new -key certs/server.key \
  -subj "/CN=localhost" \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" \
  -out certs/server.csr
openssl x509 -req -in certs/server.csr \
  -CA certs/ca.crt -CAkey certs/ca.key -CAcreateserial \
  -out certs/server.crt -days 825 -sha256 \
  -copy_extensions copy

openssl ecparam -name prime256v1 -genkey -noout -out certs/client.key
openssl req -new -key certs/client.key \
  -subj "/CN=client-local/O=dev/OU=local" \
  -out certs/client.csr
openssl x509 -req -in certs/client.csr \
  -CA certs/ca.crt -CAkey certs/ca.key -CAcreateserial \
  -out certs/client.crt -days 825 -sha256
```

If you need the server to enforce client subject checks in addition to CA trust, start it with:

```bash
./bin/loglugger-server -config examples/config/server.yaml
```

When using `tls_client_subject_cn`, `tls_client_subject_o`, or `tls_client_subject_ou` with entries like `regex:<pattern>`:

- Regular expressions are compiled and validated at server startup.
- Invalid patterns now fail fast with a configuration error instead of being ignored during the TLS handshake.

For a local end-to-end run with the mock backend:

```bash
./bin/loglugger-server -config examples/config/server.yaml
```

```bash
./bin/loglugger-client \
  -server https://localhost:27312 \
  -client-id myhost \
  -tls-ca-file certs/ca.crt \
  -tls-cert-file certs/client.crt \
  -tls-key-file certs/client.key
```

## Tests

```bash
go test ./...
```

The test suite includes unit tests for `parser`, `batcher`, `models`, and `handler`, together with a functional test that exercises the full client-server flow.

## YDB Integration

The server supports `writer_backend: ydb` and uses `github.com/ydb-platform/ydb-go-sdk/v3` for `BulkUpsert`. When enabling the YDB backend, set `ydb_endpoint`, `ydb_database`, and `ydb_table` in the configuration file. The position storage backend is selected automatically from `writer_backend` (`mock` -> in-memory positions, `ydb` -> YDB-backed positions via `position_table`). `MockWriter` remains available for tests and local runs.

Supported YDB auth modes:

- `anonymous` (default)
- `static` via `ydb_auth_login` + `ydb_auth_password`
- `service-account-key` via `ydb_auth_sa_key_file`
- `metadata` (instance metadata service credentials, optional `ydb_auth_metadata_url`)

Optional path to a YDB TLS CA certificate:

- `ydb_ca_path` points to a PEM file with CA certificates used to validate YDB TLS.

Create the YDB table for the position store before starting the server with `writer_backend: ydb`:

```sql
CREATE TABLE `loglugger_positions` (
  client_id Utf8 NOT NULL,
  expected_position Utf8 NOT NULL,
  PRIMARY KEY (client_id)
);
```

Example YDB run:

```bash
# copy examples/config/server.yaml and set:
# writer_backend: ydb
# ydb_endpoint: grpcs://localhost:2135
# ydb_database: /local
# ydb_table: logs
# field_mapping_file: examples/mappings/ydb.json
./bin/loglugger-server -config examples/config/server.yaml
```

Notes on YDB schema and mapping:

- In `examples/ydbd/target_table.sql`, `log_timestamp_us` and `ts_orig` use `Timestamp64`.
- In `examples/ydbd/field_mapping.yaml`, use:
  - `transform: timestamp64_us` for microsecond epoch values (e.g., `log_timestamp_us`)
  - `transform: timestamp64` for parsed datetime strings (e.g., `parsed.P_DTTM` -> `ts_orig`)
- `convert_time_to_local_tz` (server config, default `false`) changes how timezone-less `timestamp64` values are parsed:
  - `false`: interpret as UTC
  - `true`: interpret in the OS local timezone before saving (useful, but risky when timezone configuration differs across hosts)
- `message_regex` and `message_regex_no_match` are now configured on the server in YAML/JSON configuration.
