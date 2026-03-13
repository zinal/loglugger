# Loglugger

A two-component system for collecting log records from systemd journald and persisting them to a backend (YDB or mock for testing).

See [SPECIFICATION.md](SPECIFICATION.md) for the formal specification.

## Components

- **Client** (`cmd/client`): Reads from journald, optionally parses messages with regex, batches records, and sends to the server via HTTP.
- **Server** (`cmd/server`): Accepts batches, validates position continuity, maps fields, and writes to a store (MockWriter by default).

## Build

```bash
mkdir -p bin
go build -o bin/server ./cmd/server
go build -o bin/client ./cmd/client
```

The client requires Linux for journald support. On macOS/Windows, the client will fail at startup with "journald is only supported on Linux".

After building, use `./bin/server` and `./bin/client` in the run commands below.

## Run

**Server** (TLS + mTLS required):
```bash
./bin/server -config examples/config/server.yaml
```

Server config file:

- Most server settings are now loaded from YAML/JSON config passed with `-config`.
- See `examples/config/server.yaml` for all supported keys.
- Optional CLI override: `-listen :8443` (overrides `listen_addr` from config).
- The client fetches its startup position from `GET /v1/positions?client_id=...` and does not keep a local position file.

**Client** (Linux only):
```bash
./bin/client \
  -server https://localhost:8443 \
  -client-id myhost \
  -tls-ca-file certs/ca.crt \
  -tls-cert-file certs/client.crt \
  -tls-key-file certs/client.key
```

Useful client flags:

- `-service-mask nginx.service` uses exact systemd unit matching.
- `-service-mask 'nginx*.service'` uses glob matching.
- `-service-mask 'regex:^nginx-(api|worker)\\.service$'` uses regex matching.
- `-server https://a:8443,https://b:8443` configures multiple endpoints; retries rotate through the list.
- `-message-regex` and `-message-regex-no-match send_raw|skip` control MESSAGE parsing.
- `-tls-ca-path` and `-tls-use-system-pool` control the client trust store.
- Client batches are additionally limited to 10 MB of uncompressed log data per request.
- If a single record exceeds 10 MB, it is sent as a single-record request (not dropped).

### Example Mapping Files

- `examples/mappings/basic.yaml` is a readable starter mapping for local mock or YDB-backed runs.
- `examples/mappings/ydb.json` is the same idea in JSON and is convenient when wiring the YDB writer.

### Local mTLS Setup

The following `openssl` commands generate a local CA plus server/client certificates that work with the sample mTLS server.

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

If you want the server to enforce client subject checks as well as CA trust, start it with:

```bash
./bin/server -config examples/config/server.yaml
```

For a local mock-backed end-to-end run:

```bash
./bin/server -config examples/config/server.yaml
```

```bash
./bin/client \
  -server https://localhost:8443 \
  -client-id myhost \
  -tls-ca-file certs/ca.crt \
  -tls-cert-file certs/client.crt \
  -tls-key-file certs/client.key \
  -message-regex '^(?P<P_DTTM>[^ ]*) :(?P<P_SERVICE>[^ ]*) (?P<P_LEVEL>[^ ]*): (?P<P_MESSAGE>.*)$'
```

## Tests

```bash
go test ./...
```

Includes unit tests for parser, batcher, models, handler, and a functional test that exercises the full client-server flow.

## YDB Integration

The server supports `writer_backend: ydb` and uses `github.com/ydb-platform/ydb-go-sdk/v3` for `BulkUpsert`. Supply `ydb_endpoint`, `ydb_database`, and `ydb_table` in the config when enabling the YDB backend. The position store can also use YDB via `position_store: ydb` and `position_table`. `MockWriter` remains available for tests and local dry runs.

Supported YDB auth modes:

- `anonymous` (default)
- `static` via `ydb_auth_login` + `ydb_auth_password`
- `service-account-key` via `ydb_auth_sa_key_file`
- `metadata` (instance metadata credentials, optional `ydb_auth_metadata_url`)

Create the YDB table for the position store before starting the server with `position_store: ydb`:

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
# position_store: ydb
# ydb_endpoint: grpcs://localhost:2135
# ydb_database: /local
# ydb_table: logs
# field_mapping_file: examples/mappings/ydb.json
./bin/server -config examples/config/server.yaml
```
