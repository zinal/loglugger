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
./bin/server \
  -listen :8443 \
  -tls-cert-file certs/server.crt \
  -tls-key-file certs/server.key \
  -tls-ca-file certs/ca.crt \
  -field-mapping-file examples/mappings/basic.yaml
```

Useful server flags:

- `-writer mock|ydb` chooses the output backend.
- `-position-store memory|file` chooses in-memory or file-backed position tracking.
- `-field-mapping-file mapping.yaml` loads YAML/JSON source-to-destination mappings.
- `-tls-client-subject-cn`, `-tls-client-subject-o`, `-tls-client-subject-ou` restrict accepted client certificate subjects.

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

- `-position-file state/cursor.txt` stores the last accepted position on disk.
- `-service-mask nginx.service` uses exact systemd unit matching.
- `-service-mask 'nginx*.service'` uses glob matching.
- `-service-mask 'regex:^nginx-(api|worker)\\.service$'` uses regex matching.
- `-message-regex` and `-message-regex-no-match send_raw|skip` control MESSAGE parsing.
- `-tls-ca-path` and `-tls-use-system-pool` control the client trust store.

### Example Mapping Files

- `examples/mappings/basic.yaml` is a readable starter mapping for local mock or file-backed runs.
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
./bin/server \
  -listen :8443 \
  -tls-cert-file certs/server.crt \
  -tls-key-file certs/server.key \
  -tls-ca-file certs/ca.crt \
  -tls-client-subject-cn client-local \
  -tls-client-subject-o dev \
  -tls-client-subject-ou local \
  -field-mapping-file examples/mappings/basic.yaml \
  -position-store file \
  -position-file state/server-positions.json
```

For a local mock-backed end-to-end run:

```bash
./bin/server \
  -listen :8443 \
  -tls-cert-file certs/server.crt \
  -tls-key-file certs/server.key \
  -tls-ca-file certs/ca.crt \
  -field-mapping-file examples/mappings/basic.yaml
```

```bash
./bin/client \
  -server https://localhost:8443 \
  -client-id myhost \
  -tls-ca-file certs/ca.crt \
  -tls-cert-file certs/client.crt \
  -tls-key-file certs/client.key \
  -position-file state/client-position.txt \
  -message-regex '^(?P<P_DTTM>[^ ]*) :(?P<P_SERVICE>[^ ]*) (?P<P_LEVEL>[^ ]*): (?P<P_MESSAGE>.*)$'
```

## Tests

```bash
go test ./...
```

Includes unit tests for parser, batcher, models, handler, and a functional test that exercises the full client-server flow.

## YDB Integration

The server supports `-writer ydb` and uses `github.com/ydb-platform/ydb-go-sdk/v3` for `BulkUpsert`. Supply `-ydb-endpoint`, `-ydb-database`, and `-ydb-table` when enabling the YDB backend. `MockWriter` remains available for tests and local dry runs.

Example YDB run:

```bash
./bin/server \
  -listen :8443 \
  -writer ydb \
  -ydb-endpoint grpcs://localhost:2135 \
  -ydb-database /local \
  -ydb-table logs \
  -tls-cert-file certs/server.crt \
  -tls-key-file certs/server.key \
  -tls-ca-file certs/ca.crt \
  -field-mapping-file examples/mappings/ydb.json \
  -position-store file \
  -position-file state/server-positions.json
```
