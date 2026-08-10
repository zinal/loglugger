## YDBD Example for Loglugger

This folder contains a complete `ydbd`-oriented Loglugger example:

- `target_table.sql` - target YDB table schema.
- `field_mapping.yaml` - mapping from incoming records to YDB columns.
- `loglugger-server.yaml` - server destination/TLS settings.
- `loglugger-client.yaml` - client journal/parsing/TLS settings.
- `ydb-loglugger-server.service` - systemd startup unit for the server.
- `ydb-loglugger-client.service` - systemd startup unit for the client.
- `ydb-loglugger-client.env` - example `EnvironmentFile` for the client unit; install as `/etc/default/ydb-loglugger-client`.

TLS paths in the YAML configs reuse typical YDB node certificates from `/opt/ydb/certs` (`ca.crt`, `node.crt`, `node.key`), same as the Ansible example.

### Extraction Example

Use `loglugger-extractor` to export YDB rows into rotating TSV files:

```bash
./bin/loglugger-extractor \
  -server-config examples/ydbd/loglugger-server.yaml \
  -from 2025-03-13T10:00:00Z \
  -to 2025-03-13T11:00:00Z \
  -filter dbname=local,prod \
  -filter service=ydbd \
  -zstd \
  -output-dir ./out
```

Notes:

- Time filter is required (`-from`, `-to`) and is applied as `[from,to)`.
- Optional `-filter field=v1,v2` flags add `IN` filters to SQL. Filter field names must match target table columns (see `target_table.sql`).
- Rotation defaults: `200MiB` for plain TSV, `10MiB` with `-zstd` (override via `-max-file-size`).
- Matching `<output-prefix>_*.tsv[.zst]` files in `-output-dir` are removed before each attempt, so a failed run can be retried with the same parameters without duplicate rows.

### Mapping Notes

- parser regexes are configured on client (`loglugger-client.yaml`, `message_regex`, `systemd_unit_regex`):
  `^(?:(?P<P_DTTM>[^ ]+)\s+)?:(?P<P_SERVICE>[^ ]+)\s+(?P<P_LEVEL>[^ ]+):\s+(?P<P_MESSAGE>.*)$`
- fallback `level=unknown` when parser fields are absent.
- source is systemd journald records filtered by unit mask.
- `dbname` is configured as a default value in mapping and can be customized per environment.
- `seqno` is mapped as a monotonic client-side ordering field (starts from client startup epoch milliseconds).
