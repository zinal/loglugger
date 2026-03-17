## YDBD Example for Loglugger

This folder contains a complete `ydbd`-oriented Loglugger example:

- `target_table.sql` - target YDB table schema.
- `field_mapping.yaml` - mapping from incoming records to YDB columns.
- `loglugger-server.yaml` - server destination/TLS settings.
- `loglugger-client.yaml` - client journal/parsing/TLS settings.
- `ydb-loglugger-server.service` - systemd startup unit for the server.
- `ydb-loglugger-client.service` - systemd startup unit for the client.
- `ydb-loglugger-client.env` - example variables for the systemd unit.

### Mapping Notes

- parser regexes are configured on client (`loglugger-client.yaml`, `message_regex`, `systemd_unit_regex`):
  `^(?:(?P<P_DTTM>[^ ]+)\s+)?:(?P<P_SERVICE>[^ ]+)\s+(?P<P_LEVEL>[^ ]+):\s+(?P<P_MESSAGE>.*)$`
- fallback `level=unknown` when parser fields are absent.
- source is systemd journald records filtered by unit mask.
- `dbname` is configured as a default value in mapping and can be customized per environment.
- `seqno` is mapped as a monotonic client-side ordering field (starts from client startup epoch milliseconds).
