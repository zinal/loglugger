## YDBD Example for Loglugger

This folder contains a complete `ydbd`-oriented Loglugger example:

- `target_table.sql` - target YDB table schema.
- `field_mapping.yaml` - mapping from incoming records to YDB columns.
- `loglugger-server.yaml` - server destination/TLS settings.
- `ydb-loglugger-server.service` - systemd startup unit for the server.
- `ydb-loglugger-client.service` - systemd startup unit for the client.
- `ydb-loglugger-client.env` - example variables for the systemd unit.

### Mapping Notes

- parser regex is configured on server (`loglugger-server.yaml`, `message_regex`):
  `^(?P<P_DTTM>[^ ]*) :(?P<P_SERVICE>[^ ]*) (?P<P_LEVEL>[^ ]*): (?P<P_MESSAGE>.*).*$`
- fallback `level=unknown` when parser fields are absent.
- source is systemd journald records filtered by unit mask.
- `dbname` is configured as a default value in mapping and can be customized per environment.
