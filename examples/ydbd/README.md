## YDBD Example for Loglugger

This folder contains a complete `ydbd`-oriented Loglugger example:

- `target_table.sql` - target YDB table schema.
- `field_mapping.yaml` - mapping from incoming records to YDB columns.
- `server.yaml` - server destination/TLS settings.
- `loglugger-server.service` - systemd startup unit for the server.
- `loglugger-client.service` - systemd startup unit for the client.
- `loglugger-client.env` - example variables for the systemd unit.

### Mapping Notes

- parser regex is configured on server (`server.yaml`, `message_regex`):
  `^(?P<P_DTTM>[^ ]*) :(?P<P_SERVICE>[^ ]*) (?P<P_LEVEL>[^ ]*): (?P<P_MESSAGE>.*).*$`
- fallback `level=unknown` when parser fields are absent.
- source is systemd journald records filtered by unit mask.
- `dbname` is configured as a default value in mapping and can be customized per environment.
