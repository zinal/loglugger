# Minimal Loglugger Ansible Playbook

This example installs Loglugger with two roles:

- `server` installs and starts `loglugger-server`
- `client` installs and starts `loglugger-client`

By default, both roles use YDB certificates from `/opt/ydb/certs`.

## Files

- `playbook.yml` - main playbook with `server` and `client` plays
- `inventory.example.ini` - sample inventory
- `roles/server` - server role (binary, config, systemd unit)
- `roles/client` - client role (binary, config, systemd unit)

## Prerequisites

1. Build binaries in this repository:

```bash
./build.sh
```

2. Ensure target hosts already have certificates in `/opt/ydb/certs`:

- `/opt/ydb/certs/ca.crt`
- `/opt/ydb/certs/node.crt`
- `/opt/ydb/certs/node.key`

## Usage

Copy and adjust the inventory:

```bash
cp inventory.example.ini inventory.ini
```

Run the playbook from this directory:

```bash
ansible-playbook -i inventory.ini playbook.yml
```

## Configure YDB and ports

The easiest way is to define variables in `inventory.ini` under `[all:vars]`.

Example:

```ini
[loglugger_server]
server-1 ansible_host=192.168.10.10
server-2 ansible_host=192.168.10.11

[loglugger_client]
client-1 ansible_host=192.168.10.21
client-2 ansible_host=192.168.10.22

[all:vars]
ansible_user=ubuntu
ansible_become=true

# Loglugger server listen port (server config listen_addr)
loglugger_server_listen_addr=:28443

# Client-side server URL generation from loglugger_server inventory group
loglugger_client_server_scheme=https
loglugger_client_server_port=28443

# YDB connection settings used by the server role
loglugger_server_ydb_endpoint=grpcs://ydb.example.internal:2135
loglugger_server_ydb_database=/Root/logdb
loglugger_server_ydb_table=ydblogs
loglugger_server_ydb_auth_mode=static
loglugger_server_ydb_auth_login=ydb_user
loglugger_server_ydb_auth_password=change_me
# optional
# loglugger_server_ydb_open_timeout=20s
# loglugger_server_ydb_ca_path=/opt/ydb/certs/ca.crt
```

How this works:

- `loglugger_server_listen_addr` controls the server bind port.
- clients build `server_urls` from hosts in `loglugger_server` using `loglugger_client_server_scheme` + host + `loglugger_client_server_port`.
- YDB connection parameters come from `loglugger_server_ydb_*` variables.
- when `loglugger_server_ydb_auth_mode=static`, you must define both `loglugger_server_ydb_auth_login` and `loglugger_server_ydb_auth_password`; the role validates this with an Ansible assert.
- if you want a fixed server list instead of inventory-derived URLs, set `loglugger_client_server_urls` (or `loglugger_client_server_urls_override` in `playbook.yml`).

YDB table mapping note:

- the Ansible server role installs `field_mapping.yaml` compatible with `examples/ydbd/target_table.sql`
- required YDB columns mapped by default: `ts_log`, `seqno`, `hostname`, `message_hash`
- if you use a different YDB schema, override `loglugger_server_field_mapping_file` and provide your own mapping file

## Common overrides

Set these in inventory/group vars/host vars as needed:

- `loglugger_local_bin_dir` (default: `{{ playbook_dir }}/../../bin`) - local source directory for built binaries
- `loglugger_prefix` (default: `/opt/ydb/loglugger`) - install prefix on target hosts
- `loglugger_client_server_urls` - explicit client server URL list (e.g. `["https://s1:27312","https://s2:27312"]`)
- `loglugger_server_listen_addr` - Loglugger server listen address (e.g. `:27312`)
- `loglugger_client_server_scheme`, `loglugger_client_server_port` - default client URL generation controls
- `loglugger_server_ydb_endpoint`, `loglugger_server_ydb_database`, `loglugger_server_ydb_table`
- `loglugger_server_ydb_auth_mode` (`anonymous` or `static`)
- `loglugger_server_ydb_auth_login`, `loglugger_server_ydb_auth_password` (required for `static`)

Client server URL behavior:

- by default, the client role builds `server_urls` from inventory hosts in `loglugger_server`
- each URL uses `https://<ansible_host>:27312` (or inventory hostname when `ansible_host` is not set)
- you can override in `playbook.yml` via `loglugger_client_server_urls_override`

## Certificate defaults

The roles default to this certificate directory:

```yaml
loglugger_cert_dir: /opt/ydb/certs
```

Derived defaults:

- server TLS: `node.crt`, `node.key`, `ca.crt`
- client TLS: `node.crt`, `node.key`, `ca.crt`
- YDB CA: `ca.crt`
