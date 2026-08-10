# Minimal Loglugger Ansible Playbook

Russian version: [README-ru.md](README-ru.md).

This example installs Loglugger with two roles:

- `server` installs and starts `loglugger-server`
- `client` installs and starts `loglugger-client`

By default, both roles use YDB certificates from `/opt/ydb/certs`.
The server runs as the dedicated `loglugger` system user. Its main
configuration, which can contain a YDB password, is readable only by that user.

## Files

- `playbook.yml` - main playbook with `server` and `client` plays
- `inventory.example.ini` - sample inventory
- `target_table.sql` - YDB schema for the log table and position store (same as `examples/ydbd/target_table.sql`)
- `roles/server` - server role (binary, config, field mapping, systemd unit)
- `roles/client` - client role (binary, config, systemd unit)

## Prerequisites

1. Build binaries in this repository:

```bash
./build.sh
```

If target hosts are older than the build machine (for example Ubuntu 20.04 with glibc 2.31) and `loglugger-client` fails with `GLIBC_2.32' not found`, rebuild with `./build-docker.sh` (or `LOGLUGGER_PORTABLE=1 ./build.sh`) before running the playbook.

For a ready-to-ship tree (portable binaries + this Ansible example), use `./build-ditto.sh` and run the playbook from the unpacked archive under `examples/ansible`.

2. Ensure target hosts already have certificates in `/opt/ydb/certs`:

- `/opt/ydb/certs/ca.crt`
- `/opt/ydb/certs/node.crt`
- `/opt/ydb/certs/node.key`

3. Create the YDB tables before starting the server (see `target_table.sql`):

- log table (`ydblogs` by default)
- position table (`loglugger_positions` by default)

## Usage

Copy and adjust the inventory:

```bash
cp inventory.example.ini inventory.ini
```

For static YDB authentication, create and encrypt the password variables file:

```bash
cp group_vars/loglugger_server/vault.yml.example \
  group_vars/loglugger_server/vault.yml
# Replace change_me in vault.yml, then encrypt the file:
ansible-vault encrypt group_vars/loglugger_server/vault.yml
```

The local `vault.yml` path is ignored by Git. Do not put the password directly
in `inventory.ini`, command-line extra vars, or an unencrypted variables file.

Run the playbook from this directory:

```bash
ansible-playbook -i inventory.ini -f 50 playbook.yml --ask-vault-pass
```

`--vault-password-file` can be used instead of the interactive prompt. When
using anonymous YDB authentication and no Vault file, omit the Vault option.

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
# The password comes from the encrypted group_vars/loglugger_server/vault.yml.
# optional
# loglugger_server_ydb_open_timeout=20s
# loglugger_server_ydb_ca_path=/opt/ydb/certs/ca.crt
```

How this works:

- `loglugger_server_listen_addr` controls the server bind port.
- clients build `server_urls` from hosts in `loglugger_server` using `loglugger_client_server_scheme` + host + `loglugger_client_server_port`.
- YDB connection parameters come from `loglugger_server_ydb_*` variables.
- when `loglugger_server_ydb_auth_mode=static`, the login must be set and `vault_loglugger_server_ydb_auth_password` must come from the encrypted Vault file; the role validates the resulting credentials without logging them.
- if you want a fixed server list instead of inventory-derived URLs, set `loglugger_client_server_urls` (or `loglugger_client_server_urls_override` in `playbook.yml`).

YDB table mapping note:

- the Ansible server role installs `field_mapping.yaml` compatible with `target_table.sql` / `examples/ydbd/target_table.sql`
- required YDB columns mapped by default: `ts_log`, `seqno`, `hostname`, `message_hash`
- if you use a different YDB schema, override `loglugger_server_field_mapping_file` and provide your own mapping file

## Common overrides

Set these in inventory/group vars/host vars as needed:

- `loglugger_local_bin_dir` (default: `{{ playbook_dir }}/../../bin`) - local source directory for built binaries
- `loglugger_prefix` (default: `/opt/ydb/loglugger`) - install prefix on target hosts
- `loglugger_server_user`, `loglugger_server_group` (default: `loglugger`) - dedicated system account used by the server
- `loglugger_server_additional_groups` - existing groups the server account must join to read externally managed TLS private keys or CA files
- `loglugger_client_server_urls` - explicit client server URL list (e.g. `["https://s1:27312","https://s2:27312"]`)
- `loglugger_server_listen_addr` - Loglugger server listen address (e.g. `:27312`)
- `loglugger_client_server_scheme`, `loglugger_client_server_port` - default client URL generation controls
- `loglugger_client_service_mask` (default: `regex:^ydbd-.*\\.service$`)
- `loglugger_client_message_regex`, `loglugger_client_systemd_unit_regex`
- `loglugger_client_journal_recovery` (default: `true`)
- `loglugger_server_ydb_endpoint`, `loglugger_server_ydb_database`, `loglugger_server_ydb_table`
- `loglugger_server_ydb_auth_mode` (`anonymous` or `static`; Ansible template does not wire `service-account-key` / `metadata`)
- `loglugger_server_ydb_auth_login`, `vault_loglugger_server_ydb_auth_password` (required for `static`; keep the latter in Ansible Vault)
- `loglugger_server_ydb_open_timeout`, `loglugger_server_ydb_ca_path`
- `loglugger_server_position_table` (default: `loglugger_positions`)
- `loglugger_cert_dir`, `loglugger_server_tls_*`, `loglugger_client_tls_*` - certificate paths

Client server URL behavior:

- by default, the client role builds `server_urls` from inventory hosts in `loglugger_server`
- each URL uses `<scheme>://<ansible_host>:<port>` (defaults: `https`, port `27312`; inventory hostname is used when `ansible_host` is not set)
- you can override in `playbook.yml` via `loglugger_client_server_urls_override`

YDBD parsing defaults in the client role:

- `message_regex` extracts `P_SERVICE`, `P_LEVEL`, and `P_MESSAGE` from YDBD log lines
- `systemd_unit_regex` extracts `P_DBNAME` from database units like `ydbd-database-a.service`
- storage units such as `ydbd-storage.service` do not match this regex, so `dbname` stays at the mapping default `-` (storage nodes have no database name)
- these defaults prevent empty `msg` and fallback values like `unknown`/`-` in mapped YDB columns

## Certificate defaults

The roles default to this certificate directory:

```yaml
loglugger_cert_dir: /opt/ydb/certs
```

Derived defaults:

- server TLS: `node.crt`, `node.key`, `ca.crt`
- client TLS: `node.crt`, `node.key`, `ca.crt`
- YDB CA: `ca.crt`
