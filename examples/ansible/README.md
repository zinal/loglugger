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

## Common overrides

Set these in inventory/group vars/host vars as needed:

- `loglugger_local_bin_dir` (default: `{{ playbook_dir }}/../../bin`) - local source directory for built binaries
- `loglugger_prefix` (default: `/opt/ydb/loglugger`) - install prefix on target hosts
- `loglugger_client_server_urls` - explicit client server URL list (e.g. `["https://s1:27312","https://s2:27312"]`)
- `loglugger_server_ydb_endpoint`, `loglugger_server_ydb_database`, `loglugger_server_ydb_table`

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
