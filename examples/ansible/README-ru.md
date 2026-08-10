# Минимальный Ansible-плейбук Loglugger

English version: [README.md](README.md).

Этот пример устанавливает Loglugger с помощью двух ролей:

- `server` устанавливает и запускает `loglugger-server`
- `client` устанавливает и запускает `loglugger-client`

По умолчанию обе роли используют сертификаты YDB из `/opt/ydb/certs`.

## Файлы

- `playbook.yml` — основной плейбук с plays `server` и `client`
- `inventory.example.ini` — пример inventory
- `target_table.sql` — схема YDB для таблицы логов и хранилища позиций (аналог `examples/ydbd/target_table.sql`)
- `roles/server` — роль сервера (бинарник, конфиг, field mapping, systemd-юнит)
- `roles/client` — роль клиента (бинарник, конфиг, systemd-юнит)

## Предварительные требования

1. Соберите бинарники в этом репозитории:

```bash
./build.sh
```

Если целевые хосты старше машины сборки (например Ubuntu 20.04 с glibc 2.31) и `loglugger-client` падает с ошибкой `GLIBC_2.32' not found`, перед запуском плейбука пересоберите бинарники через `./build-docker.sh` (или `LOGLUGGER_PORTABLE=1 ./build.sh`).

2. Убедитесь, что на целевых хостах уже есть сертификаты в `/opt/ydb/certs`:

- `/opt/ydb/certs/ca.crt`
- `/opt/ydb/certs/node.crt`
- `/opt/ydb/certs/node.key`

3. Создайте таблицы YDB до запуска сервера (см. `target_table.sql`):

- таблица логов (по умолчанию `ydblogs`)
- таблица позиций (по умолчанию `loglugger_positions`)

## Использование

Скопируйте и отредактируйте inventory:

```bash
cp inventory.example.ini inventory.ini
```

Запустите плейбук из этого каталога:

```bash
ansible-playbook -i inventory.ini -f 50 playbook.yml
```

## Настройка YDB и портов

Проще всего задать переменные в `inventory.ini` в секции `[all:vars]`.

Пример:

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

# Порт прослушивания сервера Loglugger (listen_addr в конфиге сервера)
loglugger_server_listen_addr=:28443

# Генерация server_urls на клиенте из группы inventory loglugger_server
loglugger_client_server_scheme=https
loglugger_client_server_port=28443

# Параметры подключения к YDB для роли server
loglugger_server_ydb_endpoint=grpcs://ydb.example.internal:2135
loglugger_server_ydb_database=/Root/logdb
loglugger_server_ydb_table=ydblogs
loglugger_server_ydb_auth_mode=static
loglugger_server_ydb_auth_login=ydb_user
loglugger_server_ydb_auth_password=change_me
# необязательно
# loglugger_server_ydb_open_timeout=20s
# loglugger_server_ydb_ca_path=/opt/ydb/certs/ca.crt
```

Как это работает:

- `loglugger_server_listen_addr` задаёт порт привязки сервера.
- клиенты собирают `server_urls` из хостов группы `loglugger_server` по схеме `loglugger_client_server_scheme` + host + `loglugger_client_server_port`.
- параметры подключения к YDB берутся из переменных `loglugger_server_ydb_*`.
- при `loglugger_server_ydb_auth_mode=static` нужно задать и `loglugger_server_ydb_auth_login`, и `loglugger_server_ydb_auth_password`; роль проверяет это через Ansible assert.
- если нужен фиксированный список серверов вместо URL из inventory, задайте `loglugger_client_server_urls` (или `loglugger_client_server_urls_override` в `playbook.yml`).

Замечание по сопоставлению таблицы YDB:

- роль server устанавливает `field_mapping.yaml`, совместимый с `target_table.sql` / `examples/ydbd/target_table.sql`
- обязательные колонки YDB, сопоставляемые по умолчанию: `ts_log`, `seqno`, `hostname`, `message_hash`
- если используется другая схема YDB, переопределите `loglugger_server_field_mapping_file` и укажите свой файл сопоставления

## Типовые переопределения

Задавайте при необходимости в inventory / group_vars / host_vars:

- `loglugger_local_bin_dir` (по умолчанию: `{{ playbook_dir }}/../../bin`) — локальный каталог со собранными бинарниками
- `loglugger_prefix` (по умолчанию: `/opt/ydb/loglugger`) — префикс установки на целевых хостах
- `loglugger_client_server_urls` — явный список URL серверов для клиента (например, `["https://s1:27312","https://s2:27312"]`)
- `loglugger_server_listen_addr` — адрес прослушивания сервера Loglugger (например, `:27312`)
- `loglugger_client_server_scheme`, `loglugger_client_server_port` — параметры генерации URL клиента по умолчанию
- `loglugger_client_service_mask` (по умолчанию: `regex:^ydbd-.*\\.service$`)
- `loglugger_client_message_regex`, `loglugger_client_systemd_unit_regex`
- `loglugger_client_journal_recovery` (по умолчанию: `true`)
- `loglugger_server_ydb_endpoint`, `loglugger_server_ydb_database`, `loglugger_server_ydb_table`
- `loglugger_server_ydb_auth_mode` (`anonymous` или `static`; шаблон Ansible не подключает `service-account-key` / `metadata`)
- `loglugger_server_ydb_auth_login`, `loglugger_server_ydb_auth_password` (обязательны для `static`)
- `loglugger_server_ydb_open_timeout`, `loglugger_server_ydb_ca_path`
- `loglugger_server_position_table` (по умолчанию: `loglugger_positions`)
- `loglugger_cert_dir`, `loglugger_server_tls_*`, `loglugger_client_tls_*` — пути к сертификатам

Поведение URL серверов на клиенте:

- по умолчанию роль client собирает `server_urls` из хостов inventory-группы `loglugger_server`
- каждый URL имеет вид `<scheme>://<ansible_host>:<port>` (по умолчанию: `https`, порт `27312`; если `ansible_host` не задан, используется имя хоста из inventory)
- можно переопределить в `playbook.yml` через `loglugger_client_server_urls_override`

Значения по умолчанию для разбора логов YDBD в роли client:

- `message_regex` извлекает `P_SERVICE`, `P_LEVEL` и `P_MESSAGE` из строк журнала YDBD
- `systemd_unit_regex` извлекает `P_DBNAME` из юнитов БД вида `ydbd-database-a.service`
- юниты хранения вроде `ydbd-storage.service` этому regex не соответствуют, поэтому `dbname` остаётся равным значению по умолчанию из mapping — `-` (у узлов хранения нет имени БД)
- эти значения по умолчанию помогают избежать пустого `msg` и fallback-значений вроде `unknown`/`-` в сопоставляемых колонках YDB

## Сертификаты по умолчанию

Роли по умолчанию используют следующий каталог сертификатов:

```yaml
loglugger_cert_dir: /opt/ydb/certs
```

Производные значения:

- TLS сервера: `node.crt`, `node.key`, `ca.crt`
- TLS клиента: `node.crt`, `node.key`, `ca.crt`
- CA для YDB: `ca.crt`
