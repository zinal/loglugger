# Loglugger

Двухкомпонентная система для сбора логов из systemd journald и сохранения их в бэкенд (YDB или mock для тестирования).

Формальная спецификация: [SPECIFICATION.md](SPECIFICATION.md).

## Компоненты

- **Клиент** (`cmd/client`): читает данные из journald, при необходимости парсит сообщения регулярным выражением, формирует батчи и отправляет их на сервер по HTTP.
- **Сервер** (`cmd/server`): принимает батчи, проверяет непрерывность позиции, маппит поля и записывает их в хранилище (по умолчанию `MockWriter`).

## Сборка

```bash
sudo apt-get install -y libsystemd-dev  # либо аналог для вашей операционной системы
./build.sh
```

Для работы клиента с journald требуется Linux. На macOS/Windows клиент завершится на старте с ошибкой "journald is only supported on Linux".

После сборки используйте `./bin/server` и `./bin/client` в командах запуска ниже.

## Запуск

**Сервер** (обязательны TLS + mTLS):
```bash
./bin/server -config examples/config/server.yaml
```

Конфигурация сервера:

- Большинство настроек сервера теперь читается из YAML/JSON-файла, переданного через `-config`.
- Пример всех ключей: `examples/config/server.yaml`.
- Опциональный CLI-override: `-listen :27312` (переопределяет `listen_addr` из конфига).
- Клиент получает стартовую позицию из `GET /v1/positions?client_id=...` и не хранит локальный файл позиции.

**Клиент** (только Linux):
```bash
./bin/client \
  -server https://localhost:27312 \
  -client-id myhost \
  -tls-ca-file certs/ca.crt \
  -tls-cert-file certs/client.crt \
  -tls-key-file certs/client.key
```

Полезные флаги клиента:

- `-service-mask nginx.service` использует точное совпадение имени systemd unit.
- `-service-mask 'nginx*.service'` использует glob-сопоставление.
- `-service-mask 'regex:^nginx-(api|worker)\\.service$'` использует регулярное выражение.
- `-server https://a:27312,https://b:27312` задает несколько endpoint'ов; выбор endpoint sticky: клиент использует текущий endpoint, пока запросы успешны, и переключается на следующий только после временной ошибки (`5xx` или сетевая ошибка).
- `-message-regex` и `-message-regex-no-match send_raw|skip` управляют парсингом `MESSAGE`.
- `-tls-ca-path` и `-tls-use-system-pool` управляют доверенным хранилищем сертификатов клиента.
- Батчи клиента дополнительно ограничены 10 MB несжатых лог-данных на один запрос.
- Если одна запись больше 10 MB, она отправляется отдельным запросом из одной записи (не отбрасывается).

### Примеры файлов маппинга

- `examples/mappings/basic.yaml` — удобный стартовый маппинг для локального запуска с mock или YDB.
- `examples/mappings/ydb.json` — тот же подход в JSON, удобно при подключении YDB writer.

### Локальная настройка mTLS

Следующие команды `openssl` создают локальный CA, а также серверный и клиентский сертификаты, которые подходят для примера mTLS-сервера.

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

Если нужно, чтобы сервер проверял не только доверие к CA, но и значения Subject клиентского сертификата, запускайте его так:

```bash
./bin/server -config examples/config/server.yaml
```

Для локального end-to-end запуска на mock-бэкенде:

```bash
./bin/server -config examples/config/server.yaml
```

```bash
./bin/client \
  -server https://localhost:27312 \
  -client-id myhost \
  -tls-ca-file certs/ca.crt \
  -tls-cert-file certs/client.crt \
  -tls-key-file certs/client.key \
  -message-regex '^(?P<P_DTTM>[^ ]*) :(?P<P_SERVICE>[^ ]*) (?P<P_LEVEL>[^ ]*): (?P<P_MESSAGE>.*)$'
```

## Тесты

```bash
go test ./...
```

Включает unit-тесты для parser, batcher, models, handler и функциональный тест полного client-server потока.

## Интеграция с YDB

Сервер поддерживает `writer_backend: ydb` и использует `github.com/ydb-platform/ydb-go-sdk/v3` для `BulkUpsert`. При включении YDB-бэкенда укажите в конфиге `ydb_endpoint`, `ydb_database` и `ydb_table`. Хранилище позиций также может использовать YDB через `position_store: ydb` и `position_table`. `MockWriter` остается доступным для тестов и локальных прогонов.

Поддерживаемые режимы аутентификации YDB:

- `anonymous` (по умолчанию)
- `static` через `ydb_auth_login` + `ydb_auth_password`
- `service-account-key` через `ydb_auth_sa_key_file`
- `metadata` (учетные данные metadata-сервиса, опционально `ydb_auth_metadata_url`)

Опциональный путь к CA-сертификату для TLS к YDB:

- `ydb_ca_path` — путь к PEM-файлу с CA-сертификатами для проверки TLS-соединения с YDB.

Создайте таблицу YDB для хранилища позиций перед запуском сервера с `position_store: ydb`:

```sql
CREATE TABLE `loglugger_positions` (
  client_id Utf8 NOT NULL,
  expected_position Utf8 NOT NULL,
  PRIMARY KEY (client_id)
);
```

Пример запуска с YDB:

```bash
# скопируйте examples/config/server.yaml и задайте:
# writer_backend: ydb
# position_store: ydb
# ydb_endpoint: grpcs://localhost:2135
# ydb_database: /local
# ydb_table: logs
# field_mapping_file: examples/mappings/ydb.json
./bin/server -config examples/config/server.yaml
```
