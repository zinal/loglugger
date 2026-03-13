# Loglugger

Двухкомпонентная система для сбора логов из systemd journald и сохранения их в бэкенд (YDB или mock для тестирования).

Формальная спецификация: [SPECIFICATION.md](SPECIFICATION.md).

## Компоненты

- **Клиент** (`cmd/client`): читает данные из journald, при необходимости парсит сообщения регулярным выражением, формирует батчи и отправляет их на сервер по HTTP.
- **Сервер** (`cmd/server`): принимает батчи, проверяет непрерывность позиции, маппит поля и записывает их в хранилище (по умолчанию `MockWriter`).

## Сборка

```bash
mkdir -p bin
go build -o bin/server ./cmd/server
go build -o bin/client ./cmd/client
```

Для работы клиента с journald требуется Linux. На macOS/Windows клиент завершится на старте с ошибкой "journald is only supported on Linux".

После сборки используйте `./bin/server` и `./bin/client` в командах запуска ниже.

## Запуск

**Сервер** (обязательны TLS + mTLS):
```bash
./bin/server \
  -listen :8443 \
  -tls-cert-file certs/server.crt \
  -tls-key-file certs/server.key \
  -tls-ca-file certs/ca.crt \
  -field-mapping-file examples/mappings/basic.yaml
```

Полезные флаги сервера:

- `-writer mock|ydb` выбирает выходной бэкенд.
- `-position-store memory|ydb` выбирает хранение позиций в памяти или в YDB.
- `-position-table loglugger_positions` задает таблицу YDB для сохранения ожидаемых позиций клиентов.
- `-field-mapping-file mapping.yaml` загружает маппинг source->destination из YAML/JSON.
- `-tls-client-subject-cn`, `-tls-client-subject-o`, `-tls-client-subject-ou` ограничивают допустимые поля Subject в клиентском сертификате.
- Клиент получает стартовую позицию из `GET /v1/positions?client_id=...` и не хранит локальный файл позиции.

**Клиент** (только Linux):
```bash
./bin/client \
  -server https://localhost:8443 \
  -client-id myhost \
  -tls-ca-file certs/ca.crt \
  -tls-cert-file certs/client.crt \
  -tls-key-file certs/client.key
```

Полезные флаги клиента:

- `-service-mask nginx.service` использует точное совпадение имени systemd unit.
- `-service-mask 'nginx*.service'` использует glob-сопоставление.
- `-service-mask 'regex:^nginx-(api|worker)\\.service$'` использует регулярное выражение.
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
./bin/server \
  -listen :8443 \
  -tls-cert-file certs/server.crt \
  -tls-key-file certs/server.key \
  -tls-ca-file certs/ca.crt \
  -tls-client-subject-cn client-local \
  -tls-client-subject-o dev \
  -tls-client-subject-ou local \
  -field-mapping-file examples/mappings/basic.yaml \
  -position-store ydb \
  -ydb-endpoint grpcs://localhost:2135 \
  -ydb-database /local \
  -position-table loglugger_positions
```

Для локального end-to-end запуска на mock-бэкенде:

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
  -message-regex '^(?P<P_DTTM>[^ ]*) :(?P<P_SERVICE>[^ ]*) (?P<P_LEVEL>[^ ]*): (?P<P_MESSAGE>.*)$'
```

## Тесты

```bash
go test ./...
```

Включает unit-тесты для parser, batcher, models, handler и функциональный тест полного client-server потока.

## Интеграция с YDB

Сервер поддерживает `-writer ydb` и использует `github.com/ydb-platform/ydb-go-sdk/v3` для `BulkUpsert`. При включении YDB-бэкенда укажите `-ydb-endpoint`, `-ydb-database` и `-ydb-table`. Хранилище позиций также может использовать YDB через `-position-store ydb` и `-position-table`. `MockWriter` остается доступным для тестов и локальных прогонов.

Создайте таблицу YDB для хранилища позиций перед запуском сервера с `-position-store ydb`:

```sql
CREATE TABLE `loglugger_positions` (
  client_id Utf8 NOT NULL,
  expected_position Utf8 NOT NULL,
  PRIMARY KEY (client_id)
);
```

Пример запуска с YDB:

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
  -position-store ydb \
  -position-table loglugger_positions
```
