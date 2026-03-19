# Loglugger

Двухкомпонентная система для сбора журналов из systemd journald и сохранения их в целевое хранилище (YDB или имитационный бэкенд для тестирования).

Формальная спецификация: [SPECIFICATION.md](SPECIFICATION.md).

## Компоненты

- **Клиент** (`cmd/client`): читает записи из journald, при необходимости разбирает сообщения с помощью регулярного выражения, формирует пакеты и отправляет их на сервер по HTTP.
- **Сервер** (`cmd/server`): принимает пакеты, проверяет непрерывность позиции, сопоставляет поля и записывает их в хранилище (по умолчанию используется `MockWriter`).
- **Экстрактор** (`cmd/extractor`): читает исторические данные из YDB с фильтрами на этапе запроса и пишет их в ротируемые TSV-файлы (опционально сжатыe zstd).

## Сборка

```bash
sudo apt-get install -y libsystemd-dev  # либо аналог для вашей операционной системы
./build.sh
```

В репозитории используется форк `github.com/coreos/go-systemd/v22`, добавленный в `third_party/go-systemd` и подключенный через локальный `replace` в `go.mod`. Этот форк добавляет нативную поддержку namespace в journald (`sd_journal_open_namespace`), которая нужна для надежного чтения из namespace, отличных от стандартного.

Для работы клиента с journald требуется Linux. На macOS и Windows клиент завершится при запуске с ошибкой "journald is only supported on Linux".

После сборки используйте `./bin/loglugger-server`, `./bin/loglugger-client` и `./bin/loglugger-extractor` в командах запуска ниже.

## Запуск

**Сервер** (обязательны TLS и mTLS):
```bash
./bin/loglugger-server -config examples/config/server.yaml
```

Конфигурация сервера:

- Большинство параметров сервера загружается из файла YAML/JSON, указанного через `-config`.
- Пример всех ключей: `examples/config/server.yaml`.
- Необязательное переопределение через командную строку: `-listen :27312` (заменяет `listen_addr` из конфигурационного файла).
- Клиент получает стартовую позицию из `GET /v1/positions?client_id=...` и не хранит локальный файл позиции.
- Защита по размеру запроса настраивается через:
  - `max_compressed_body_bytes`: максимальный размер тела HTTP-запроса до декодирования `Content-Encoding`.
  - `max_decompressed_body_bytes`: максимальный размер JSON-пакета после распаковки.
  - Значения по умолчанию: `8388608` (8 MiB) и `33554432` (32 MiB).

**Клиент** (только Linux):
```bash
./bin/loglugger-client -config examples/config/client.yaml
```

Конфигурация клиента:

- Большинство параметров клиента загружается из файла YAML/JSON, указанного через `-config`.
- Пример всех ключей: `examples/config/client.yaml`.
- `service_mask: nginx.service` использует точное совпадение имени службы systemd.
- `service_mask: "nginx*.service"` использует сопоставление по шаблону.
- `service_mask: "regex:^nginx-(api|worker)\\.service$"` использует регулярное выражение.
- `message_regex`, `systemd_unit_regex` и `message_regex_no_match` настраивают клиентский парсинг до отправки записей на сервер.
- Склейка многострочных сообщений работает только при заданном `message_regex`:
  - строки-продолжения добавляются к текущему сообщению до появления следующей строки, совпавшей с `message_regex`;
  - `multiline_timeout` (по умолчанию `1s`) завершает текущее склеиваемое сообщение, если следующая строка не пришла вовремя;
  - `multiline_max_messages` (по умолчанию `1000`) ограничивает число исходных строк в одном итоговом сообщении.
- `journal_recovery: true` включает режим best-effort восстановления после повреждения журнала (`EBADMSG` / `bad message`). По умолчанию этот режим выключен, так как при обходе поврежденного участка возможна потеря части данных. Без этой опции клиент пишет ошибку о повреждении и сразу завершает работу.
- `server_url` / `server_urls` задают одну или несколько конечных точек; клиент сохраняет текущую конечную точку, пока запросы выполняются успешно, и переключается на следующую только после временной ошибки (`5xx` или сетевой ошибки).
- `tls_ca_file` и `tls_use_system_pool` управляют доверенным хранилищем сертификатов клиента.
- Пакеты клиента дополнительно ограничены 10 MB несжатых данных журналов на один запрос.
- Если размер одной записи превышает 10 MB, она отправляется отдельным запросом из одной записи и не отбрасывается.
- Каждая исходящая запись содержит `seqno`: монотонно возрастающий номер последовательности на стороне клиента. Первое значение равно времени запуска клиента в миллисекундах Unix epoch.

Если режим восстановления включен и клиент сталкивается с повреждением журнала, он выводит предупреждение о возможной потере данных, затем пытается заново открыть журнал и продолжить чтение с последней корректной позиции, а при неудаче переходит к попытке чтения начиная с момента сразу после последней корректной временной отметки. При успешном восстановлении следующий пакет отправляется с `reset`, чтобы сервер принял новую позицию. Если восстановление все равно невозможно, клиент завершает работу.

**Экстрактор** (чтение из YDB и запись TSV):
```bash
./bin/loglugger-extractor \
  -server-config examples/ydbd/loglugger-server.yaml \
  -from 2025-03-13T10:00:00Z \
  -to 2025-03-13T11:00:00Z \
  -output-dir ./out
```

Параметры и поведение экстрактора:

- `-server-config` обязателен; используются YDB-настройки сервера (`ydb_endpoint`, `ydb_database`, `ydb_table`, `ydb_auth_*`, `ydb_ca_path`, `ydb_open_timeout`).
- `-from` и `-to` обязательны и задают интервал `[from,to)` (нижняя граница включительно, верхняя исключительно; формат RFC3339/RFC3339Nano).
- `-time-column` задает колонку event-time для обязательного фильтра по времени (по умолчанию `ts_orig`).
- `-filter field=v1,v2` добавляет необязательные фильтры по списку значений (`IN`); ключ можно повторять для разных полей.
- Формат вывода: TSV; в текстовых значениях нормализуются TAB/переводы строк, чтобы не ломать структуру TSV.
- Ротация файлов по размеру:
  - по умолчанию без сжатия: `200MiB`
  - по умолчанию при `-zstd`: `10MiB`
  - переопределение через `-max-file-size`
- Имена файлов: `<output-prefix>_<NNNNNN>.tsv` (или `.tsv.zst` при `-zstd`).

### Примеры файлов сопоставления

- `examples/mappings/basic.yaml` — базовый файл сопоставления для локального запуска с имитационным бэкендом или YDB.
- `examples/mappings/ydb.json` — тот же подход в формате JSON; удобен при использовании модуля записи в YDB.
- Механизм сопоставления поддерживает вычисляемые исходные поля:
  - `log_timestamp_us`: метка времени записи с точностью до микросекунд; для YDB `Timestamp64` используйте `transform: timestamp64_us`.
  - `message_cityhash64`: `CityHash64` от полного содержимого записи; обычно сопоставляется с `message_hash` (`Uint64`) для обеспечения уникальности.
- Примеры схемы и сопоставления полей для YDB приведены в `examples/ydbd/field_mapping.yaml` и `examples/ydbd/target_table.sql`.

### Локальная настройка mTLS

Следующие команды `openssl` создают локальный CA, а также серверный и клиентский сертификаты, используемые в примере с mTLS.

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
  -subj "/CN=loglugger-client/O=dev/OU=ydb" \
  -out certs/client.csr
openssl x509 -req -in certs/client.csr \
  -CA certs/ca.crt -CAkey certs/ca.key -CAcreateserial \
  -out certs/client.crt -days 1825 -sha256
```

Если требуется, чтобы сервер проверял не только доверие к CA, но и атрибуты Subject клиентского сертификата, запускайте его так:

```bash
./bin/loglugger-server -config examples/config/server.yaml
```

Если для `tls_client_subject_cn`, `tls_client_subject_o` или `tls_client_subject_ou` используются правила вида `regex:<pattern>`:

- Регулярные выражения компилируются и проверяются при запуске сервера.
- Некорректные шаблоны приводят к ошибке конфигурации при запуске, а не игнорируются во время TLS-рукопожатия.

Для локального сквозного запуска с имитационным бэкендом:

```bash
./bin/loglugger-server -config examples/config/server.yaml
```

```bash
./bin/loglugger-client -config examples/config/client.yaml
```

## Тесты

```bash
go test ./...
```

Набор тестов включает модульные тесты для `parser`, `batcher`, `models`, `handler`, а также функциональный тест полного клиент-серверного потока.

## Интеграция с YDB

Сервер поддерживает `writer_backend: ydb` и использует `github.com/ydb-platform/ydb-go-sdk/v3` для `BulkUpsert`. При включении YDB-бэкенда укажите в конфигурационном файле `ydb_endpoint`, `ydb_database` и `ydb_table`. Реализация хранилища позиций выбирается автоматически на основе `writer_backend` (`mock` -> позиции в памяти, `ydb` -> хранение позиций в YDB через `position_table`). `MockWriter` остается доступным для тестов и локальных запусков.

Поддерживаемые режимы аутентификации YDB:

- `anonymous` (по умолчанию)
- `static` через `ydb_auth_login` + `ydb_auth_password`
- `service-account-key` через `ydb_auth_sa_key_file`
- `metadata` (учетные данные сервиса метаданных, необязательно `ydb_auth_metadata_url`)

Необязательный путь к CA-сертификату для TLS-соединения с YDB:

- `ydb_ca_path` — путь к PEM-файлу с CA-сертификатами для проверки TLS-соединения с YDB.
- `ydb_open_timeout` — таймаут открытия соединения с YDB при запуске (по умолчанию `10s`).

Создайте таблицу YDB для хранилища позиций перед запуском сервера с `writer_backend: ydb`:

```sql
CREATE TABLE `loglugger_positions` (
  client_id Utf8 NOT NULL,
  exp_pos Utf8 NOT NULL,
  ts_wall Timestamp64 NOT NULL,
  seqno Int64,
  ts_orig Timestamp64,
  PRIMARY KEY (client_id)
);
```

Пример запуска с YDB:

```bash
# скопируйте examples/config/server.yaml и задайте:
# writer_backend: ydb
# ydb_endpoint: grpcs://localhost:2135
# ydb_database: /local
# ydb_table: logs
# ydb_open_timeout: 10s
# field_mapping_file: examples/mappings/ydb.json
./bin/loglugger-server -config examples/config/server.yaml
```

Замечания по схеме и сопоставлению полей для YDB:

- В `examples/ydbd/target_table.sql` столбцы `log_timestamp_us` и `ts_orig` имеют тип `Timestamp64`.
- В `examples/ydbd/field_mapping.yaml` используйте:
  - `transform: timestamp64` для строковых даты/времени (например, `parsed.P_DTTM` -> `ts_orig` в кастомных полях)
  - `transform: timestamp64_us` для Unix timestamps в микросекундах (например, `log_timestamp_us`, `realtime_ts` -> `ts_orig`)
- Значения без таймзоны при `transform: timestamp64` интерпретируются как UTC.
- Параметры `message_regex`, `systemd_unit_regex` и `message_regex_no_match` задаются на клиенте, в YAML/JSON-конфигурации.
- Именованные группы из обоих regex объединяются в одном пространстве `parsed.*` и доступны при настройке записи полей в базу данных.
