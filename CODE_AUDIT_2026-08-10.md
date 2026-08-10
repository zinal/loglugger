# Аудит кода Loglugger

- **Дата анализа:** 2026-08-10 (UTC)
- **Анализируемый Git describe:** `v1.0-69-gcd25718`
- **Анализируемый commit:** `cd257185a947355e62a9e07eed201a213d23dedf`
- **Ветка-источник:** чистый `main`, синхронизированный с `origin/main`
- **Требуемая семантика доставки:** at-least-once

## Краткий итог

Основные ранее найденные дефекты исправлены: точная проверка journald cursor,
согласованные лимиты 15/16/32 MiB, корректный YDB DSN и полные пути таблиц,
HTTP 400 для непригодных к повтору mapping-ошибок, очистка stale-остатка batcher
после `409`, защита Ansible-секретов и HTTP timeouts.

При этом обнаружена одна новая ошибка с риском потери данных и несколько
остаточных ошибок обработки отказов. Наиболее опасна очистка неотправленного
буфера после journal recovery: recovery продолжает чтение после последнего
прочитанного cursor, поэтому очищенные записи могут больше не перечитаться.

## Подтверждённые исправления

1. `SeekCursor` проверяется через `Next` + `TestCursor`; vacuum/rotation больше
   не приводит к пропуску ближайшей сохранившейся записи.
2. Default сервера `max_compressed_body_bytes` исправлен на 16 MiB и согласован
   с клиентским JSON cap 15 MiB и decompressed cap 32 MiB.
3. YDB DSN больше не содержит двойной `/`; native API получают полный путь
   таблицы с database prefix.
4. Mapping/transform ошибки записи возвращаются как HTTP 400 и не вызывают
   бесконечный retry poison batch.
5. После `409 position_mismatch` клиент очищает stale batcher remainder и
   незавершённый multiline state.
6. Shutdown клиента ограничен отдельным десятисекундным context и больше не
   может бесконечно ждать недоступный сервер.
7. Клиент считает точный размер JSON, повторно использует сериализованные
   записи и отклоняет одиночную запись, не помещающуюся в лимит.
8. HTTP response bodies ограничены 1 MiB; конфигурация клиента валидирует
   batch/timing значения и запрещает URL userinfo.
9. В Ansible пароль YDB вынесен в Vault, server config имеет mode `0600`, а
   замена бинарников уведомляет handler перезапуска сервиса.
10. Дистрибутив включает LICENSE и changelog-файлы; EN/RU changelog согласованы.

## Находки

### Высокий приоритет

#### AUD-01 — потеря неотправленных записей после journal recovery

**Код:** `cmd/client/main.go:247-259`, `cmd/client/main.go:588-598`,
`internal/client/journal_linux.go:197-270`.

При чтении записи `journalReader.last` двигается до её cursor ещё до отправки
batch. Если следующий `Next` обнаруживает corruption, recovery продолжает
чтение после `last`. После успешного recovery код вызывает
`discardUnsentBuffers`, очищая batcher и multiline state.

Сценарий:

1. Клиент прочитал и накопил `c1..c10`, но batch ещё не отправлен.
2. Следующая операция возвращает `EBADMSG`.
3. Recovery возобновляет journal после `c10`.
4. Batcher с `c1..c10` очищается.
5. Следующий reset batch начинается с `c11`.

`c1..c10` не были записаны сервером и больше не будут перечитаны. Это нарушает
at-least-once. Аналогичная потеря возможна для незавершённого multiline,
cursor которого уже учтён в `last`.

Обновлённая спецификация в §4.3.1 также требует очищать буферы после recovery,
то есть несогласованность присутствует и в контракте. Очистка нужна после
server-side `409`, но не должна механически применяться к локальному recovery.

#### AUD-02 — ошибка fallback seek после `409` проглатывается

**Код:** `cmd/client/main.go:623-639`.

Если seek к `expected_position` не удался и последующий `SeekToPosition("")`
тоже завершился ошибкой, `sendBatch` всё равно возвращает `err=nil` и
`streamRestarted=true`. Вызывающий код очищает локальные буферы и продолжает
работу, хотя journal не был перепозиционирован.

Результатом может быть reset batch из неопределённой позиции, повторный пропуск
записей или протокольная рассинхронизация. Ошибка fallback seek должна быть
фатальной и возвращаться вызывающему коду.

### Средний приоритет

#### AUD-03 — extractor retry может дублировать строки

**Код:** `cmd/extractor/main.go:552-596`.

`runExtraction` выполняет callback с `table.WithIdempotent()`, но callback имеет
неидемпотентный внешний side effect — пишет строки в файлы. При transient YDB
ошибке после частичной выгрузки SDK может повторить callback с начала, добавив
тот же префикс строк повторно.

После окончательной ошибки partial-файлы остаются. `O_EXCL` защищает их от
перезаписи, но повторный ручной запуск с тем же output prefix немедленно
завершается с `EEXIST`. Требуется либо отключить автоматический retry вокруг
file-writing callback, либо добавить checkpoint/staging/resume семантику.

#### AUD-04 — timeout финального flush трактуется как успех

**Код:** `cmd/client/main.go:197-217`, `cmd/client/main.go:614-620`.

При `context.Canceled` или `context.DeadlineExceeded` уже извлечённый через
`Flush` batch считается успешно обработанным (`err=nil`). После десяти секунд
shutdown завершается обычным сообщением, хотя batch не принят сервером.

Server position позволяет перечитать данные при следующем запуске, если journal
ещё хранит cursor, поэтому это не обязательная постоянная потеря. Однако при
намеренной остановке без рестарта доставка не завершена и факт этого скрыт.
Нужен как минимум явный warning/error status; для строгого graceful drain —
отдельная политика завершения.

#### AUD-05 — oversized record завершает процесс без flush предыдущего буфера

**Код:** `cmd/client/main.go:153-165`, `cmd/client/main.go:193-195`,
`internal/client/batcher.go:67-80`.

Если в batcher уже есть допустимые записи, а следующая запись превышает 15 MiB,
`Batcher.Add` возвращает `ErrRecordJSONTooLarge`, после чего вызывается прямой
`os.Exit(1)`. Ранее накопленный допустимый batch не отправляется.

После supervisor restart записи обычно перечитаются по server position, но
поведение зависит от сохранности journal. Перед fail-stop следует попытаться
ограниченно отправить уже накопленный допустимый batch.

#### AUD-06 — oversized/нечитаемое тело 4xx превращается в бесконечный retry

**Код:** `internal/client/sender.go:165-170`,
`internal/client/sender.go:237-254`, `internal/client/sender.go:280-284`.

Ошибка чтения response body обрабатывается до классификации HTTP status.
Поэтому 400/401/403/413 с телом больше 1 MiB считается transient transport
ошибкой и повторяется бесконечно вместо предусмотренного fail-stop для 4xx.
Лимит памяти полезен, но status должен учитываться даже при truncated body.

#### AUD-07 — сервер не выполняет graceful HTTP shutdown

**Код:** `cmd/server/main.go:131-154`.

Сервер не обрабатывает SIGINT/SIGTERM через `signal.NotifyContext` и
`http.Server.Shutdown`. Во время rolling deploy или Ansible restart активный
batch может быть оборван. Порядок write-before-position сохраняет at-least-once,
но создаёт ненужные повторы и расходится с общим требованием graceful shutdown
из спецификации.

### Низкий приоритет и документированные ограничения

1. `ValidateFieldMappingsAgainstColumns` не проверяет, что mapping покрывает все
   `NOT NULL` колонки. Конфигурация может пройти startup validation и упасть на
   первом `BulkUpsert`.
2. YAML/JSON конфиги декодируются нестрого: неизвестные ключи и опечатки могут
   быть проигнорированы.
3. mTLS разрешает доверенный сертификат, но не связывает его identity с
   `client_id`; uniqueness/authorization объявлены out of scope.
4. `ShouldFlush` считает `len(entries)`, а `Flush` ограничивает cursor-bearing
   `journalCount`; synthetic recovery record может дать batch на одну запись
   больше `batch_size`.
5. README включается в portable archive, но ссылается на исключённый из архива
   `SPECIFICATION.md`.
6. Без Docker/Podman/Zig нельзя проверить portable archive; `build-ditto.sh`
   напрямую требует Zig.
7. Поведение extractor `O_EXCL` и необходимость нового/очищенного output
   directory перед повторным запуском не описаны в README.

## Проверки

На анализируемом commit успешно выполнены:

```text
go version
  go version go1.26.5 linux/amd64

go test -count=1 ./...
  PASS

go test -race -count=1 ./...
  PASS

go vet ./...
  PASS

go test -shuffle=on -count=5 ./...
  PASS

./build.sh
  PASS
```

Обычная сборка предупредила, что клиент требует glibc >= 2.34; для старых
систем предусмотрена portable-сборка.

## Ограничения анализа

- Не запускались live-интеграционные тесты с YDB.
- Не выполнялся end-to-end тест с реальным journald corruption/rotation.
- Не выполнялись `build-docker.sh` и `build-ditto.sh`: в среде отсутствуют
  Docker, Podman и Zig.
- Анализ является статическим аудитом и проверкой доступных unit/functional
  тестов; он не доказывает отсутствие иных дефектов.
