# Повторный аудит кода Loglugger

- **Дата анализа:** 2026-08-10 (UTC)
- **Анализируемый Git describe:** `v1.0-77-g79148c8`
- **Анализируемый commit:** `79148c86d8c6bcf084f4f8ff55d0bca8f1ab5985`
- **Предыдущая анализируемая ревизия:** `v1.0-69-gcd25718`
- **Ветка-источник:** чистый `main`, синхронизированный с `origin/main`
- **Требуемая семантика доставки:** at-least-once

## Краткий итог

Три наиболее значимые находки предыдущего анализа закрыты:

1. После journal corruption recovery клиент сохраняет и отправляет уже
   накопленные записи вместо их удаления.
2. Ошибка fallback seek-to-head после HTTP 409 теперь приводит к fail-stop.
3. Extractor больше не использует автоматический SDK retry вокруг callback,
   записывающего файлы, поэтому тот же callback не дописывает строки повторно.

Прямых новых нарушений at-least-once в основном client/server потоке не
обнаружено. Главная новая остаточная проблема находится в extractor: очистка
всех файлов prefix перед каждой попыткой делает повторный запуск
деструктивным и не обеспечивает заявленную защиту от конкурентных процессов.

## Статус предыдущих находок

### Закрыто

#### AUD-01 — потеря buffered-записей после journal recovery

**Статус:** исправлено.

`cmd/client/main.go:247-269` сохраняет batcher и multiline state, устанавливает
`reset: true` и ограниченно отправляет накопленный batch. Спецификация §4.3.1
исправлена: буферы сохраняются после локального recovery, но очищаются после
server-side `409` stream restart.

#### AUD-02 — ошибка fallback seek после `409` проглатывалась

**Статус:** исправлено.

`cmd/client/main.go:645-662` возвращает ошибку, если не удались и seek к
`expected_position`, и fallback seek к head. `streamRestarted` остаётся false,
поэтому локальные буферы не очищаются, а клиент fail-stop.

#### AUD-03 — extractor retry дублировал строки

**Статус:** исходный механизм дублирования исправлен, но появился новый риск
очистки результатов.

`runExtractionOnce` отключает SDK auto-retry для file-writing callback.
`runExtractionWithRetries` выполняет ограниченные внешние попытки, закрывая
writer и очищая файлы prefix между попытками.

### Осталось без изменений

- AUD-04: timeout/cancel финального client flush трактуется как успешное
  завершение.
- AUD-05: oversized record завершает клиент без предварительной отправки уже
  накопленного допустимого batch.
- AUD-06: ошибка чтения oversized response body классифицируется до HTTP status;
  большой 4xx body может уйти в бесконечный retry.
- AUD-07: сервер не выполняет graceful HTTP shutdown.

## Остаточные и новые проблемы

### Высокий приоритет

#### AUD-R01 — extractor безусловно удаляет прежние результаты и небезопасен при конкурентном запуске

**Код:** `cmd/extractor/main.go:567-633`, `cmd/extractor/main.go:711-754`.

Перед **первой** и каждой последующей попыткой `removeOutputPrefixFiles`
удаляет все файлы вида `<prefix>_<NNNNNN>.tsv[.zst]`.

Это создаёт два сценария потери данных:

1. Повторный запуск с тем же prefix удаляет полностью успешную предыдущую
   выгрузку до выполнения нового scan. Если новый запуск затем падает, старый
   корректный результат уже потерян.
2. Два extractor-процесса с одинаковыми `output-dir` и prefix не защищены
   `O_EXCL`: второй процесс сначала удаляет открытые файлы первого, а затем
   создаёт пути заново. Первый продолжает писать в unlink'ed inode и может
   завершиться успешно, хотя его результат недоступен.

README и SPEC документируют удаление prefix-файлов, поэтому первый сценарий
является явно выбранной, но опасной семантикой. Утверждение, что `O_EXCL`
защищает конкурентных writers, неверно при наличии предварительного удаления.

Нужна изоляция попыток: per-run staging directory/unique suffix, lock-файл и
atomic publish после полного успеха. Удалять ранее опубликованный набор следует
только явно или после успешной подготовки замены.

### Средний приоритет

#### AUD-R02 — timeout финального flush скрывается как успех

**Код:** `cmd/client/main.go:197-217`, `cmd/client/main.go:636-642`.

При `context.Canceled` или `context.DeadlineExceeded` уже извлечённый через
`Flush` batch считается успешно обработанным (`err=nil`). После десяти секунд
shutdown завершается обычным сообщением, хотя сервер batch не принял.

Server position обычно позволяет перечитать записи при следующем запуске, если
journal сохранил cursor. Но при намеренной остановке без рестарта доставка не
завершена и факт этого не виден оператору. Нужен явный warning/non-zero status
или отдельная документированная политика bounded shutdown.

#### AUD-R03 — oversized record обходит flush предыдущего допустимого batch

**Код:** `cmd/client/main.go:153-165`, `cmd/client/main.go:193-195`,
`internal/client/batcher.go:68-81`.

Если batcher уже содержит допустимые записи, а следующая запись превышает
15 MiB, `Batcher.Add` возвращает `ErrRecordJSONTooLarge`, после чего клиент
вызывает прямой `os.Exit(1)`. Накопленный допустимый batch не отправляется.

После supervisor restart данные обычно перечитываются по server position, но
это зависит от сохранности journald. Перед fail-stop следует ограниченно
отправить уже сформированный допустимый batch.

#### AUD-R04 — большой или нечитаемый body ответа 4xx превращается в transient retry

**Код:** `internal/client/sender.go:165-170`,
`internal/client/sender.go:237-254`, `internal/client/sender.go:280-284`.

Response body читается и ограничивается до классификации HTTP status. Поэтому
400/401/403/413 с телом больше 1 MiB считается transport/read ошибкой и
повторяется бесконечно вместо предусмотренного fail-stop для 4xx.

#### AUD-R05 — сервер не выполняет graceful HTTP shutdown

**Код:** `cmd/server/main.go:131-154`.

Сервер не обрабатывает SIGINT/SIGTERM через `signal.NotifyContext` и
`http.Server.Shutdown`. Rolling deploy или Ansible restart может оборвать
активный batch. Порядок write-before-position сохраняет at-least-once, но
создаёт повторы и расходится с общим graceful-shutdown требованием.

### Низкий приоритет и ограничения дизайна

1. `ValidateFieldMappingsAgainstColumns` не проверяет покрытие всех `NOT NULL`
   колонок. Конфигурация может пройти startup validation и упасть на первом
   `BulkUpsert`.
2. YAML/JSON конфиги декодируются нестрого: неизвестные ключи и опечатки могут
   быть проигнорированы.
3. mTLS identity не связывается с `client_id`; uniqueness/authorization
   объявлены out of scope.
4. `ShouldFlush` считает `len(entries)`, а `Flush` ограничивает cursor-bearing
   `journalCount`; synthetic recovery record может дать batch на одну запись
   больше `batch_size`.
5. Изменение `service_mask` не определяется автоматически, хотя SPEC относит
   его к причинам reset.
6. Portable README ссылается на `SPECIFICATION.md`, который исключён из archive.
7. `CHANGELOG.md` и `CHANGELOG-ru.md` расходятся: русская версия дополнительно
   содержит пункты про `AGENTS.md` и обновление примеров/спецификации, нарушая
   требование синхронного содержания из `AGENTS.md`.
8. Portable build не проверялся: в среде отсутствуют Docker, Podman и Zig.

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
- Не выполнялись `build-docker.sh` и `build-ditto.sh`.
- Анализ является статическим аудитом и проверкой доступных тестов; он не
  доказывает отсутствие иных дефектов.
