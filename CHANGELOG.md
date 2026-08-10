# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

Russian version: [CHANGELOG-ru.md](CHANGELOG-ru.md).

## [Unreleased]

High-level changes on `main` since the `v1.0` tag.

### Changed

- Delivery semantics are documented as **at-least-once** (with ordered per-client processing), replacing the earlier exactly-once wording.
- Client batch size limits are measured as the exact uncompressed JSON request body (default cap **15 MiB**), instead of an approximate uncompressed log-data budget.
- Default server compressed request-body limit raised from 8 MiB to **16 MiB**, aligned with the client JSON cap and the existing 32 MiB decompressed limit.
- Go toolchain upgraded from 1.24.x to **1.26.5**.

### Added

- Configurable HTTP server timeouts (`read_header_timeout`, `read_timeout`, `write_timeout`, `idle_timeout`) to mitigate Slowloris and stalled connections.
- Client fail-stop behavior for non-retryable batch HTTP failures (4xx other than handled position mismatch) and for persistent journald read I/O errors after a short retry window (~15s).
- Stricter journald resume: stored cursors are verified with `sd_journal_test_cursor` so vacuumed/rotated positions reset instead of skipping records.
- Server returns **HTTP 400** when field mapping or transforms fail for a record in a batch (position is not advanced).
- Client configuration validation for batch/HTTP timing settings and rejection of credentials embedded in server URLs.
- Ansible example support for storing static YDB passwords in **Ansible Vault**, with server config installed mode `0600`.
- Ansible roles restart Loglugger services when installed binaries are replaced.

### Fixed

- After HTTP `409` position mismatch (reseek/reset) or successful journal corruption recovery, the client discards any unsent batcher remainder and unfinished multiline state left by JSON/count splits, so stale `current_position` values cannot trigger repeated `409`s or corrupt a reset batch.
- Client no longer busy-polls under a continuous journald stream; non-corruption `GetEntry` failures are skipped without bypassing recovery paths.
- Position continuity is preserved when `message_regex_no_match` is `skip` (discarded records do not advance protocol positions).
- HTTP `409` responses with an invalid JSON body are no longer treated as successful batch accepts.
- YDB writes encode values from live table column types (`DescribeTable`), correctly wrap present nullable values as optional, and keep position `seqno` / `ts_orig` when a batch omits them.
- YDB DSN construction and native API table paths handle absolute database paths correctly (no double-slash / incomplete path issues).
- Extractor output file creation is hardened; TSV escaping documentation matches the implementation (`\t` / `\n` / `\r`).
- Final shutdown batch send is bounded by a short timeout so exit cannot hang indefinitely.
- Client HTTP sender limits how much of error/response bodies are read into memory.

### Security

- Ansible example no longer encourages plaintext YDB passwords in inventory; credentials move to an encrypted Vault file and a restricted server config file mode.

## [1.0] - 2026-08-10

Initial tagged release (`v1.0`).
