-- Target table for ydbd log records ingested by loglugger.
-- Primary key uses fields that are always present in loglugger records.
CREATE TABLE `ydblogs` (
  ts_orig Timestamp64,
  dbname Utf8,
  service Utf8,
  level Utf8,
  msg Utf8,
  hostname Utf8,
  unit Utf8,
  input Utf8,
  ts_log Timestamp64 NOT NULL,
  message_hash Uint64 NOT NULL,
  client_id Utf8 NOT NULL,
  PRIMARY KEY (ts_log, client_id, message_hash)
) WITH (
  STORE = COLUMN
);
