-- Target table for ydbd log records ingested by loglugger.
-- Primary key uses fields that are always present in loglugger records.
CREATE TABLE `ydblogs` (
  client_id Utf8 NOT NULL,
  ts_epoch_us Int64 NOT NULL,
  monotonic_ts Uint64 NOT NULL,
  dbname Utf8,
  service Utf8,
  ts_orig Utf8,
  level Utf8,
  msg Utf8,
  hostname Utf8,
  unit Utf8,
  input Utf8,
  PRIMARY KEY (client_id, ts_epoch_us, monotonic_ts)
);
