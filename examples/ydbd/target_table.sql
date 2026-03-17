-- Target table for ydbd log records ingested by loglugger.
-- Primary key uses fields that are always present in loglugger records.
CREATE TABLE `ydblogs` (
  ts_orig Timestamp64,
  dbname Utf8,
  service Utf8,
  level Utf8,
  msg Utf8,
  unit Utf8,
  seqno Int64,
  ts_log Timestamp64 NOT NULL,
  message_hash Uint64 NOT NULL,
  hostname Utf8 NOT NULL,
  PRIMARY KEY (ts_log, hostname, message_hash)
) WITH (
  STORE = COLUMN
);

-- Table for position tracking.
CREATE TABLE `loglugger_positions` (
  client_id Utf8 NOT NULL,
  expected_position Utf8 NOT NULL,
  PRIMARY KEY (client_id)
);
