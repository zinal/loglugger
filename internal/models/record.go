package models

import "strconv"

// Record represents a log record sent from client to server.
// Parsed fields are server-side enrichment and are not part of the wire protocol.
type Record struct {
	Message            string            `json:"message,omitempty"`
	Parsed             map[string]string `json:"-"`
	SeqNo              *int64            `json:"seqno,omitempty"`
	Priority           *int              `json:"priority,omitempty"`
	SyslogIdentifier   string            `json:"syslog_identifier,omitempty"`
	SystemdUnit        string            `json:"systemd_unit,omitempty"`
	RealtimeTimestamp  *int64            `json:"realtime_timestamp,omitempty"`
	MonotonicTimestamp *uint64           `json:"monotonic_timestamp,omitempty"`
	Fields             map[string]string `json:"fields,omitempty"`
}

// HasParsed returns true if the record has parsed fields.
func (r *Record) HasParsed() bool {
	return len(r.Parsed) > 0
}

// GetField returns a value by source path (e.g., "message", "parsed.P_DTTM", "fields.CODE_FILE").
func (r *Record) GetField(path string) (string, bool) {
	switch path {
	case "message":
		if r.Message != "" {
			return r.Message, true
		}
		return "", false
	case "seqno":
		if r.SeqNo != nil {
			return fmtInt64(*r.SeqNo), true
		}
		return "", false
	case "syslog_identifier":
		return r.SyslogIdentifier, r.SyslogIdentifier != ""
	case "systemd_unit":
		return r.SystemdUnit, r.SystemdUnit != ""
	case "realtime_timestamp":
		if r.RealtimeTimestamp != nil {
			return fmtInt64(*r.RealtimeTimestamp), true
		}
		return "", false
	case "monotonic_timestamp":
		if r.MonotonicTimestamp != nil {
			return fmtUint64(*r.MonotonicTimestamp), true
		}
		return "", false
	case "priority":
		if r.Priority != nil {
			return fmtInt(*r.Priority), true
		}
		return "", false
	}
	if len(path) > 7 && path[:7] == "parsed." {
		key := path[7:]
		if v, ok := r.Parsed[key]; ok {
			return v, true
		}
		return "", false
	}
	if len(path) > 7 && path[:7] == "fields." {
		key := path[7:]
		if v, ok := r.Fields[key]; ok {
			return v, true
		}
		return "", false
	}
	return "", false
}

func fmtInt64(n int64) string {
	return strconv.FormatInt(n, 10)
}

func fmtUint64(n uint64) string {
	return strconv.FormatUint(n, 10)
}

func fmtInt(n int) string {
	return strconv.Itoa(n)
}
