package client

import (
	"regexp"

	"github.com/ydb-platform/loglugger/internal/models"
)

// NoMatchAction defines behavior when regex does not match.
type NoMatchAction string

const (
	NoMatchSendRaw NoMatchAction = "send_raw"
	NoMatchSkip    NoMatchAction = "skip"
)

// MessageParser parses the MESSAGE field with a regex and extracts named groups.
type MessageParser interface {
	Parse(rec models.Record) (models.Record, bool)
}

type messageParser struct {
	re       *regexp.Regexp
	noMatch  NoMatchAction
	groupNames []string
}

// NewMessageParser creates a message parser. If regexStr is empty, returns nil (no parsing).
func NewMessageParser(regexStr string, noMatch NoMatchAction) (MessageParser, error) {
	if regexStr == "" {
		return nil, nil
	}
	re, err := regexp.Compile(regexStr)
	if err != nil {
		return nil, err
	}
	names := re.SubexpNames()
	groupNames := make([]string, 0, len(names))
	for _, n := range names {
		if n != "" {
			groupNames = append(groupNames, n)
		}
	}
	return &messageParser{re: re, noMatch: noMatch, groupNames: groupNames}, nil
}

func (p *messageParser) Parse(rec models.Record) (models.Record, bool) {
	matches := p.re.FindStringSubmatch(rec.Message)
	if matches == nil {
		if p.noMatch == NoMatchSkip {
			return models.Record{}, false
		}
		return rec, true
	}
	parsed := make(map[string]string)
	for _, name := range p.groupNames {
		idx := p.re.SubexpIndex(name)
		if idx >= 0 && idx < len(matches) && matches[idx] != "" {
			parsed[name] = matches[idx]
		}
	}
	out := rec
	out.Message = ""
	out.Parsed = parsed
	return out, true
}
