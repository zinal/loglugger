package client

import (
	"regexp"

	"github.com/ydb-platform/loglugger/internal/models"
)

// NoMatchAction defines behavior when message regex does not match.
type NoMatchAction string

const (
	NoMatchSendRaw NoMatchAction = "send_raw"
	NoMatchSkip    NoMatchAction = "skip"
)

// MessageParser parses record fields using regexes and extracts named groups.
type MessageParser interface {
	Parse(rec models.Record) (models.Record, bool)
}

type messageParser struct {
	messageRe             *regexp.Regexp
	messageGroupNames     []string
	systemdUnitRe         *regexp.Regexp
	systemdUnitGroupNames []string
	noMatch               NoMatchAction
}

// NewRecordParser creates a parser for optional message/systemd unit regex extraction.
// If both regexes are empty, returns nil (no parsing).
func NewRecordParser(messageRegex string, noMatch NoMatchAction, systemdUnitRegex string) (MessageParser, error) {
	messageRe, messageGroupNames, err := compileNamedRegex(messageRegex)
	if err != nil {
		return nil, err
	}
	systemdUnitRe, systemdUnitGroupNames, err := compileNamedRegex(systemdUnitRegex)
	if err != nil {
		return nil, err
	}
	if messageRe == nil && systemdUnitRe == nil {
		return nil, nil
	}
	return &messageParser{
		messageRe:             messageRe,
		messageGroupNames:     messageGroupNames,
		systemdUnitRe:         systemdUnitRe,
		systemdUnitGroupNames: systemdUnitGroupNames,
		noMatch:               noMatch,
	}, nil
}

func compileNamedRegex(regexStr string) (*regexp.Regexp, []string, error) {
	if regexStr == "" {
		return nil, nil, nil
	}
	re, err := regexp.Compile(regexStr)
	if err != nil {
		return nil, nil, err
	}
	names := re.SubexpNames()
	groupNames := make([]string, 0, len(names))
	for _, n := range names {
		if n != "" {
			groupNames = append(groupNames, n)
		}
	}
	return re, groupNames, nil
}

func (p *messageParser) Parse(rec models.Record) (models.Record, bool) {
	out := rec

	if p.messageRe != nil {
		matches := p.messageRe.FindStringSubmatch(rec.Message)
		if matches == nil {
			if p.noMatch == NoMatchSkip {
				return models.Record{}, false
			}
		} else {
			p.extractNamedGroups(&out, p.messageRe, p.messageGroupNames, matches)
		}
	}

	if p.systemdUnitRe != nil {
		matches := p.systemdUnitRe.FindStringSubmatch(rec.SystemdUnit)
		if matches != nil {
			p.extractNamedGroups(&out, p.systemdUnitRe, p.systemdUnitGroupNames, matches)
		}
	}

	return out, true
}

func (p *messageParser) extractNamedGroups(rec *models.Record, re *regexp.Regexp, names []string, matches []string) {
	if rec.Parsed == nil {
		rec.Parsed = make(map[string]string)
	}
	for _, name := range names {
		idx := re.SubexpIndex(name)
		if idx >= 0 && idx < len(matches) && matches[idx] != "" {
			if _, exists := rec.Parsed[name]; exists {
				continue
			}
			rec.Parsed[name] = matches[idx]
		}
	}
}
