package client

import (
	"regexp"
	"strings"

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
	messageGroups         []namedGroup
	messageBodyGroupIndex int
	systemdUnitRe         *regexp.Regexp
	systemdUnitGroups     []namedGroup
	noMatch               NoMatchAction
}

type namedGroup struct {
	name  string
	index int
}

// NewRecordParser creates a parser for optional message/systemd unit regex extraction.
// If both regexes are empty, returns nil (no parsing).
func NewRecordParser(messageRegex string, noMatch NoMatchAction, systemdUnitRegex string) (MessageParser, error) {
	messageRe, messageGroups, err := compileNamedRegex(messageRegex)
	if err != nil {
		return nil, err
	}
	systemdUnitRe, systemdUnitGroups, err := compileNamedRegex(systemdUnitRegex)
	if err != nil {
		return nil, err
	}
	if messageRe == nil && systemdUnitRe == nil {
		return nil, nil
	}
	messageBodyGroupIndex := -1
	if messageRe != nil {
		messageBodyGroupIndex = messageRe.SubexpIndex("P_MESSAGE")
	}
	return &messageParser{
		messageRe:             messageRe,
		messageGroups:         messageGroups,
		messageBodyGroupIndex: messageBodyGroupIndex,
		systemdUnitRe:         systemdUnitRe,
		systemdUnitGroups:     systemdUnitGroups,
		noMatch:               noMatch,
	}, nil
}

func compileNamedRegex(regexStr string) (*regexp.Regexp, []namedGroup, error) {
	if regexStr == "" {
		return nil, nil, nil
	}
	re, err := regexp.Compile(regexStr)
	if err != nil {
		return nil, nil, err
	}
	names := re.SubexpNames()
	groups := make([]namedGroup, 0, len(names))
	for i, n := range names {
		if n != "" {
			groups = append(groups, namedGroup{name: n, index: i})
		}
	}
	return re, groups, nil
}

func (p *messageParser) Parse(rec models.Record) (models.Record, bool) {
	out := rec

	if p.messageRe != nil {
		if !p.parseMessage(&out, rec.Message) {
			if p.noMatch == NoMatchSkip {
				return models.Record{}, false
			}
		}
	}

	if p.systemdUnitRe != nil {
		matches := p.systemdUnitRe.FindStringSubmatch(rec.SystemdUnit)
		if matches != nil {
			p.extractNamedGroups(&out, p.systemdUnitGroups, matches)
		}
	}

	return out, true
}

func (p *messageParser) parseMessage(out *models.Record, message string) bool {
	matches := p.messageRe.FindStringSubmatch(message)
	if matches != nil {
		p.extractNamedGroups(out, p.messageGroups, matches)
		return true
	}

	firstLine, extraLines, hasExtra := splitFirstLine(message)
	if !hasExtra {
		return false
	}
	matches = p.messageRe.FindStringSubmatch(firstLine)
	if matches == nil {
		return false
	}
	p.extractNamedGroups(out, p.messageGroups, matches)
	if extraLines != "" && p.messageBodyGroupIndex >= 0 && p.messageBodyGroupIndex < len(matches) {
		if out.Parsed == nil {
			out.Parsed = make(map[string]string)
		}
		base := matches[p.messageBodyGroupIndex]
		if base == "" {
			out.Parsed["P_MESSAGE"] = extraLines
		} else {
			out.Parsed["P_MESSAGE"] = base + "\n" + extraLines
		}
	}
	return true
}

func splitFirstLine(message string) (string, string, bool) {
	idx := strings.IndexByte(message, '\n')
	if idx < 0 {
		return message, "", false
	}
	return message[:idx], message[idx+1:], true
}

func (p *messageParser) extractNamedGroups(rec *models.Record, groups []namedGroup, matches []string) {
	if rec.Parsed == nil {
		rec.Parsed = make(map[string]string)
	}
	for _, group := range groups {
		idx := group.index
		if idx >= 0 && idx < len(matches) && matches[idx] != "" {
			if _, exists := rec.Parsed[group.name]; exists {
				continue
			}
			rec.Parsed[group.name] = matches[idx]
		}
	}
}
