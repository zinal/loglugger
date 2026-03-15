package server

import "testing"

func TestCompileAllowedMatcherRejectsInvalidRegex(t *testing.T) {
	_, err := compileAllowedMatcher([]string{"regex:("})
	if err == nil {
		t.Fatalf("expected invalid regex error")
	}
}

func TestCompileAllowedMatcherMatchesExactAndRegex(t *testing.T) {
	matcher, err := compileAllowedMatcher([]string{"exact-cn", "regex:^svc-[0-9]+$"})
	if err != nil {
		t.Fatalf("compile matcher failed: %v", err)
	}
	if !matcher.Match("exact-cn") {
		t.Fatalf("expected exact match to pass")
	}
	if !matcher.Match("svc-42") {
		t.Fatalf("expected regex match to pass")
	}
	if matcher.Match("nope") {
		t.Fatalf("expected unmatched value to fail")
	}
}
