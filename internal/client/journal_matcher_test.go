//go:build linux

package client

import "testing"

func TestNewServiceMatcherModes(t *testing.T) {
	t.Parallel()

	all, exact, err := newServiceMatcher("")
	if err != nil {
		t.Fatalf("empty mask error = %v", err)
	}
	if exact != "" {
		t.Fatalf("exact match for empty mask = %q, want \"\"", exact)
	}
	if !all.Match("any.service") {
		t.Fatal("empty mask should match all units")
	}

	exactMatcher, exactValue, err := newServiceMatcher("nginx.service")
	if err != nil {
		t.Fatalf("exact mask error = %v", err)
	}
	if exactValue != "nginx.service" {
		t.Fatalf("exact value = %q, want nginx.service", exactValue)
	}
	if !exactMatcher.Match("nginx.service") || exactMatcher.Match("apache.service") {
		t.Fatal("exact matcher mismatch")
	}

	globMatcher, exactValue, err := newServiceMatcher("ydb*.service")
	if err != nil {
		t.Fatalf("glob mask error = %v", err)
	}
	if exactValue != "" {
		t.Fatalf("glob exact value = %q, want \"\"", exactValue)
	}
	if !globMatcher.Match("ydbd.service") || globMatcher.Match("nginx.service") {
		t.Fatal("glob matcher mismatch")
	}

	regexMatcher, exactValue, err := newServiceMatcher("regex:^foo-[0-9]+\\.service$")
	if err != nil {
		t.Fatalf("regex mask error = %v", err)
	}
	if exactValue != "" {
		t.Fatalf("regex exact value = %q, want \"\"", exactValue)
	}
	if !regexMatcher.Match("foo-12.service") || regexMatcher.Match("foo.service") {
		t.Fatal("regex matcher mismatch")
	}

	if _, _, err := newServiceMatcher("regex:["); err == nil {
		t.Fatal("expected invalid regex error")
	}
}
