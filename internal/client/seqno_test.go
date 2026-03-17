package client

import (
	"testing"
	"time"
)

func TestSeqNoGeneratorStartsFromStartupMillis(t *testing.T) {
	startup := time.UnixMilli(1710345600123)
	gen := NewSeqNoGenerator(startup)

	first := gen.Next()
	second := gen.Next()

	if first != startup.UnixMilli() {
		t.Fatalf("first seqno = %d, want %d", first, startup.UnixMilli())
	}
	if second != first+1 {
		t.Fatalf("second seqno = %d, want %d", second, first+1)
	}
}
