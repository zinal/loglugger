package client

import (
	"sync/atomic"
	"time"
)

// SeqNoGenerator produces monotonically increasing sequence numbers.
type SeqNoGenerator interface {
	Next() int64
}

type seqNoGenerator struct {
	next uint64
}

// NewSeqNoGenerator creates a sequence generator starting from startup milliseconds since epoch.
func NewSeqNoGenerator(startup time.Time) SeqNoGenerator {
	return &seqNoGenerator{
		next: uint64(startup.UnixMilli()),
	}
}

func (g *seqNoGenerator) Next() int64 {
	return int64(atomic.AddUint64(&g.next, 1) - 1)
}
