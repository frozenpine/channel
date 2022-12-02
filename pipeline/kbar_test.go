package pipeline

import (
	"testing"
	"time"
)

func TestSequence(t *testing.T) {
	var seq Sequence[time.Time, Trade] = NewTradeSequence(nil)

	t.Log(seq, seq.IsWaterMark())
}
