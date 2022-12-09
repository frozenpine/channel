package pipeline

import (
	"testing"
	"time"

	"github.com/frozenpine/msgqueue/core"
	"github.com/pkg/errors"
)

func TestSequence(t *testing.T) {
	var seq Sequence[time.Time, Trade] = NewTradeSequence(nil)

	t.Log(seq, seq.IsWaterMark())
}

func TestKBarIndex(t *testing.T) {
	// 2022-12-09 10:42:56.497 2022-12-09 10:44:00.000

	ts1, _ := time.Parse("2006-01-02 15:04:05.000", "2022-12-09 10:42:56.497")

	idx := ts1.Round(kbarGap)

	t.Log(ts1.UnixNano(), idx.UnixNano())

	t.Log(idx, core.TimeCompare(ts1, idx))

	for idx := 0; idx < 100; idx++ {
		now := time.Now()
		index := now.Round(kbarGap)
		if core.TimeCompare(now, index) >= 0 {
			index = index.Add(kbarGap)
		}

		t.Log(now.Format("2006-01-02 15:04:05.000"), index.Format("2006-01-02 15:04:05.000"))

		<-time.After(time.Millisecond * 500)
	}
}

func TestErrCheck(t *testing.T) {
	err := errors.Wrap(ErrFutureTick, "test")

	if !errors.Is(err, ErrFutureTick) {
		t.Fatal(err)
	}
}
