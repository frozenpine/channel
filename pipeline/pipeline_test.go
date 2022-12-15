package pipeline

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/frozenpine/msgqueue/core"
)

type seq[T int | float64] struct {
	v  T
	ts time.Time
}

func (seq *seq[T]) Value() T         { return seq.v }
func (seq *seq[T]) Index() time.Time { return seq.ts }
func (seq *seq[T]) Compare(than Sequence[time.Time, T]) int {
	return core.TimeCompare(seq.ts, than.Index())
}
func (seq *seq[T]) IsWaterMark() bool { return false }

func TestMemoPipeline(t *testing.T) {
	line := NewMemoPipeLine(
		context.TODO(), "test",
		func(s Sequence[time.Time, int], c core.Producer[Sequence[time.Time, float64]]) error {
			o := float64(s.Value()) * 0.5

			return c.Publish(&seq[float64]{v: o, ts: s.Index()}, -1)
		},
	)

	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		subID, ch := line.Subscribe("test1", core.Quick)

		defer func() {
			line.UnSubscribe(subID)
			wg.Done()
		}()

		for v := range ch {
			t.Log("output:", v.Index(), v.Value())
		}
	}()

	for idx := 0; idx < 100; idx++ {
		line.Publish(&seq[int]{v: idx, ts: time.Now()}, -1)
	}

	line.Release()

	line.Join()

	wg.Wait()
}
