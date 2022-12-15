package stream

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/frozenpine/msgqueue/core"
	"github.com/pkg/errors"
)

func TestErrCheck(t *testing.T) {
	err := errors.Wrap(ErrFutureTick, "test")

	if !errors.Is(err, ErrFutureTick) {
		t.Fatal(err)
	}
}

func TestKBarIndex(t *testing.T) {
	// 2022-12-09 10:42:56.497 2022-12-09 10:44:00.000

	ts1, _ := time.Parse("2006-01-02 15:04:05.000", "2022-12-09 10:42:56.497")

	idx := ts1.Round(Min1BarGap)

	t.Log(ts1.UnixNano(), idx.UnixNano())

	t.Log(idx, core.TimeCompare(ts1, idx))

	for idx := 0; idx < 100; idx++ {
		now := time.Now()
		index := now.Round(Min1BarGap)
		if core.TimeCompare(now, index) >= 0 {
			index = index.Add(Min1BarGap)
		}

		t.Log(now.Format("2006-01-02 15:04:05.000"), index.Format("2006-01-02 15:04:05.000"))

		<-time.After(time.Millisecond * 500)
	}
}

type trade struct {
	price  float64
	volume int
	ts     time.Time
}

func (td *trade) Identity() string     { return td.ts.Format("2006-01-02 15:04:05.000") }
func (td *trade) Price() float64       { return td.price }
func (td *trade) Volume() int          { return td.volume }
func (td *trade) TradeTime() time.Time { return td.ts }

type sequence[V int | float64] struct {
	data V
	ts   time.Time
	mark bool
}

func (s *sequence[V]) Value() V         { return s.data }
func (s *sequence[V]) Index() time.Time { return s.ts }
func (s *sequence[V]) Compare(than Sequence[time.Time, V]) int {
	v := s.data - than.Value()

	switch {
	case v > 0:
		return 1
	case v < 0:
		return -1
	default:
		return 0
	}
}
func (s *sequence[V]) IsWaterMark() bool { return s.mark }

func TestMemoStream(t *testing.T) {
	stream, err := NewMemoStream[
		time.Time, int,
		time.Time, float64,
		string,
	](
		context.TODO(), "MemoStream", nil,
		func(in Window[
			time.Time, int,
			time.Time, float64,
		]) (Sequence[time.Time, float64], error) {
			var result float64

			for _, v := range in.Series() {
				result += float64(v.Value()) * 0.5
			}

			return &sequence[float64]{data: result, ts: time.Now()}, nil
		},
	)

	if err != nil {
		t.Fatal(err)
	}

	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		subID, ch := stream.Subscribe("test1", core.Quick)

		defer func() {
			stream.UnSubscribe(subID)
			wg.Done()
		}()

		for out := range ch {
			t.Log(out.Index(), out.Value())
		}
	}()

	<-time.After(time.Second)
	for idx := 0; idx < 200; idx++ {
		if err := stream.Publish(&sequence[int]{
			data: idx,
			ts:   time.Now(),
			mark: idx%3 == 0,
		}, -1); err != nil {
			t.Fatal(err)
		}
	}

	stream.Release()
	stream.Join()

	wg.Wait()
}
