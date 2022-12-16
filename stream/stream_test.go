package stream

import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/frozenpine/msgqueue/core"
	"github.com/pkg/errors"
)

func TestErrCheck(t *testing.T) {
	if !errors.Is(ErrWindowClosed, ErrWindowClosed) {
		t.Fatal("base error")
	}

	err := errors.Wrap(ErrWindowClosed, "clise")

	if !errors.Is(err, ErrWindowClosed) {
		t.Fatal(err)
	}

	err = errors.Wrap(ErrHistorySequence, "history")
	if !errors.Is(err, ErrHistorySequence) {
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
		time.Time, int, float64,
		string,
	](
		context.TODO(), "MemoStream", nil,
		func(in Window[
			time.Time, int, float64,
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
		if idx%3 == 2 {
			if err = stream.Publish(&sequence[int]{
				ts:   time.Now(),
				mark: true,
			}, -1); err != nil {
				t.Fatalf("input water mark failed: %+v", err)
			}
		}

		if err = stream.Publish(&sequence[int]{
			data: idx,
			ts:   time.Now(),
		}, -1); err != nil {
			t.Fatalf("input data failed: %+v", err)
		}
	}

	stream.Release()
	stream.Join()

	wg.Wait()
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

func TestKBar(t *testing.T) {
	priceList := []float64{
		1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008,
		1009, 1010, 1011, 1012, 1013, 1014, 1015,
	}
	priceLen := len(priceList)

	src := rand.NewSource(time.Now().UnixNano())

	stream := NewKBarStream(context.TODO(), "Kbar", 1006, Min1BarGap)
	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		subID, ch := stream.Subscribe("test", core.Quick)

		defer func() {
			stream.UnSubscribe(subID)
			wg.Done()
		}()

		for seq := range ch {
			bar := seq.Value()

			t.Log(
				seq.Index().Format("2006-01-02 15:04:05.000"),
				bar.Volume(),
				bar.Open(), bar.High(),
				bar.Low(), bar.Close(),
			)
		}
	}()

	var multple time.Duration = 0

	for idx := 0; idx < 100; idx++ {
		if idx%3 == 2 {
			multple++
		}

		price := priceList[src.Int63()%int64(priceLen)]

		td := trade{
			price:  price,
			volume: 1,
			ts:     time.Now().Add(Min1BarGap * multple),
		}

		tds := NewTradeSequence(&td)

		if err := stream.Publish(tds, -1); err != nil {
			t.Fatal(err)
		}
	}

	stream.Release()
	stream.Join()

	wg.Wait()
}
