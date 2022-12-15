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

func TestKBarStream(t *testing.T) {
	gap := time.Millisecond * 500
	tradePrice := []float64{
		1000, 1100, 1200, 1300, 1400, 1500, 1600, 1700, 1800, 1900, 2000,
	}
	priceLen := len(tradePrice)
	src := rand.NewSource(time.Now().UnixNano())

	stream := NewKBarStream(context.TODO(), "test", 10000.0, Min1BarGap)

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		subID, ch := stream.Subscribe("kbar_consumer", core.Quick)

		defer func() {
			stream.UnSubscribe(subID)
			wg.Done()
		}()

		for bar := range ch {
			t.Log("BAR:", bar)
		}
	}()

	tdList := []Trade{}
	now := time.Now()

	for idx := 0; idx < 200; idx++ {
		v := int(src.Int63()) % priceLen
		td := trade{
			price:  tradePrice[v],
			volume: v,
			ts:     now.Add(gap * time.Duration(idx)),
		}

		tdList = append(tdList, &td)

		seq := NewTradeSequence(&td)
		seq.ts = td.ts

		stream.Publish(seq, -1)
	}

	stream.Release()
	stream.Join()

	wg.Wait()

	for _, v := range tdList {
		t.Log("TD:", v)
	}
}
