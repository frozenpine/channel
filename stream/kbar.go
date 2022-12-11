package stream

import (
	"context"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/frozenpine/msgqueue/core"
	"github.com/frozenpine/msgqueue/pipeline"
	"github.com/gofrs/uuid"
	"github.com/pkg/errors"
)

const (
	Min1BarGap = time.Minute
	Min5BarGap = time.Minute * 5
	HourBarGap = time.Hour
	DayBarGap  = time.Hour * 24
)

var (
	tradeSequencePool = sync.Pool{New: func() any { return &TradeSequence{} }}
	kbarSequencePool  = sync.Pool{New: func() any { return &KBarWindow{} }}
)

type Trade interface {
	// 必须单调有序
	Identity() string
	Price() float64
	Volume() int
	TradeTime() time.Time
}

type TradeSequence struct {
	Trade
	ts time.Time
}

func NewTradeSequence(v Trade) *TradeSequence {
	result := tradeSequencePool.Get().(*TradeSequence)

	result.Trade = v
	result.ts = time.Now()

	runtime.SetFinalizer(result, tradeSequencePool.Put)

	return result
}

func (td *TradeSequence) Index() time.Time {
	return td.ts
}

func (td *TradeSequence) Value() Trade {
	return td.Trade
}

func (td *TradeSequence) Compare(than pipeline.Sequence[time.Time, Trade]) int {
	if td.IsWaterMark() || than.IsWaterMark() {
		return core.TimeCompare(td.ts, than.Index())
	}

	return strings.Compare(td.Trade.Identity(), than.Value().Identity())
}

func (td *TradeSequence) IsWaterMark() bool {
	return td.Trade == nil
}

type KBar interface {
	High() float64
	Low() float64
	Open() float64
	Close() float64
	Volume() int
	Precise() time.Duration
	Index() time.Time
}

type KBarWindow struct {
	preBar      KBar
	preSettle   float64
	data        []Trade
	totalVolume int
	max, min    Trade
	gap         time.Duration
	index       time.Time
}

func NewKBarWindow(preBar *KBarWindow, preSettle float64, gap time.Duration) *KBarWindow {
	if gap < Min1BarGap {
		gap = Min1BarGap
	}

	gap = gap.Round(Min1BarGap)

	bar := kbarSequencePool.Get().(*KBarWindow)

	bar.preBar = preBar
	bar.preSettle = preSettle
	bar.gap = gap

	if bar.preBar != nil {
		bar.index = preBar.index.Add(gap)
	} else {
		now := time.Now()
		index := now.Round(gap)
		if core.TimeCompare(now, index) >= 0 {
			index = index.Add(gap)
		}
		bar.index = index
	}

	// to prevent dirty data in history
	bar.data = bar.data[:0]

	runtime.SetFinalizer(bar, kbarSequencePool.Put)

	return bar
}

func (k *KBarWindow) Push(v Trade) error {
	if v == nil {
		return nil
	}

	if core.TimeCompare(k.index, v.TradeTime()) < 0 {
		return errors.Wrap(ErrFutureTick, "trade ts after current bar")
	}

	if core.TimeCompare(k.index.Add(-k.gap), v.TradeTime()) >= 0 {
		return errors.Wrap(ErrHistoryTick, "trade ts before current bar")
	}

	k.data = append(k.data, v)

	k.totalVolume += v.Volume()

	if k.max == nil || v.Price() > k.max.Price() {
		k.max = v
	}

	if k.min == nil || v.Price() < k.min.Price() {
		k.min = v
	}

	return nil
}

func (k *KBarWindow) getPrePrice() float64 {
	if k.preBar != nil {
		return k.preBar.Close()
	}

	return k.preSettle
}

func (k *KBarWindow) High() float64 {
	if len(k.data) == 0 {
		return k.getPrePrice()
	}

	return k.max.Price()
}

func (k *KBarWindow) Low() float64 {
	if len(k.data) == 0 {
		return k.getPrePrice()
	}

	return k.min.Price()
}

func (k *KBarWindow) Open() float64 {
	if len(k.data) == 0 {
		return k.getPrePrice()
	}

	return k.data[0].Price()
}

func (k *KBarWindow) Close() float64 {
	if len(k.data) == 0 {
		return k.getPrePrice()
	}

	return k.data[len(k.data)-1].Price()
}

func (k *KBarWindow) Volume() int {
	return k.totalVolume
}

func (k *KBarWindow) Index() time.Time {
	return k.index
}

func (k *KBarWindow) Precise() time.Duration {
	return k.gap
}

func (k *KBarWindow) Value() KBar {
	return k
}

func (k *KBarWindow) IsWarterMark() bool {
	return k.index.UnixMilli()%500 == 0
}

func (k *KBarWindow) Compare(than pipeline.Sequence[time.Time, KBar]) int {
	return core.TimeCompare(k.index, than.Index())
}

type KBarSource interface {
	Trade
	KBar
}

type KBarStream[I KBarSource] struct {
	stream MemoStream[time.Time, I, time.Time, KBar, string]

	currBar *KBarWindow
}

func NewKBarStream[I KBarSource](ctx context.Context, name string, preSettle float64, gap time.Duration) *KBarStream {
	stream := KBarStream[I]{}

	stream.Init(ctx, name, func() {
		// TODO: extra init
		// stream.aggregator =
		// stream.currBar =
	})

	return &stream
}

func (stm *KBarStream[I]) Init(ctx context.Context, name string, extraInit func()) {
	stm.stream.Init(ctx, name, extraInit)
}

func (stm *KBarStream[I]) Name() string {
	return stm.stream.Name()
}

func (stm *KBarStream[I]) ID() uuid.UUID {
	return stm.stream.ID()
}
