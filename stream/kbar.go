package stream

import (
	"context"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/frozenpine/msgqueue/core"
	"github.com/frozenpine/msgqueue/pipeline"
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
	data        []*TradeSequence
	totalVolume int
	max, min    Trade
	precise     time.Duration
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
	bar.precise = gap

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

func (k *KBarWindow) Indexs() []time.Time {
	indexes := make([]time.Time, len(k.data))

	for idx, td := range k.data {
		indexes[idx] = td.Index()
	}

	sort.SliceStable(indexes, func(i, j int) bool {
		return indexes[i].Before(indexes[j])
	})

	return indexes
}

func (k *KBarWindow) Values() []Trade {
	values := make([]Trade, len(k.data))

	for idx, td := range k.data {
		values[idx] = td
	}

	sort.Slice(values, func(i, j int) bool {
		return strings.Compare(values[i].Identity(), values[j].Identity()) < 0
	})

	return values
}

func (k *KBarWindow) Series() []pipeline.Sequence[time.Time, Trade] {
	series := make([]pipeline.Sequence[time.Time, Trade], len(k.data))

	for idx, td := range k.data {
		series[idx] = td
	}

	sort.SliceStable(series, func(i, j int) bool {
		return series[i].Index().Before(series[j].Index())
	})

	return series
}

func (k *KBarWindow) Push(v pipeline.Sequence[time.Time, Trade]) error {
	if v == nil {
		return nil
	}

	td := v.Value()

	if core.TimeCompare(k.index, td.TradeTime()) < 0 {
		return errors.Wrap(ErrFutureTick, "trade ts after current bar")
	}

	if core.TimeCompare(k.index.Add(-k.precise), td.TradeTime()) >= 0 {
		return errors.Wrap(ErrHistoryTick, "trade ts before current bar")
	}

	k.data = append(k.data, v.(*TradeSequence))

	k.totalVolume += td.Volume()

	if k.max == nil || td.Price() > k.max.Price() {
		k.max = td
	}

	if k.min == nil || td.Price() < k.min.Price() {
		k.min = td
	}

	return nil
}

func (k *KBarWindow) NextWindow() Window[time.Time, Trade, time.Time, KBar] {
	return NewKBarWindow(k, k.preSettle, k.precise)
}

func (k *KBarWindow) IsWaterMark() bool { return true }

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
	return k.precise
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

type KBarStream struct {
	MemoStream[time.Time, Trade, time.Time, KBar, string]
}

func NewKBarStream(ctx context.Context, name string, preSettle float64, gap time.Duration) *KBarStream {
	stream := KBarStream{}

	stream.Init(ctx, name, func() {
		stream.pipeline = pipeline.NewMemoPipeLine(ctx, name, stream.convert)
		stream.currWindow = NewKBarWindow(nil, preSettle, Min1BarGap)

		stream.aggregator = func(
			w Window[time.Time, Trade, time.Time, KBar],
		) (pipeline.Sequence[time.Time, KBar], error) {
			if bar, ok := w.(*KBarWindow); ok {
				return bar, nil
			} else {
				return nil, errors.New("not Kbar window")
			}
		}
	})

	return &stream
}
