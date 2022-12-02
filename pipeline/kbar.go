package pipeline

import (
	"runtime"
	"sync"
	"time"
)

var (
	tradeSequencePool = sync.Pool{New: func() any { return &TradeSequence{} }}
)

type Trade interface {
	Price() float64
	Volume() int
	TradeID() string
	TradeTime() time.Time

	Data[Trade]
}

type TradeSequence struct {
	data Trade
	ts   time.Time
}

func NewTradeSequence(v Trade) *TradeSequence {
	result := tradeSequencePool.Get().(*TradeSequence)

	if v != nil {
		result.data = v
	}

	result.ts = time.Now()

	runtime.SetFinalizer(result, tradeSequencePool.Put)

	return result
}

func (td *TradeSequence) Index() time.Time {
	return td.ts
}

func (td *TradeSequence) Value() Trade {
	return td.data
}

func (td *TradeSequence) IsSame(right Sequence[time.Time, Trade]) bool {
	if td.IsWaterMark() || right.IsWaterMark() {
		return td.ts.Equal(right.Index())
	}

	return td.data.TradeID() == right.Value().TradeID()
}

func (td *TradeSequence) IsBefore(right Sequence[time.Time, Trade]) bool {
	if td.IsWaterMark() || right.IsWaterMark() {
		return td.ts.Before(right.Index())
	}

	return td.data.TradeID() < right.Value().TradeID()
}

func (td *TradeSequence) IsAfter(right Sequence[time.Time, Trade]) bool {
	if td.IsWaterMark() || right.IsWaterMark() {
		return td.ts.After(right.Index())
	}

	return td.data.TradeID() > right.Value().TradeID()
}

func (td *TradeSequence) IsSameOrBefore(right Sequence[time.Time, Trade]) bool {
	return td.IsSame(right) || td.IsBefore(right)
}

func (td *TradeSequence) IsSameOrAfter(right Sequence[time.Time, Trade]) bool {
	return td.IsSame(right) || td.IsAfter(right)
}

func (td *TradeSequence) IsWaterMark() bool {
	return td.data == nil
}

type KBar struct {
	High  Trade
	Open  Trade
	Low   Trade
	Close Trade
}

type KBarChan[S any, V Data[V]] struct {
	buffer []Sequence[S, V]
}
