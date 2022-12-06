package pipeline

import (
	"log"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/frozenpine/msgqueue/channel"
	"github.com/frozenpine/msgqueue/core"
)

var (
	tradeSequencePool = sync.Pool{New: func() any { return &TradeSequence{} }}
)

type Trade interface {
	InvestorID() string
	ExchangeID() string
	InstrumentID() string
	Price() float64
	Volume() int
	TradeID() string
	TradeTime() time.Time
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

func (td *TradeSequence) Compare(than Sequence[time.Time, Trade]) int {
	if td.IsWaterMark() || than.IsWaterMark() {
		return core.TimeCompare(td.ts, than.Index())
	}

	return strings.Compare(td.data.TradeID(), than.Value().TradeID())
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

type SequenceSlice[S comparable, V any] []Sequence[S, V]

type KBarChan struct {
	buffer     SequenceSlice[time.Time, Trade]
	inputChan  channel.Channel[Trade]
	outputChan channel.Channel[*KBar]
}

func (ch *KBarChan) dispatcher() {
	subID, in := ch.inputChan.Subscribe("kbar", core.Quick)

	log.Printf("Start trade consumer[kbar]: %v", subID)

	for v := range in {
		tdSeq := NewTradeSequence(v)
		if tdSeq.IsWaterMark() {
			// TODO: aggreate
		} else {
			ch.buffer = append(ch.buffer, tdSeq)
		}
	}
}
