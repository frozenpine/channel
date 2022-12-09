package pipeline

import (
	"log"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/frozenpine/msgqueue/channel"
	"github.com/frozenpine/msgqueue/core"
	"github.com/pkg/errors"
)

const (
	kbarGap = time.Minute
)

var (
	tradeSequencePool = sync.Pool{New: func() any { return &TradeSequence{} }}
	kbarSequencePool  = sync.Pool{New: func() any { return &KBarSequence{} }}
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
	Trade
	watermark bool
	ts        time.Time
}

func NewTradeSequence(v Trade) *TradeSequence {
	result := tradeSequencePool.Get().(*TradeSequence)

	result.Trade = v
	result.ts = time.Now()

	runtime.SetFinalizer(result, tradeSequencePool.Put)

	return result
}

func NewTradeWarterMark(v Trade) *TradeSequence {
	td := NewTradeSequence(v)
	td.watermark = true
	return td
}

func (td *TradeSequence) Index() time.Time {
	return td.ts
}

func (td *TradeSequence) Value() Trade {
	return td.Trade
}

func (td *TradeSequence) Compare(than Sequence[time.Time, Trade]) int {
	if td.IsWaterMark() || than.IsWaterMark() {
		return core.TimeCompare(td.ts, than.Index())
	}

	return strings.Compare(td.Trade.TradeID(), than.Value().TradeID())
}

func (td *TradeSequence) IsWaterMark() bool {
	return td.Trade == nil || td.watermark
}

type KBar interface {
	High() float64
	Low() float64
	Open() float64
	Close() float64
	Volume() int
	Index() time.Time
}

type KBarSequence struct {
	preBar      KBar
	preSettle   float64
	data        []Trade
	totalVolume int
	max, min    Trade
	index       time.Time
}

func NewKBarSequence(preBar *KBarSequence, preSettle float64) *KBarSequence {
	bar := kbarSequencePool.Get().(*KBarSequence)

	bar.preBar = preBar
	bar.preSettle = preSettle

	if bar.preBar != nil {
		bar.index = preBar.index.Add(kbarGap)
	} else {
		now := time.Now()
		index := now.Round(kbarGap)
		if core.TimeCompare(now, index) >= 0 {
			index = index.Add(kbarGap)
		}
		bar.index = index
	}

	runtime.SetFinalizer(bar, kbarSequencePool.Put)

	return bar
}

func (k *KBarSequence) Push(v Trade) error {
	if v == nil {
		return nil
	}

	if core.TimeCompare(k.index, v.TradeTime()) < 0 {
		return errors.New("out of sequence range")
	}

	k.totalVolume += v.Volume()
	k.data = append(k.data, v)

	if k.max == nil || v.Price() > k.max.Price() {
		k.max = v
	}

	if k.min == nil || v.Price() < k.min.Price() {
		k.min = v
	}

	return nil
}

func (k *KBarSequence) getPrePrice() float64 {
	if k.preBar != nil {
		return k.preBar.Close()
	}

	return k.preSettle
}

func (k *KBarSequence) High() float64 {
	if len(k.data) == 0 {
		return k.getPrePrice()
	}

	return k.max.Price()
}

func (k *KBarSequence) Low() float64 {
	if len(k.data) == 0 {
		return k.getPrePrice()
	}

	return k.min.Price()
}

func (k *KBarSequence) Open() float64 {
	if len(k.data) == 0 {
		return k.getPrePrice()
	}

	return k.data[0].Price()
}

func (k *KBarSequence) Close() float64 {
	if len(k.data) == 0 {
		return k.getPrePrice()
	}

	return k.data[len(k.data)-1].Price()
}

func (k *KBarSequence) Volume() int {
	return k.totalVolume
}

func (k *KBarSequence) Index() time.Time {
	return k.index
}

func (k *KBarSequence) Value() KBar {
	return k
}

func (k *KBarSequence) IsWarterMark() bool {
	return k.index.UnixMilli()%500 == 0
}

func (k *KBarSequence) Compare(than Sequence[time.Time, KBar]) int {
	return core.TimeCompare(k.index, than.Index())
}

type KBarPipeline struct {
	inputChan  channel.Channel[Trade]
	outputChan channel.Channel[*KBar]
}

func (ch *KBarPipeline) dispatcher() {
	subID, in := ch.inputChan.Subscribe("kbar", core.Quick)

	log.Printf("Start trade consumer[kbar]: %v", subID)

	for v := range in {
		tdSeq := NewTradeSequence(v)
		if tdSeq.IsWaterMark() {
			// TODO: aggreate
		}
	}
}
