package stream

import (
	"context"
	"log"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/frozenpine/msgqueue/channel"
	"github.com/frozenpine/msgqueue/core"
	"github.com/frozenpine/msgqueue/pipeline"
	"github.com/gofrs/uuid"
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

	// to prevent dirty data in history
	bar.data = bar.data[:0]

	runtime.SetFinalizer(bar, kbarSequencePool.Put)

	return bar
}

func (k *KBarSequence) Push(v Trade) error {
	if v == nil {
		return nil
	}

	if core.TimeCompare(k.index, v.TradeTime()) < 0 {
		return errors.Wrap(ErrFutureTick, "trade ts after current bar")
	}

	if core.TimeCompare(k.index.Add(-kbarGap), v.TradeTime()) >= 0 {
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

func (k *KBarSequence) Compare(than pipeline.Sequence[time.Time, KBar]) int {
	return core.TimeCompare(k.index, than.Index())
}

type KBarPipeline struct {
	name     string
	id       uuid.UUID
	ctx      context.Context
	cancelFn context.CancelFunc

	inputChan  channel.Channel[*TradeSequence]
	outputChan channel.Channel[KBar]

	currBar *KBarSequence
}

func NewKBarPipeline(ctx context.Context, name string, preSettle float64) *KBarPipeline {
	if ctx == nil {
		ctx = context.Background()
	}

	if name == "" {
		name = core.GenName("KBarPipe")
	}

	pipe := KBarPipeline{
		name:    name,
		id:      core.GenID(name),
		currBar: NewKBarSequence(nil, preSettle),
	}
	pipe.ctx, pipe.cancelFn = context.WithCancel(ctx)
	pipe.id = core.GenID(pipe.name)

	go pipe.dispatcher()

	return &pipe
}

func (ch *KBarPipeline) Name() string {
	return ch.name
}

func (ch *KBarPipeline) ID() uuid.UUID {
	return ch.id
}

func (ch *KBarPipeline) dispatcher() {
	subID, in := ch.inputChan.Subscribe("kbar", core.Quick)

	log.Printf("Start trade consumer[kbar]: %v", subID)

	var err error

	for seq := range in {
		if err = ch.currBar.Push(seq.Trade); err == nil {
			continue
		}

		switch errors.Unwrap(err) {
		case ErrFutureTick:
			ch.outputChan.Publish(ch.currBar, -1)
			ch.currBar = NewKBarSequence(ch.currBar, ch.currBar.preSettle)
		case ErrHistoryTick:
			log.Printf("Received history data: %+v, %+v", seq, ch.currBar)
		default:
			log.Printf("Unhandled err occoured: %+v", err)
		}
	}
}

func (ch *KBarPipeline) Publish(v Trade, timeout time.Duration) error {
	return ch.inputChan.Publish(NewTradeSequence(v), timeout)
}

func (ch *KBarPipeline) Subscribe(name string, resume core.ResumeType) (uuid.UUID, <-chan KBar) {
	return ch.outputChan.Subscribe(name, resume)
}

func (ch *KBarPipeline) UnSubscribe(subID uuid.UUID) error {
	return ch.outputChan.UnSubscribe(subID)
}

func (ch *KBarPipeline) PipelineUpStream(src core.Consumer[Trade]) error {
	if src == nil {
		return errors.Wrap(core.ErrPipeline, "upstream empty")
	}

	subID, upChan := src.Subscribe(ch.name, core.Quick)

	go func() {
		defer src.UnSubscribe(subID)

		for {
			select {
			case <-ch.ctx.Done():
				return
			case td := <-upChan:
				if td == nil {
					continue
				}

				ch.inputChan.Publish(NewTradeSequence(td), -1)
			}
		}
	}()

	return nil
}

func (ch *KBarPipeline) PipelineDownStream(dst core.Upstream[KBar]) error {
	if dst == nil {
		return errors.Wrap(core.ErrPipeline, "empty downstream")
	}

	return nil
	// return dst.PipelineUpStream(ch)
}
