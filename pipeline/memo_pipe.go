package pipeline

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/frozenpine/msgqueue/channel"
	"github.com/frozenpine/msgqueue/core"
	"github.com/gofrs/uuid"
	"github.com/pkg/errors"
)

type MemoPipeLine[
	IS, IV comparable,
	OS, OV comparable,
	KEY comparable,
] struct {
	name     string
	id       uuid.UUID
	runCtx   context.Context
	cancelFn context.CancelFunc

	inputChan      channel.Channel[Sequence[IS, IV]]
	inputConverter func(IV) Sequence[IS, IV]
	inputWaterMark func() Sequence[IS, IV]

	filterFn func(Sequence[IS, IV]) bool
	groupFn  func(Sequence[IS, IV]) KEY
	groupMap sync.Map

	outputChan channel.Channel[OV]

	currBuffer SequenceSlice[IS, IV]
}

func (pipe *MemoPipeLine[IS, IV, OS, OV, KEY]) Name() string {
	return pipe.name
}

func (pipe *MemoPipeLine[IS, IV, OS, OV, KEY]) ID() uuid.UUID {
	return pipe.id
}

func (pipe *MemoPipeLine[IS, IV, OS, OV, KEY]) Join() {}

func (pipe *MemoPipeLine[IS, IV, OS, OV, KEY]) Release() {}

func (pipe *MemoPipeLine[IS, IV, OS, OV, KEY]) init(ctx context.Context, name string, extraInit func()) {
	if ctx == nil {
		ctx = context.Background()
	}

	pipe.runCtx, pipe.cancelFn = context.WithCancel(ctx)

	if name == "" {
		name = core.GenName("")
	}

	pipe.name = name
	pipe.id = core.GenID(name)

	if extraInit != nil {
		extraInit()
	}
}

func (pipe *MemoPipeLine[IS, IV, OS, OV, KEY]) Publish(v IV, timeout time.Duration) error {
	return pipe.inputChan.Publish(pipe.inputConverter(v), timeout)
}

func (pipe *MemoPipeLine[IS, IV, OS, OV, KEY]) Subscribe(name string, resume core.ResumeType) (uuid.UUID, <-chan OV) {
	return pipe.outputChan.Subscribe(name, resume)
}

func (pipe *MemoPipeLine[IS, IV, OS, OV, KEY]) UnSubscribe(subID uuid.UUID) error {
	return pipe.outputChan.UnSubscribe(subID)
}

func (pipe *MemoPipeLine[IS, IV, OS, OV, KEY]) PipelineUpStream(src core.Consumer[IV]) error {
	if src == nil {
		return errors.Wrap(core.ErrPipeline, "empty upstream")
	}

	subID, upChan := src.Subscribe(pipe.name, core.Quick)

	go func() {
		defer src.UnSubscribe(subID)

		var empty IV

		for {
			select {
			case <-pipe.runCtx.Done():
				return
			case v := <-upChan:
				if v == empty {
					log.Printf("upstream[%s] chan closed", src.Name())
					return
				}

				pipe.inputChan.Publish(pipe.inputConverter(v), -1)
			}
		}
	}()

	return nil
}

func (pipe *MemoPipeLine[IS, IV, OS, OV, KEY]) PipelineDownStream(dst core.Upstream[OV]) error {
	if dst == nil {
		return errors.Wrap(core.ErrPipeline, "empty downstream")
	}

	return dst.PipelineUpStream(pipe)
}

func (pipe *MemoPipeLine[IS, IV, OS, OV, KEY]) WindowBy(markIn <-chan WaterMark) Aggregatorable[IS, IV, OS, OV, KEY] {
	if markIn == nil {
		return nil
	}

	go func() {
		for wm := range markIn {
			if !wm.IsWaterMark() {
				continue
			}

			pipe.inputChan.Publish(pipe.inputWaterMark(), -1)
		}
	}()

	return pipe
}

func (pipe *MemoPipeLine[IS, IV, OS, OV, KEY]) FilterBy(fn func(Sequence[IS, IV]) bool) Aggregatorable[IS, IV, OS, OV, KEY] {
	pipe.filterFn = fn
	return pipe
}

func (pipe *MemoPipeLine[IS, IV, OS, OV, KEY]) GroupBy(fn func(Sequence[IS, IV]) KEY) Aggregatorable[IS, IV, OS, OV, KEY] {
	pipe.groupFn = fn

	return pipe
}

func (pipe *MemoPipeLine[IS, IV, OS, OV, KEY]) GetGroupAggregator(key KEY) Aggregatorable[IS, IV, OS, OV, KEY] {
	if g, exist := pipe.groupMap.Load(key); !exist {
		return nil
	} else {
		return g.(Aggregatorable[IS, IV, OS, OV, KEY])
	}
}

func (pipe *MemoPipeLine[IS, IV, OS, OV, KEY]) Action(act func(SequenceSlice[IS, IV]) Sequence[OS, OV]) {
	subID, upChan := pipe.inputChan.Subscribe(pipe.name, core.Quick)

	log.Printf("Start aggregation for input data")

	go func() {
		defer pipe.inputChan.UnSubscribe(subID)

		for {
			select {
			case <-pipe.runCtx.Done():
				log.Printf("Pipeline exist")
				return
			case v := <-upChan:
				if v == nil {
					return
				}

				if pipe.filterFn != nil && !pipe.filterFn(v) {
					continue
				}

				if pipe.groupFn != nil {
					// key := pipe.groupFn(v)

					// TODO: new aggregator
					// g, _ := pipe.groupMap.LoadOrStore(key, nil)

					// agg := g.(Aggregatorable[IS, IV, OS, OV, KEY])

					// agg.Publish(v.Value(), -1)
				}

				if v.IsWaterMark() {
					out := act(pipe.currBuffer)
					pipe.outputChan.Publish(out.Value(), -1)
				} else {
					switch errors.Unwrap(pipe.currBuffer.Push(v.Value())) {
					case ErrFutureTick:
					case ErrHistoryTick:
					}
				}
			}
		}
	}()
}
