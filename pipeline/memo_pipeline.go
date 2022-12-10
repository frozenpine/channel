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
] struct {
	name     string
	id       uuid.UUID
	runCtx   context.Context
	cancelFn context.CancelFunc

	initOnce    sync.Once
	releaseOnce sync.Once

	inputChan  channel.Channel[Sequence[IS, IV]]
	outputChan channel.Channel[Sequence[OS, OV]]

	converter Converter[IS, IV, OS, OV]
}

func NewMemoPipeLine[
	IS, IV comparable,
	OS, OV comparable,
](ctx context.Context, name string, cvt Converter[IS, IV, OS, OV]) *MemoPipeLine[IS, IV, OS, OV] {
	pipe := MemoPipeLine[IS, IV, OS, OV]{}

	pipe.init(ctx, name, func() {
		pipe.converter = cvt
	})

	return &pipe
}

func (pipe *MemoPipeLine[IS, IV, OS, OV]) Name() string {
	return pipe.name
}

func (pipe *MemoPipeLine[IS, IV, OS, OV]) ID() uuid.UUID {
	return pipe.id
}

func (pipe *MemoPipeLine[IS, IV, OS, OV]) Join() {
	<-pipe.runCtx.Done()

	pipe.inputChan.Join()
	pipe.outputChan.Join()
}

func (pipe *MemoPipeLine[IS, IV, OS, OV]) Release() {
	pipe.releaseOnce.Do(func() {
		pipe.cancelFn()
		pipe.inputChan.Release()
		pipe.outputChan.Release()
	})
}

func (pipe *MemoPipeLine[IS, IV, OS, OV]) init(ctx context.Context, name string, extraInit func()) {
	pipe.initOnce.Do(func() {
		if ctx == nil {
			ctx = context.Background()
		}

		pipe.runCtx, pipe.cancelFn = context.WithCancel(ctx)

		if name == "" {
			name = core.GenName("")
		}

		pipe.name = name
		pipe.id = core.GenID(name)
		pipe.inputChan = channel.NewMemoChannel[Sequence[IS, IV]](ctx, name+"_input", 0)
		pipe.outputChan = channel.NewMemoChannel[Sequence[OS, OV]](ctx, name+"_output", 0)

		if extraInit != nil {
			extraInit()
		}

		go pipe.dispatcher()
	})
}

func (pipe *MemoPipeLine[IS, IV, OS, OV]) dispatcher() {
	if pipe.converter == nil {
		log.Panic("Input converter to output missing")
	}

	subID, upChan := pipe.inputChan.Subscribe(pipe.name, core.Quick)

	log.Printf("Starting dispatcher from input to output: %+v", subID)

	for {
		select {
		case <-pipe.runCtx.Done():
			pipe.Release()
		case in, ok := <-upChan:
			if !ok {
				return
			}

			if out := pipe.converter.Convert(in); out == nil {
				continue
			} else if err := pipe.outputChan.Publish(out, -1); err != nil {
				log.Printf("Dispatch to output chan failed: %+v", err)
			}
		}
	}
}

func (pipe *MemoPipeLine[IS, IV, OS, OV]) Publish(v Sequence[IS, IV], timeout time.Duration) error {
	return pipe.inputChan.Publish(v, timeout)
}

func (pipe *MemoPipeLine[IS, IV, OS, OV]) Subscribe(name string, resume core.ResumeType) (uuid.UUID, <-chan Sequence[OS, OV]) {
	return pipe.outputChan.Subscribe(name, resume)
}

func (pipe *MemoPipeLine[IS, IV, OS, OV]) UnSubscribe(subID uuid.UUID) error {
	return pipe.outputChan.UnSubscribe(subID)
}

func (pipe *MemoPipeLine[IS, IV, OS, OV]) PipelineUpStream(src core.Consumer[Sequence[IS, IV]]) error {
	if src == nil {
		return errors.Wrap(core.ErrPipeline, "empty upstream")
	}

	subID, upChan := src.Subscribe(pipe.name, core.Quick)

	go func() {
		defer src.UnSubscribe(subID)

		for {
			select {
			case <-pipe.runCtx.Done():
				return
			case v, ok := <-upChan:
				if !ok {
					log.Printf("upstream[%s] chan closed", src.Name())
					return
				}

				if err := pipe.inputChan.Publish(v, -1); err != nil {
					log.Printf("Pipeline %s[%+v] upstream failed: +%v", pipe.name, pipe.id, err)
				}
			}
		}
	}()

	return nil
}

func (pipe *MemoPipeLine[IS, IV, OS, OV]) PipelineDownStream(dst core.Upstream[Sequence[OS, OV]]) error {
	if dst == nil {
		return errors.Wrap(core.ErrPipeline, "empty downstream")
	}

	return dst.PipelineUpStream(pipe)
}
