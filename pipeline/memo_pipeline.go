package pipeline

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/frozenpine/msgqueue/channel"
	"github.com/frozenpine/msgqueue/core"
	"github.com/gofrs/uuid"
	"github.com/pkg/errors"
)

type MemoPipeLine[
	IV, OV any,
] struct {
	name     string
	id       uuid.UUID
	runCtx   context.Context
	cancelFn context.CancelFunc

	initOnce    sync.Once
	releaseOnce sync.Once

	inputChan  channel.Channel[IV]
	outputChan channel.Channel[OV]

	converter Converter[IV, OV]
}

func NewMemoPipeLine[
	IV, OV any,
](ctx context.Context, name string, cvt Converter[IV, OV]) *MemoPipeLine[IV, OV] {
	pipe := MemoPipeLine[IV, OV]{}

	pipe.Init(ctx, name, func() {
		pipe.converter = cvt
	})

	return &pipe
}

func (pipe *MemoPipeLine[IV, OV]) Name() string {
	return pipe.name
}

func (pipe *MemoPipeLine[IV, OV]) ID() uuid.UUID {
	return pipe.id
}

func (pipe *MemoPipeLine[IV, OV]) Join() {
	<-pipe.runCtx.Done()

	pipe.inputChan.Join()
	pipe.outputChan.Join()
}

func (pipe *MemoPipeLine[IV, OV]) Release() {
	pipe.releaseOnce.Do(func() {
		pipe.cancelFn()

		pipe.inputChan.Release()
	})
}

func (pipe *MemoPipeLine[IV, OV]) Init(
	ctx context.Context, name string,
	extraInit func(),
) {
	pipe.initOnce.Do(func() {
		if ctx == nil {
			ctx = context.Background()
		}

		pipe.runCtx, pipe.cancelFn = context.WithCancel(ctx)

		if name == "" {
			name = "MemoPipeline"
		}

		pipe.name = core.GenName(name)
		pipe.id = core.GenID(pipe.name)

		// use seperate context to prevent exit same time
		pipe.inputChan = channel.NewMemoChannel[IV](
			context.Background(), name+"_input", 0)
		pipe.outputChan = channel.NewMemoChannel[OV](
			context.Background(), name+"_output", 0)

		if extraInit != nil {
			extraInit()
		}

		go pipe.dispatcher()
	})
}

func (pipe *MemoPipeLine[IV, OV]) dispatcher() {
	if pipe.converter == nil {
		panic("input converter to output missing")
	}

	subID, upChan := pipe.inputChan.Subscribe(pipe.name, core.Quick)

	slog.Info(
		"starting dispatcher from input to output",
		slog.String("sub_id", subID.String()),
	)

	for {
		select {
		case <-pipe.runCtx.Done():
			pipe.Release()
		case in, ok := <-upChan:
			if !ok {
				pipe.outputChan.Release()
				return
			}

			if err := pipe.converter(in, pipe.outputChan); err != nil {
				slog.Error(
					"dispatch to output chan failed",
					slog.Any("error", err),
				)
			}
		}
	}
}

func (pipe *MemoPipeLine[IV, OV]) Publish(v IV, timeout time.Duration) error {
	return pipe.inputChan.Publish(v, timeout)
}

func (pipe *MemoPipeLine[IV, OV]) Subscribe(name string, resume core.ResumeType) (uuid.UUID, <-chan OV) {
	return pipe.outputChan.Subscribe(name, resume)
}

func (pipe *MemoPipeLine[IV, OV]) UnSubscribe(subID uuid.UUID) error {
	return pipe.outputChan.UnSubscribe(subID)
}

func (pipe *MemoPipeLine[IV, OV]) PipelineUpStream(src core.Consumer[IV]) error {
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
					slog.Warn(
						"upstream chan closed",
						slog.String("name", src.Name()),
						slog.String("id", src.ID().String()),
					)
					return
				}

				if err := pipe.inputChan.Publish(v, -1); err != nil {
					slog.Error(
						"pipeline upstream failed",
						slog.Any("error", err),
						slog.String("name", pipe.name),
						slog.String("id", pipe.id.String()),
					)
				}
			}
		}
	}()

	return nil
}

func (pipe *MemoPipeLine[IV, OV]) PipelineDownStream(dst core.Upstream[OV]) error {
	if dst == nil {
		return errors.Wrap(core.ErrPipeline, "empty downstream")
	}

	return dst.PipelineUpStream(pipe)
}
