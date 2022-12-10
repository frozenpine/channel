package pipeline

import (
	"context"
	"log"
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

	inputChan  channel.Channel[Sequence[IS, IV]]
	outputChan channel.Channel[Sequence[OS, OV]]
}

func NewMemoPipeLine[
	IS, IV comparable,
	OS, OV comparable,
](ctx context.Context, name string) *MemoPipeLine[IS, IV, OS, OV] {
	pipe := MemoPipeLine[IS, IV, OS, OV]{}

	return &pipe
}

func (pipe *MemoPipeLine[IS, IV, OS, OV]) Name() string {
	return pipe.name
}

func (pipe *MemoPipeLine[IS, IV, OS, OV]) ID() uuid.UUID {
	return pipe.id
}

func (pipe *MemoPipeLine[IS, IV, OS, OV]) Join() {
	pipe.inputChan.Join()
	pipe.outputChan.Join()
}

func (pipe *MemoPipeLine[IS, IV, OS, OV]) Release() {
	pipe.inputChan.Release()
	pipe.outputChan.Release()
}

func (pipe *MemoPipeLine[IS, IV, OS, OV]) init(ctx context.Context, name string, extraInit func()) {
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
			case v := <-upChan:
				if v == nil {
					log.Printf("upstream[%s] chan closed", src.Name())
					return
				}

				pipe.inputChan.Publish(v, -1)
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
