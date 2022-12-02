package channel

import (
	"context"
	"errors"

	"github.com/frozenpine/msgqueue/core"
	"github.com/gofrs/uuid"
)

const (
	upstreamIdtSep  = "."
	defaultChanSize = 1
)

var (
	ErrNoSubcriber       = errors.New("no subscriber")
	ErrChanClosed        = errors.New("channel closed")
	ErrPubTimeout        = errors.New("pub timeout")
	ErrPipeline          = errors.New("pipeline upstream is nil")
	ErrAlreadySubscribed = errors.New("already subscribed")

	ChannelTypeKey = "HubType"
)

type BaseChan interface {
	ID() uuid.UUID
	Name() string
	Release()
	Join()

	init(ctx context.Context, name string, bufSize int, extraInit func())
}

type Channel[T any] interface {
	BaseChan
	core.Consumer[T]
	core.Producer[T]

	PipelineDownStream(dst Channel[T]) (Channel[T], error)
	PipelineUpStream(src Channel[T]) error
}

func NewChannel[T any](ctx context.Context, name string, bufSize int) (Channel[T], error) {
	var typ core.Type = core.Memory

	if ctx == nil {
		ctx = context.Background()
	} else if v := ctx.Value(core.CtxQueueType); v != nil {
		if t, ok := v.(core.Type); ok {
			typ = t
		}
	}

	switch typ {
	case core.Memory:
		return NewMemoChannel[T](ctx, name, bufSize), nil
	}

	return nil, core.ErrInvalidType
}
