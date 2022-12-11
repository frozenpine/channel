package channel

import (
	"context"

	"github.com/frozenpine/msgqueue/core"
	"github.com/pkg/errors"
)

const (
	defaultChanSize = 1
)

var (
	ErrChanClosed = errors.New("channel closed")

	ChannelTypeKey = "HubType"
)

type Channel[T any] interface {
	core.QueueBase
	core.Consumer[T]
	core.Producer[T]
	core.Upstream[T]
	core.Downstream[T]
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
