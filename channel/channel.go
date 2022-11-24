package channel

import (
	"context"
	"errors"
	"time"

	"github.com/frozenpine/msgqueue"
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

type ResumeType uint8

const (
	Restart ResumeType = iota
	Resume
	Quick
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
	Subscribe(name string, resumeType ResumeType) (uuid.UUID, <-chan T)
	UnSubscribe(subID uuid.UUID) error
	Publish(v T, timeout time.Duration) error
	PipelineDownStream(dst Channel[T]) (Channel[T], error)
	PipelineUpStream(src Channel[T]) error
}

func NewChannel[T any](ctx context.Context, name string, bufSize int) (Channel[T], error) {
	var typ msgqueue.Type = msgqueue.Memory

	if ctx == nil {
		ctx = context.Background()
	} else if v := ctx.Value(msgqueue.CtxQueueType); v != nil {
		if t, ok := v.(msgqueue.Type); ok {
			typ = t
		}
	}

	switch typ {
	case msgqueue.Memory:
		return NewMemoChannel[T](ctx, name, bufSize), nil
	}

	return nil, msgqueue.ErrInvalidType
}
