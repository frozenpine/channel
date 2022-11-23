package channel

import (
	"errors"
	"time"

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
)

type ResumeType uint8

const (
	Restart ResumeType = iota
	Resume
	Quick
)

type Channel[T any] interface {
	ID() uuid.UUID
	Name() string
	Release()
	Join()
	Subscribe(name string, resumeType ResumeType) (uuid.UUID, <-chan *T)
	UnSubscribe(subID uuid.UUID) error
	Publish(v *T, timeout time.Duration) error
	PipelineDownStream(dst Channel[T]) (Channel[T], error)
	PipelineUpStream(src Channel[T]) error
}
