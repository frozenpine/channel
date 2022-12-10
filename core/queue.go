package core

import (
	"time"

	"github.com/gofrs/uuid"
)

type ResumeType uint8

const (
	Restart ResumeType = iota
	Resume
	Quick
)

type QueueBase interface {
	ID() uuid.UUID
	Name() string
	Release()
	Join()
}

type Consumer[T any] interface {
	QueueBase
	Subscribe(name string, resumeType ResumeType) (uuid.UUID, <-chan T)
	UnSubscribe(subID uuid.UUID) error
}

type Producer[T any] interface {
	QueueBase
	Publish(v T, timeout time.Duration) error
}

type Upstream[T any] interface {
	QueueBase
	PipelineUpStream(src Consumer[T]) error
}

type Downstream[T any] interface {
	QueueBase
	PipelineDownStream(dst Upstream[T]) error
}
