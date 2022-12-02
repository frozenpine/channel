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

type Consumer[T any] interface {
	Subscribe(name string, resumeType ResumeType) (uuid.UUID, <-chan T)
	UnSubscribe(subID uuid.UUID) error
}

type Producer[T any] interface {
	Publish(v T, timeout time.Duration) error
}
