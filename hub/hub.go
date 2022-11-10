package hub

import (
	"errors"
	"time"

	"github.com/gofrs/uuid"
)

const defaultChanSize = 1

var (
	ErrNoSubcriber = errors.New("no subscriber")
	ErrNoTopic     = errors.New("no topic")
	ErrHubClosed   = errors.New("hub closed")
	ErrPubTimeout  = errors.New("pub timeout")
	ErrPipeline    = errors.New("pipeline upstream is nil")

	HubTypeKey = "HubType"
)

func GenID(name string) uuid.UUID {
	if name == "" {
		uuid4, _ := uuid.NewV4()
		name = uuid4.String()
	}
	return uuid.NewV5(uuid.NamespaceDNS, name)
}

type HubType uint

const (
	MemoHubType HubType = iota
)

type ResumeType uint8

const (
	Restart ResumeType = iota
	Resume
	Quick
)

type Hub[T any] interface {
	ID() uuid.UUID
	Name() string
	Stop()
	Join()

	Topics() []string

	Subscribe(topic string, subscriber string, resumeType ResumeType) (uuid.UUID, <-chan T)
	UnSubscribe(topic string, subID uuid.UUID) error
	Publish(topic string, v T, timeout time.Duration) error

	PipelineDownStream(dst Hub[T], topics ...string) (Hub[T], error)
	PipelineUpStream(src Hub[T], topics ...string) error
}
