package hub

import (
	"errors"
	"time"
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

type HubType uint

const (
	MemoHubType HubType = iota
)

type Hub[T any] interface {
	Stop()
	Join()

	Topics() []string

	Subscribe(topic string, subscriber interface{}) <-chan T
	UnSubscribe(topic string, subscriber interface{}) error
	Publish(topic string, v T, timeout time.Duration) error

	PipelineDownStream(dst Hub[T], topics ...string) (Hub[T], error)
	PipelineUpStream(src Hub[T], topics ...string) error
}
