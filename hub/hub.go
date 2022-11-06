package hub

import (
	"errors"
	"time"
)

const defaultChanSize = 1

var (
	ErrNoSubcriber = errors.New("no subscriber")
	ErrHubClosed   = errors.New("hub closed")
	ErrPubTimeout  = errors.New("pub timeout")

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

	Subscribe(topic string, subscribe interface{}) <-chan T
	Publish(topic string, v T, timeout time.Duration) error

	Pipeline(dst Hub[T], topics ...string) (Hub[T], error)
}
