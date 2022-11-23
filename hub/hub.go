package hub

import (
	"errors"

	"github.com/frozenpine/msgqueue/channel"
	"github.com/gofrs/uuid"
)

const defaultChanSize = 1

var (
	ErrNoSubcriber       = errors.New("no subscriber")
	ErrNoTopic           = errors.New("no topic")
	ErrHubClosed         = errors.New("hub closed")
	ErrPubTimeout        = errors.New("pub timeout")
	ErrPipeline          = errors.New("pipeline upstream is nil")
	ErrAlreadySubscribed = errors.New("topic already subscribed")

	HubTypeKey = "HubType"
)

type HubType uint

const (
	MemoHubType HubType = iota
	RemoteHubType
)

type Hub interface {
	ID() uuid.UUID
	Name() string
	Release()
	Join()

	Topics() []string
	GetTopicChannel(topic string) interface{}
}

func GetHubTopicChannel[T any](hub Hub, topic string) channel.Channel[T] {
	return hub.GetTopicChannel(topic).(channel.Channel[T])
}
