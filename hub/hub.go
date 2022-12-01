package hub

import (
	"context"
	originErr "errors"

	"github.com/frozenpine/msgqueue/channel"
	"github.com/frozenpine/msgqueue/core"
	"github.com/gofrs/uuid"
	"github.com/pkg/errors"
)

var (
	ErrInvalidHub     = originErr.New("invalid hub")
	ErrNoSubcriber    = originErr.New("no subscriber")
	ErrNoTopic        = originErr.New("no topic")
	ErrTopicExist     = originErr.New("topic already exist")
	ErrHubClosed      = originErr.New("hub closed")
	ErrInvalidChannel = originErr.New("invalid channel")
)

type ChannelCreateWrapper func(context.Context, string, int) (channel.BaseChan, error)

type Hub interface {
	ID() uuid.UUID
	Name() string
	Type() core.Type
	Release()
	Join()
	Topics() []string

	createTopicChannel(topic string, fn ChannelCreateWrapper) (channel.BaseChan, error)
	getTopicChannel(topic string) (channel.BaseChan, error)
}

func GetHubTopicChannel[T any](hub Hub, topic string) (channel.Channel[T], error) {
	if hub == nil {
		return nil, ErrInvalidHub
	}

	if topicChan, err := hub.getTopicChannel(topic); err == nil {
		if ch, ok := topicChan.(channel.Channel[T]); ok {
			return ch, nil
		}
		return nil, errors.Wrap(ErrInvalidChannel, "channel type mismatch")
	} else {
		return nil, err
	}
}

func GetOrCreateTopicChannel[T any](hub Hub, topic string) (channel.Channel[T], error) {
	if hub == nil {
		return nil, ErrInvalidHub
	}

	if ch, err := hub.createTopicChannel(
		topic,
		func(ctx context.Context, name string, bufSize int) (channel.BaseChan, error) {
			return channel.NewChannel[T](ctx, name, bufSize)
		},
	); err == nil {
		return ch.(channel.Channel[T]), nil
	} else if err == ErrTopicExist {
		if result, ok := ch.(channel.Channel[T]); ok {
			return result, nil
		} else {
			return nil, errors.Wrap(err, "channel type mismatch")
		}
	} else {
		return nil, err
	}
}
