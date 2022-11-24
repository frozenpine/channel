package hub

import (
	"context"
	"log"
	"sync"

	"github.com/frozenpine/msgqueue"
	"github.com/frozenpine/msgqueue/channel"
	"github.com/gofrs/uuid"
)

type MemoHub struct {
	id          uuid.UUID
	name        string
	initOnce    sync.Once
	releaseOnce sync.Once

	runCtx   context.Context
	cancelFn context.CancelFunc

	chanLen int

	topicChanCache sync.Map
}

func NewMemoHub(ctx context.Context, name string, bufSize int) *MemoHub {
	if ctx == nil {
		ctx = context.Background()
	}

	if name == "" {
		name = msgqueue.GenName()
	}

	hub := MemoHub{}

	hub.initOnce.Do(func() {
		hub.runCtx, hub.cancelFn = context.WithCancel(ctx)

		hub.chanLen = bufSize
		hub.id = msgqueue.GenID(name)
		hub.name = name
	})

	return &hub
}

func (hub *MemoHub) ID() uuid.UUID {
	return hub.id
}

func (hub *MemoHub) Name() string {
	return hub.name
}

func (hub *MemoHub) Type() msgqueue.Type {
	return msgqueue.Memory
}

func (hub *MemoHub) Release() {
	hub.releaseOnce.Do(func() {
		hub.cancelFn()

		hub.topicChanCache.Range(func(key, value any) bool {
			topic := key.(string)
			ch := value.(channel.BaseChan)

			log.Printf("Closing channel for topic: %s", topic)
			ch.Release()

			return true
		})
	})
}

func (hub *MemoHub) Join() {
	<-hub.runCtx.Done()

	hub.topicChanCache.Range(func(key, value any) bool {
		ch := value.(channel.BaseChan)

		ch.Join()

		hub.topicChanCache.Delete(key)

		return true
	})
}

func (hub *MemoHub) Topics() []string {
	topics := []string{}

	hub.topicChanCache.Range(func(key, value any) bool {
		topics = append(topics, key.(string))
		return true
	})

	return topics
}

func (hub *MemoHub) createTopicChannel(topic string, fn ChannelCreateWrapper) (channel.BaseChan, error) {
	if ch, exist := hub.topicChanCache.Load(topic); exist {
		return ch.(channel.BaseChan), ErrTopicExist
	}

	if ch, err := fn(
		context.WithValue(hub.runCtx, msgqueue.CtxQueueType, msgqueue.Memory),
		hub.name+"."+topic,
		hub.chanLen,
	); err != nil {
		return nil, err
	} else {
		result, _ := hub.topicChanCache.LoadOrStore(topic, ch)
		return result.(channel.BaseChan), nil
	}
}

func (hub *MemoHub) getTopicChannel(topic string) (channel.BaseChan, error) {
	if ch, exist := hub.topicChanCache.Load(topic); exist {
		return ch.(channel.BaseChan), nil
	}

	return nil, ErrNoTopic
}
