package hub

import (
	"context"
	"log"
	"sync"

	"github.com/frozenpine/msgqueue/core"
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
	hub := MemoHub{}

	hub.Init(ctx, name, func() {
		hub.chanLen = bufSize
	})

	return &hub
}

func (hub *MemoHub) Init(ctx context.Context, name string, extraInit func()) {
	hub.initOnce.Do(func() {
		if ctx == nil {
			ctx = context.Background()
		}

		if name == "" {
			name = "MemoHub"
		}

		hub.runCtx, hub.cancelFn = context.WithCancel(ctx)

		hub.name = core.GenName(name)
		hub.id = core.GenID(hub.name)

		if extraInit != nil {
			extraInit()
		}
	})
}

func (hub *MemoHub) ID() uuid.UUID {
	return hub.id
}

func (hub *MemoHub) Name() string {
	return hub.name
}

func (hub *MemoHub) Type() core.Type {
	return core.Memory
}

func (hub *MemoHub) Release() {
	hub.releaseOnce.Do(func() {
		hub.cancelFn()

		hub.topicChanCache.Range(func(key, value any) bool {
			topic := key.(string)
			ch := value.(core.QueueBase)

			log.Printf("Closing channel for topic: %s", topic)
			ch.Release()

			return true
		})
	})
}

func (hub *MemoHub) Join() {
	<-hub.runCtx.Done()

	hub.topicChanCache.Range(func(key, value any) bool {
		ch := value.(core.QueueBase)

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

func (hub *MemoHub) createTopicChannel(topic string, fn ChannelCreateWrapper) (core.QueueBase, error) {
	if ch, exist := hub.topicChanCache.Load(topic); exist {
		return ch.(core.QueueBase), ErrTopicExist
	}

	if ch, err := fn(
		context.WithValue(hub.runCtx, core.CtxQueueType, core.Memory),
		hub.name+"."+topic,
		hub.chanLen,
	); err != nil {
		return nil, err
	} else {
		result, _ := hub.topicChanCache.LoadOrStore(topic, ch)
		return result.(core.QueueBase), nil
	}
}

func (hub *MemoHub) getTopicChannel(topic string) (core.QueueBase, error) {
	if ch, exist := hub.topicChanCache.Load(topic); exist {
		return ch.(core.QueueBase), nil
	}

	return nil, ErrNoTopic
}
