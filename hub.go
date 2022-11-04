package channel

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"
)

type Hub[T any] struct {
	init     sync.Once
	release  sync.Once
	runCtx   context.Context
	cancelFn context.CancelFunc

	timeout time.Duration

	subCache sync.Map
	subLen   int

	pubCache sync.Map
	pubLen   int
}

func NewHub[T any](ctx context.Context, bufSize int, timeout time.Duration) *Hub[T] {
	if ctx == nil {
		ctx = context.Background()
	}

	hub := Hub[T]{
		timeout: timeout,
		subLen:  bufSize,
		pubLen:  2 * bufSize,
	}

	hub.init.Do(func() {
		hub.runCtx, hub.cancelFn = context.WithCancel(ctx)
	})

	return &hub
}

func (hub *Hub[T]) Stop() {
	hub.release.Do(func() { hub.cancelFn() })
}

func (hub *Hub[T]) Join() {
	<-hub.runCtx.Done()
}

func (hub *Hub[T]) dispatcher(topic string, pubCh <-chan T) {
	for {
		topicSub, exist := hub.subCache.Load(topic)

		if !exist {
			continue
		}

		subCache := topicSub.(*sync.Map)

		select {
		case <-hub.runCtx.Done():
			subCache.Range(func(subcriber, subCh any) bool {
				ch := subCh.(chan T)

				close(ch)

				return true
			})

			return
		case v := <-pubCh:
			subCache.Range(func(subcriber, subCh any) bool {
				ch := subCh.(chan T)

				select {
				case <-time.After(time.Second):
					log.Printf("Publish timeout to subscriber[%v] on topic[%s].", subcriber, topic)
				case ch <- v:
				}

				return true
			})
		}
	}
}

func (hub *Hub[T]) Subscribe(topic string, subscriber interface{}) <-chan T {
	topicSub, _ := hub.subCache.LoadOrStore(topic, &sync.Map{})
	subCache := topicSub.(*sync.Map)
	ch, subExist := subCache.LoadOrStore(subscriber, make(chan T))
	if subExist {
		log.Printf("Channel on topic[%s] exist for subscriber[%v]", topic, subscriber)
	}

	hub.loadOrCreatePub(topic)

	return ch.(chan T)
}

func (hub *Hub[T]) loadOrCreatePub(topic string) chan<- T {
	topicPub, pubExist := hub.pubCache.LoadOrStore(topic, make(chan T, hub.pubLen))
	pubCh := topicPub.(chan T)

	if !pubExist {
		go hub.dispatcher(topic, pubCh)
	}

	return pubCh
}

func (hub *Hub[T]) PublishWithTimeout(topic string, v T, timeout time.Duration) error {
	pubCh := hub.loadOrCreatePub(topic)

	if timeout <= 0 {
		timeout = hub.timeout
	}

	select {
	case <-hub.runCtx.Done():
		return errors.New("hub closed")
	case <-time.After(timeout):
		return errors.New("pub timeout")
	case pubCh <- v:
		return nil
	}
}
