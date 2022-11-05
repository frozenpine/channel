package channel

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"
)

var (
	ErrNoSubcriber = errors.New("no subscriber")
	ErrHubClosed   = errors.New("hub closed")
	ErrPubTimeout  = errors.New("pub timeout")

	defaultChanLen = 10
)

type Hub[T any] struct {
	init    sync.Once
	release sync.Once

	runCtx   context.Context
	cancelFn context.CancelFunc

	chanLen int

	subCache sync.Map
	subWg    sync.WaitGroup

	pubCache sync.Map
}

// NewHub create channel hub.
//
// bufSize: pub & sub chan buffer size, if bufSize < 0, use default size 10
func NewHub[T any](ctx context.Context, bufSize int) *Hub[T] {
	if ctx == nil {
		ctx = context.Background()
	}

	if bufSize < 0 {
		bufSize = defaultChanLen
	}

	hub := Hub[T]{
		chanLen: bufSize,
	}

	hub.init.Do(func() {
		hub.runCtx, hub.cancelFn = context.WithCancel(ctx)
	})

	return &hub
}

func (hub *Hub[T]) Stop() {
	hub.release.Do(func() {
		hub.cancelFn()

		hub.pubCache.Range(func(topic, topicPub any) bool {
			pubCh := topicPub.(chan T)

			log.Printf("Closing pub channel for topic: %s", topic.(string))

			close(pubCh)

			return true
		})
	})
}

func (hub *Hub[T]) Join() {
	<-hub.runCtx.Done()

	hub.subWg.Wait()
}

func (hub *Hub[T]) dispatcher(topic string, pubCh <-chan T) {
	hub.subWg.Add(1)

	defer hub.subWg.Done()

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

				log.Printf("Closing sub channel for subscriber[%+v] on topic[%s].", subcriber, topic)
				close(ch)

				return true
			})

			return
		case v := <-pubCh:
			subCache.Range(func(subcriber, subCh any) bool {
				ch := subCh.(chan T)

				select {
				case <-time.After(time.Second):
					log.Printf("Publish timeout to subscriber[%+v] on topic[%s].", subcriber, topic)
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

	ch, subExist := subCache.LoadOrStore(subscriber, make(chan T, 1))

	if subExist {
		log.Printf("Channel on topic[%s] exist for subscriber[%v]", topic, subscriber)
	} else {
		log.Printf("New subscriber[%+v] on topic[%s]", subscriber, topic)
	}

	hub.loadOrCreatePub(topic)

	return ch.(chan T)
}

func (hub *Hub[T]) loadOrCreatePub(topic string) chan<- T {
	if _, exist := hub.subCache.Load(topic); !exist {
		return nil
	}

	topicPub, pubExist := hub.pubCache.LoadOrStore(topic, make(chan T, 1))
	pubCh := topicPub.(chan T)

	if !pubExist {
		go hub.dispatcher(topic, pubCh)
	}

	return pubCh
}

func (hub *Hub[T]) timeout(timeout time.Duration) <-chan time.Time {
	if timeout > 0 {
		return time.After(timeout)
	}

	return make(<-chan time.Time)
}

func (hub *Hub[T]) Publish(topic string, v T, timeout time.Duration) error {
	pubCh := hub.loadOrCreatePub(topic)
	if pubCh == nil {
		return ErrNoSubcriber
	}

	select {
	case <-hub.runCtx.Done():
		return ErrHubClosed
	case <-hub.timeout(timeout):
		return ErrPubTimeout
	case pubCh <- v:
		return nil
	}
}
