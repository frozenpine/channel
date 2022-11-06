package hub

import (
	"context"
	"log"
	"sync"
	"time"
)

var dummyData = struct{}{}

type MemoHub[T any] struct {
	init    sync.Once
	release sync.Once

	runCtx   context.Context
	cancelFn context.CancelFunc

	chanLen int

	topicList sync.Map

	subCache sync.Map
	subWg    sync.WaitGroup

	pubCache sync.Map
}

func NewMemoHub[T any](ctx context.Context, bufSize int) *MemoHub[T] {
	hub := MemoHub[T]{
		chanLen: bufSize,
	}

	hub.init.Do(func() {
		hub.runCtx, hub.cancelFn = context.WithCancel(ctx)
	})

	return &hub
}

func (hub *MemoHub[T]) Stop() {
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

func (hub *MemoHub[T]) Join() {
	<-hub.runCtx.Done()

	hub.subWg.Wait()
}

func (hub *MemoHub[T]) makeChan() chan T {
	if hub.chanLen >= 0 {
		return make(chan T, hub.chanLen)
	}

	return make(chan T, defaultChanSize)
}

func (hub *MemoHub[T]) dispatcher(topic string, pubCh <-chan T) {
	defer hub.subWg.Done()

	var pubFinished bool

	for {
		topicSub, exist := hub.subCache.Load(topic)

		if !exist {
			continue
		}

		subCache := topicSub.(*sync.Map)

		select {
		case <-hub.runCtx.Done():
			if !pubFinished {
				continue
			}

			subCache.Range(func(subcriber, subCh any) bool {
				ch := subCh.(chan T)

				log.Printf("Closing sub channel for subscriber[%+v] on topic[%s].", subcriber, topic)
				close(ch)

				return true
			})

			hub.topicList.Delete(topic)

			return
		case v, ok := <-pubCh:
			if !ok {
				pubFinished = true
				continue
			}

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

func (hub *MemoHub[T]) Subscribe(topic string, subscriber interface{}) <-chan T {
	topicSub, _ := hub.subCache.LoadOrStore(topic, &sync.Map{})
	subCache := topicSub.(*sync.Map)

	ch, subExist := subCache.LoadOrStore(subscriber, hub.makeChan())

	if subExist {
		log.Printf("Channel on topic[%s] exist for subscriber[%v]", topic, subscriber)
	} else {
		log.Printf("New subscriber[%+v] on topic[%s]", subscriber, topic)
	}

	hub.loadOrCreatePub(topic)

	return ch.(chan T)
}

func (hub *MemoHub[T]) UnSubscribe(topic string, subscriber interface{}) error {
	topicSub, topicExist := hub.subCache.Load(topic)
	if !topicExist {
		return ErrNoTopic
	}
	subCache := topicSub.(*sync.Map)

	ch, subExist := subCache.LoadAndDelete(subscriber)

	subCount := 0
	subCache.Range(func(key, value any) bool {
		subCount++
		return true
	})

	if subCount <= 0 {
		hub.topicList.Delete(topic)
	}

	if !subExist {
		return ErrNoSubcriber
	}

	close(ch.(chan T))

	return nil
}

func (hub *MemoHub[T]) loadOrCreatePub(topic string) chan<- T {
	if _, exist := hub.subCache.Load(topic); !exist {
		hub.topicList.Delete(topic)
		return nil
	}

	topicPub, pubExist := hub.pubCache.LoadOrStore(topic, hub.makeChan())
	pubCh := topicPub.(chan T)

	if !pubExist {
		hub.subWg.Add(1)
		hub.topicList.LoadOrStore(topic, dummyData)
		go hub.dispatcher(topic, pubCh)
	}

	return pubCh
}

func (hub *MemoHub[T]) timeout(timeout time.Duration) <-chan time.Time {
	if timeout > 0 {
		return time.After(timeout)
	}

	return make(<-chan time.Time)
}

func (hub *MemoHub[T]) Publish(topic string, v T, timeout time.Duration) error {
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

func (hub *MemoHub[T]) Topics() []string {
	topics := []string{}

	hub.topicList.Range(func(topic, _s any) bool {
		topics = append(topics, topic.(string))
		return true
	})

	return topics
}

func (hub *MemoHub[T]) PipelineDownStream(dst Hub[T], topics ...string) (Hub[T], error) {
	if dst == nil {
		dst = NewMemoHub[T](context.Background(), hub.chanLen)
	}

	for _, topic := range topics {
		pipeline := func(topic string) {
			defer dst.Stop()

			for v := range hub.Subscribe(topic, hub) {
				dst.Publish(topic, v, -1)
			}
		}

		go pipeline(topic)
	}

	return dst, nil
}

func (hub *MemoHub[T]) PipelineUpStream(src Hub[T], topics ...string) error {
	if src == nil {
		return ErrPipeline
	}

	for _, topic := range topics {
		pipeline := func() {
			defer hub.Stop()

			for v := range src.Subscribe(topic, hub) {
				hub.Publish(topic, v, -1)
			}
		}

		go pipeline()
	}

	return nil
}
