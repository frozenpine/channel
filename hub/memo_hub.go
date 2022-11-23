package hub

import (
	"context"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/frozenpine/msgqueue"
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
	return hub.id.String()
}

func (hub *MemoHub) Release() {
	hub.releaseOnce.Do(func() {
		hub.disconnectUpstream()

		hub.cancelFn()

		hub.closePubs()
	})
}

func (hub *MemoHub[T]) disconnectUpstream() {
	hub.upstreamCache.Range(func(key, value any) bool {
		upstreamIdentity, ok := key.(string)
		if !ok {
			log.Printf("Invalid upstream identity: %v", key)
			return true
		}

		src, ok := value.(Hub[T])
		if !ok {
			log.Printf("Invalid upstream source: %v, %v", upstreamIdentity, value)
			return true
		}

		idtList := strings.Split(upstreamIdentity, upstreamIdtSep)
		if len(idtList) < 3 {
			log.Printf("Invalid upstream identity: %v", upstreamIdentity)
			return true
		}

		lastIdx := len(idtList) - 1
		srcName := idtList[0]
		topic := strings.Join(idtList[1:lastIdx], upstreamIdtSep)
		subID, _ := uuid.FromString(idtList[lastIdx])

		if srcName != src.Name() {
			log.Printf("Parsed upstream name[%s] mismatch with source: %s", srcName, src.ID())
		}
		if err := src.UnSubscribe(topic, subID); err != nil {
			log.Printf("UnSubscribe from upstream[%s] failed: %v", src.ID(), err)
		}

		return true
	})
}

func (hub *MemoHub[T]) closePubs() {
	hub.pubCache.Range(func(topic, topicPub any) bool {
		pubCh := topicPub.(chan T)

		log.Printf("Closing pub channel for topic: %s", topic.(string))

		close(pubCh)

		return true
	})
}

func (hub *MemoHub[T]) Join() {
	<-hub.runCtx.Done()

	hub.subWg.Wait()
}

func (hub *MemoHub[T]) makeChan() chan *T {
	if hub.chanLen >= 0 {
		return make(chan *T, hub.chanLen)
	}

	return make(chan *T, defaultChanSize)
}

func (hub *MemoHub[T]) dispatcher(topic string, pubCh <-chan *T) {
	defer hub.subWg.Done()

	var pubFinished bool

	for {
		topicSub, exist := hub.subCache.Load(topic)

		if !exist {
			continue
		}

		subscribers := topicSub.(*sync.Map)

		select {
		case <-hub.runCtx.Done():
			if !pubFinished {
				continue
			}

			subscribers.Range(func(subcriber, subCh any) bool {
				ch := subCh.(chan T)

				log.Printf("Closing sub channel for subscriber[%+v] on topic[%s].", subcriber, topic)
				close(ch)

				return true
			})

			hub.subCache.Delete(topic)

			return
		case v, ok := <-pubCh:
			if !ok {
				pubFinished = true
				continue
			}

			subscribers.Range(func(subcriber, subCh any) bool {
				ch := subCh.(chan *T)

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

func (hub *MemoHub[T]) Subscribe(topic string, subscriber string, resumeType ResumeType) (uuid.UUID, <-chan *T) {
	topicSub, _ := hub.subCache.LoadOrStore(topic, cache.Get())
	subCache := topicSub.(*sync.Map)

	subID := msgqueue.GenID(subscriber)

	subChan, subExist := subCache.LoadOrStore(subID, hub.makeChan())

	if subExist {
		log.Printf("Channel on topic[%s] exist for subscriber[%v]", topic, subscriber)
	} else {
		log.Printf("New subscriber[%+v] on topic[%s]", subscriber, topic)
	}

	hub.loadOrCreatePub(topic)

	return subID, subChan.(chan *T)
}

func (hub *MemoHub[T]) UnSubscribe(topic string, subID uuid.UUID) error {
	topicSub, topicExist := hub.subCache.Load(topic)
	if !topicExist {
		return ErrNoTopic
	}
	subscribers := topicSub.(*sync.Map)

	subChan, subExist := subscribers.LoadAndDelete(subID)

	subCount := 0
	subscribers.Range(func(key, value any) bool {
		subCount++
		return true
	})

	if subCount <= 0 {
		hub.subCache.Delete(topic)
	}

	if !subExist {
		return ErrNoSubcriber
	}

	close(subChan.(chan T))

	return nil
}

func (hub *MemoHub[T]) loadOrCreatePub(topic string) chan<- *T {
	if _, exist := hub.subCache.Load(topic); !exist {
		return nil
	}

	topicPub, pubExist := hub.pubCache.LoadOrStore(topic, hub.makeChan())
	pubChan := topicPub.(chan *T)

	if !pubExist {
		hub.subWg.Add(1)
		go hub.dispatcher(topic, pubChan)
	}

	return pubChan
}

func (hub *MemoHub[T]) timeout(timeout time.Duration) <-chan time.Time {
	if timeout > 0 {
		return time.After(timeout)
	}

	return make(<-chan time.Time)
}

func (hub *MemoHub[T]) Publish(topic string, v *T, timeout time.Duration) error {
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

	hub.subCache.Range(func(topic, _ any) bool {
		topics = append(topics, topic.(string))
		return true
	})

	return topics
}

func (hub *MemoHub[T]) PipelineDownStream(dst Hub[T], topics ...string) (Hub[T], error) {
	if dst == nil {
		dst = NewMemoHub[T](context.Background(), "", hub.chanLen)
	}

	dst.PipelineUpStream(hub, topics...)

	return dst, nil
}

func (hub *MemoHub[T]) makeUpstreamIdentity(src Hub[T], topic string, subID uuid.UUID) string {
	return strings.Join([]string{src.Name(), topic, subID.String()}, upstreamIdtSep)
}

func (hub *MemoHub[T]) PipelineUpStream(src Hub[T], topics ...string) error {
	if src == nil {
		return ErrPipeline
	}

	for _, topic := range topics {
		pipeline := func(topic string) {
			defer hub.Release()

			subID, subChan := src.Subscribe(topic, hub.id.String(), Quick)
			upstreamIdt := hub.makeUpstreamIdentity(src, topic, subID)

			_, exist := hub.upstreamCache.LoadOrStore(upstreamIdt, src)
			if exist {
				log.Printf("Already connected to upstream[%s]@%s", src.ID(), topic)
				return
			}

			for v := range subChan {
				hub.Publish(topic, v, -1)
			}
		}

		go pipeline(topic)
	}

	return nil
}
