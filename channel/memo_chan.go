package channel

import (
	"context"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/frozenpine/msgqueue"
	"github.com/gofrs/uuid"
)

type sub[T any] struct {
	once sync.Once
	data chan *T
}

func (sub *sub[T]) close() {
	sub.once.Do(func() { close(sub.data) })
}

func (sub *sub[T]) ch() <-chan *T {
	return sub.data
}

type MemoChannel[T any] struct {
	name        string
	id          uuid.UUID
	initOnce    sync.Once
	releaseOnce sync.Once

	runCtx   context.Context
	cancelFn context.CancelFunc

	chanLen int

	input chan *T

	subscriberCache sync.Map
	subscriberWg    sync.WaitGroup
	upstreamCache   sync.Map
	upstreamWg      sync.WaitGroup
}

func NewMemoChannel[T any](ctx context.Context, name string, bufSize int) *MemoChannel[T] {
	if ctx == nil {
		ctx = context.Background()
	}

	if name == "" {
		name = msgqueue.GenName()
	}

	if bufSize <= 0 {
		bufSize = defaultChanSize
	}

	channel := MemoChannel[T]{}

	channel.initOnce.Do(func() {
		channel.runCtx, channel.cancelFn = context.WithCancel(ctx)
		channel.name = name
		channel.id = msgqueue.GenID(name)
		channel.chanLen = bufSize
		channel.input = channel.makeChan()

		go channel.inputDispatcher()
	})

	return &channel
}

func (ch *MemoChannel[T]) ID() uuid.UUID {
	return ch.id
}

func (ch *MemoChannel[T]) Name() string {
	return ch.name
}

func (ch *MemoChannel[T]) Release() {
	ch.releaseOnce.Do(func() {
		ch.cancelFn()

		ch.disconnectUpstream()

		ch.upstreamWg.Wait()

		close(ch.input)
	})
}

func (ch *MemoChannel[T]) disconnectUpstream() {
	ch.upstreamCache.Range(func(key, value any) bool {
		upstreamIdentity, ok := key.(string)
		if !ok {
			log.Printf("Invalid upstream identity: %v", key)
			return true
		}

		src, ok := value.(*MemoChannel[T])
		if !ok {
			log.Printf("Invalid upstream source: %v, %v", upstreamIdentity, value)
			return true
		}

		idtList := strings.Split(upstreamIdentity, upstreamIdtSep)
		if len(idtList) != 2 {
			log.Printf("Invalid upstream identity: %v", upstreamIdentity)
			return true
		}

		srcName := idtList[0]
		subID, _ := uuid.FromString(idtList[1])

		if srcName != src.Name() {
			log.Printf("Parsed upstream name[%s] mismatch with source: %s", srcName, src.ID())
		}

		if err := src.UnSubscribe(subID); err != nil {
			log.Printf("UnSubscribe from upstream[%s] failed: %v", src.ID(), err)
		}

		return true
	})
}

func (ch *MemoChannel[T]) closeSubs() {
	ch.subscriberCache.Range(func(subscriber, subData any) bool {
		pubCh := subData.(*sub[T])
		ch.subscriberCache.Delete(subscriber)

		log.Printf("Closing pub channel for subscriber: %s", subscriber.(uuid.UUID))

		pubCh.close()

		ch.subscriberWg.Done()

		return true
	})
}

func (ch *MemoChannel[T]) Join() {
	<-ch.runCtx.Done()

	ch.subscriberWg.Wait()
}

func (ch *MemoChannel[T]) makeChan() chan *T {
	if ch.chanLen > 0 {
		return make(chan *T, ch.chanLen)
	}

	return make(chan *T, defaultChanSize)
}

func (ch *MemoChannel[T]) inputDispatcher() {
	for {
		select {
		case <-ch.runCtx.Done():
			ch.Release()
		case v, ok := <-ch.input:
			if !ok {
				ch.closeSubs()
				return
			}

			ch.subscriberCache.Range(func(subcriber, subData any) bool {
				sub := subData.(*sub[T]).data

				select {
				case <-ch.timeout(500 * time.Millisecond):
					log.Printf("Publish timeout to subscriber[%+v].", subcriber)
				case sub <- v:
				}

				return true
			})
		}
	}
}

func (ch *MemoChannel[T]) Subscribe(name string, resumeType ResumeType) (uuid.UUID, <-chan *T) {
	subID := msgqueue.GenID(name)

	subData, subExist := ch.subscriberCache.LoadOrStore(subID, &sub[T]{data: ch.makeChan()})

	if subExist {
		log.Printf("Channel exist for subscriber[%v]", name)
	} else {
		ch.subscriberWg.Add(1)
		log.Printf("New subscriber[%+v]", name)
	}

	return subID, subData.(*sub[T]).ch()
}

func (ch *MemoChannel[T]) UnSubscribe(subID uuid.UUID) error {
	subData, subExist := ch.subscriberCache.LoadAndDelete(subID)

	if !subExist {
		return ErrNoSubcriber
	}

	subData.(*sub[T]).close()
	ch.subscriberWg.Done()

	return nil
}

func (ch *MemoChannel[T]) timeout(timeout time.Duration) <-chan time.Time {
	if timeout > 0 {
		return time.After(timeout)
	}

	return make(<-chan time.Time)
}

func (ch *MemoChannel[T]) Publish(v *T, timeout time.Duration) error {
	select {
	case <-ch.runCtx.Done():
		return ErrChanClosed
	case <-ch.timeout(timeout):
		return ErrPubTimeout
	case ch.input <- v:
		return nil
	}
}

func (ch *MemoChannel[T]) PipelineDownStream(dst Channel[T]) (Channel[T], error) {
	if dst == nil {
		dst = NewMemoChannel[T](context.Background(), "", ch.chanLen)
	}

	dst.PipelineUpStream(ch)

	return dst, nil
}

func (ch *MemoChannel[T]) makeUpstreamIdentity(src Channel[T], subID uuid.UUID) string {
	return strings.Join([]string{src.Name(), subID.String()}, upstreamIdtSep)
}

func (ch *MemoChannel[T]) PipelineUpStream(src Channel[T]) error {
	if src == nil {
		return ErrPipeline
	}

	subID, subCh := src.Subscribe(ch.name, Quick)
	if _, exist := ch.upstreamCache.LoadOrStore(subID, src); !exist {
		ch.upstreamWg.Add(1)

		go func() {
			defer ch.upstreamWg.Done()

			for v := range subCh {
				if err := ch.Publish(v, -1); err != nil {
					log.Printf(
						"Relay pipeline upstream[%s] failed: %+v",
						ch.makeUpstreamIdentity(src, subID), err,
					)
				}
			}
		}()
	} else {
		return ErrAlreadySubscribed
	}

	return nil
}
