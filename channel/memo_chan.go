package channel

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/frozenpine/msgqueue/core"
	"github.com/gofrs/uuid"
	"github.com/pkg/errors"
)

type sub[T any] struct {
	once sync.Once
	data chan T
}

func (sub *sub[T]) close() {
	sub.once.Do(func() { close(sub.data) })
}

func (sub *sub[T]) ch() <-chan T {
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

	input        chan T
	waitInfinite <-chan time.Time

	subscriberCache sync.Map
	subscriberWg    sync.WaitGroup
	upstreamCache   sync.Map
	upstreamWg      sync.WaitGroup
}

func NewMemoChannel[T any](ctx context.Context, name string, bufSize int) *MemoChannel[T] {
	channel := MemoChannel[T]{}

	if bufSize <= 0 {
		bufSize = defaultChanSize
	}

	channel.Init(ctx, name, func() {
		channel.chanLen = bufSize
	})

	return &channel
}

func (ch *MemoChannel[T]) Init(ctx context.Context, name string, extraInit func()) {
	ch.initOnce.Do(func() {
		if ctx == nil {
			ctx = context.Background()
		}

		if name == "" {
			name = "MemoChan"
		}

		ch.runCtx, ch.cancelFn = context.WithCancel(ctx)
		ch.name = core.GenName(name)
		ch.id = core.GenID(ch.name)
		ch.input = ch.makeChan()
		ch.waitInfinite = make(chan time.Time)

		if extraInit != nil {
			extraInit()
		}

		go ch.inputDispatcher()
	})

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
		subID, ok := key.(uuid.UUID)

		if !ok {
			log.Printf("Invalid upstream subID: %v", key)
			return true
		}

		upstream, ok := value.(core.Consumer[T])
		if !ok {
			log.Printf("Invalid upstream source: %v", value)
			return true
		}

		if err := upstream.UnSubscribe(subID); err != nil {
			log.Printf("UnSubscribe from upstream[%s] failed: %v", upstream.ID(), err)
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

func (ch *MemoChannel[T]) makeChan() chan T {
	if ch.chanLen > 0 {
		return make(chan T, ch.chanLen)
	}

	return make(chan T, defaultChanSize)
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

func (ch *MemoChannel[T]) Subscribe(name string, resumeType core.ResumeType) (uuid.UUID, <-chan T) {
	subID := core.GenID(name)

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
		return core.ErrNoSubcriber
	}

	subData.(*sub[T]).close()
	ch.subscriberWg.Done()

	return nil
}

func (ch *MemoChannel[T]) timeout(timeout time.Duration) <-chan time.Time {
	if timeout > 0 {
		return time.After(timeout)
	}

	return ch.waitInfinite
}

func (ch *MemoChannel[T]) Publish(v T, timeout time.Duration) error {
	select {
	case <-ch.runCtx.Done():
		return ErrChanClosed
	case <-ch.timeout(timeout):
		return core.ErrPubTimeout
	case ch.input <- v:
		return nil
	}
}

func (ch *MemoChannel[T]) PipelineDownStream(dst core.Upstream[T]) error {
	if dst == nil {
		return errors.Wrap(core.ErrPipeline, "empty down stream")
	}

	return dst.PipelineUpStream(ch)
}

func (ch *MemoChannel[T]) PipelineUpStream(src core.Consumer[T]) error {
	if src == nil {
		return errors.Wrap(core.ErrPipeline, "upstream empty")
	}

	subID, subCh := src.Subscribe(ch.name, core.Quick)
	if _, exist := ch.upstreamCache.LoadOrStore(subID, src); !exist {
		ch.upstreamWg.Add(1)

		go func() {
			defer ch.upstreamWg.Done()

			for {
				select {
				case <-ch.runCtx.Done():
					return
				case v, ok := <-subCh:
					if !ok {
						return
					}

					if err := ch.Publish(v, -1); err != nil {
						log.Printf(
							"Relay pipeline upstream %s failed: %+v",
							core.QueueIdentity(src), err,
						)
					}
				}
			}
		}()
	} else {
		return core.ErrAlreadySubscribed
	}

	return nil
}
