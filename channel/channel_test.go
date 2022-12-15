package channel_test

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/frozenpine/msgqueue/channel"
	"github.com/frozenpine/msgqueue/core"
)

func TestChanType(t *testing.T) {
	memoCh := channel.NewMemoChannel[int](context.TODO(), "", -1)

	var ch core.QueueBase = memoCh

	t.Log(ch.Name(), ch.ID())
}

func TestCache(t *testing.T) {
	cache := sync.Map{}

	id := core.GenID("test")

	_, exist := cache.LoadOrStore(id, struct{}{})
	if exist {
		t.Fatal("id exist")
	}

	_, exist = cache.LoadAndDelete(id)
	if !exist {
		t.Fatal("cache key missing")
	}
}

func TestMemoCh(t *testing.T) {
	ch := channel.NewMemoChannel[int](context.TODO(), "", 1)

	t.Log("new channel", ch.Name(), ch.ID())

	dataMax := 100
	subCount := 2
	wg := sync.WaitGroup{}
	wg.Add(subCount)

	for idx := 0; idx < subCount; idx++ {
		go func(seq int) {
			subID, subCh := ch.Subscribe(strconv.Itoa(seq), core.Quick)

			defer func() {
				if err := ch.UnSubscribe(subID); err != nil {
					t.Logf("unsubscribe %+v error: %+v", subID, err)
				}

				t.Logf("sub channel[%d] %v closed", seq, subID)

				wg.Done()
			}()

			t.Logf("subscriber[%d]: %v", seq, subID)

			var v int

			for v = range subCh {
				if seq%2 == 0 && v >= dataMax/2 {
					return
				}

				t.Log(seq, subID, v)
			}

			if seq%2 != 0 && v != dataMax-1 {
				t.Errorf("sub channel[%d] %v has unread value", seq, subID)
			}
		}(idx)
	}

	// 引入延迟，以防输入过早完成释放memo channel
	<-time.After(3 * time.Second)

	for idx := 0; idx < dataMax; idx++ {
		if err := ch.Publish(idx, -1); err != nil {
			t.Logf("publish[%d] failed: %+v", idx, err)
		}
	}

	ch.Release()

	t.Log("waiting for channel close")
	ch.Join()

	t.Log("waiting for subscriber exit")
	wg.Wait()
}
