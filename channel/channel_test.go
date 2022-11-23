package channel_test

import (
	"context"
	"strconv"
	"sync"
	"testing"

	"github.com/frozenpine/msgqueue/channel"
)

func TestMemoCh(t *testing.T) {
	ch := channel.NewMemoChannel[int](context.TODO(), "", 1)

	t.Log("new channel", ch.Name(), ch.ID())

	dataMax := 99
	subCount := 2
	wg := sync.WaitGroup{}
	wg.Add(subCount)

	for idx := 0; idx < subCount; idx++ {
		go func(idx int) {
			defer wg.Done()

			subID, subCh := ch.Subscribe(strconv.Itoa(idx), channel.Quick)

			t.Logf("subscriber[%d]: %v", idx, subID)

			var v *int

			for v = range subCh {
				if idx%2 == 0 && *v >= dataMax/2 {
					if err := ch.UnSubscribe(subID); err != nil {
						t.Logf("sub channel[%d] %v UnSubscribe failed: %+v", idx, subID, err)
					}
				}

				t.Log(idx, subID, *v)
			}

			if idx%2 != 0 && *v != dataMax-1 {
				t.Errorf("sub channel[%d] %v has unread value", idx, subID)
			}

			t.Logf("sub channel[%d] %v closed", idx, subID)
		}(idx)
	}

	var data = make([]int, dataMax)

	for idx := 0; idx < dataMax; idx++ {
		data[idx] = idx

		if err := ch.Publish(&data[idx], -1); err != nil {
			t.Logf("publish[%d] failed: %+v", idx, err)
		}
	}

	ch.Release()

	t.Log("waiting for channel close")
	ch.Join()

	t.Log("waiting for subscriber exit")
	wg.Wait()
}
