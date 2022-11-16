package hub

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

func TestMemoHub(t *testing.T) {
	topic := "test"

	hub := NewMemoHub[int](context.TODO(), "", -1)

	dataMax := 99
	subCount := 2
	wg := sync.WaitGroup{}
	wg.Add(subCount)

	for idx := 0; idx < subCount; idx++ {
		subName := fmt.Sprintf("%s%d", "sub", idx)

		go func(idx int) {
			defer wg.Done()

			var v int

			subID, subCh := hub.Subscribe(topic, subName, Quick)
			t.Logf("topics after %s sub: %+v", subName, hub.Topics())

			for v = range subCh {
				if idx%2 == 0 && v == dataMax/2 {
					if err := hub.UnSubscribe(topic, subID); err != nil {
						t.Error(err)
					}
				}
				t.Log(subName, subID, v)
			}

			if idx%2 != 0 && v != dataMax {
				t.Errorf("%s channel has unread value", subName)
			}

			t.Logf("%s channel closed", subName)
		}(idx)
	}

	for idx := 0; idx <= dataMax; idx++ {
		if err := hub.Publish(topic, idx, 0); err != nil {
			t.Log(idx, err)
		}
	}

	hub.Release()

	t.Log("topics after hub stop:", hub.Topics())

	hub.Join()

	wg.Wait()

	t.Log("topics after sub quit:", hub.Topics())
}
