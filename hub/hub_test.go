package hub

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

func TestMemoHub(t *testing.T) {
	topic := "test"

	hub := NewMemoHub[int](context.TODO(), -1)

	dataMax := 99
	subCount := 2
	wg := sync.WaitGroup{}
	wg.Add(subCount)

	for idx := 0; idx < subCount; idx++ {
		sub := fmt.Sprintf("%s%d", "sub", idx)

		go func() {
			defer wg.Done()

			var v int

			for v = range hub.Subscribe(topic, sub) {
				t.Log(sub, v)
			}

			if v != dataMax {
				t.Errorf("%s channel has unread value", sub)
			}

			t.Logf("%s channel closed", sub)
		}()
	}

	for idx := 0; idx <= dataMax; idx++ {
		if err := hub.Publish(topic, idx, 0); err != nil {
			t.Log(idx, err)
		}
	}

	hub.Stop()

	hub.Join()

	wg.Wait()
}
