package channel

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestHub(t *testing.T) {
	topic := "test"
	sub1 := "sub1"
	sub2 := "sub2"

	hub := NewHub[int](context.TODO(), 10, time.Duration(1)*time.Second)

	wg := sync.WaitGroup{}
	wg.Add(2)

	go func() {
		for v := range hub.Subscribe(topic, sub1) {
			t.Log(sub1, v)
		}

		wg.Done()
	}()

	go func() {
		for v := range hub.Subscribe(topic, sub2) {
			t.Log(sub2, v)
		}

		wg.Done()
	}()

	for idx := 0; idx < 100; idx++ {
		if err := hub.PublishWithTimeout(topic, idx, 0); err != nil {
			t.Log(err)
		}
	}

	hub.Stop()

	wg.Wait()
}
