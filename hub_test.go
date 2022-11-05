package channel

import (
	"context"
	"testing"
)

func TestHub(t *testing.T) {
	topic := "test"
	sub1 := "sub1"
	sub2 := "sub2"

	hub := NewHub[int](context.TODO(), -1)

	go func() {
		for v := range hub.Subscribe(topic, sub1) {
			t.Log(sub1, v)
		}
	}()

	go func() {
		for v := range hub.Subscribe(topic, sub2) {
			t.Log(sub2, v)
		}
	}()

	for idx := 0; idx < 100; idx++ {
		if err := hub.Publish(topic, idx, 0); err != nil {
			t.Log(idx, err)
		}
	}

	hub.Stop()

	hub.Join()
}
