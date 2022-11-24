package hub

import (
	"context"
	"testing"

	"github.com/frozenpine/msgqueue/channel"
)

func TestMemoHub(t *testing.T) {
	hub := NewMemoHub(context.TODO(), "memo", -1)

	t.Log("new hub:", hub.Name(), hub.ID())

	topic := "integer"
	vCount := 100

	_, err := GetHubTopicChannel[int](hub, topic)
	if err != ErrNoTopic {
		t.Fatal("topic channel should be nil before publish")
	}

	done := make(chan struct{})

	go func() {
		defer close(done)

		topicCh, err := GetOrCreateTopicChannel[int](hub, topic)

		if topicCh == nil {
			t.Error("create channel error", err)
			return
		}

		t.Log(topicCh.Name(), topicCh.ID(), err)

		subID, data := topicCh.Subscribe("test1", channel.Quick)

		t.Logf("channel sub id: %+v", subID)

		for v := range data {
			t.Log(subID, *v)
		}
	}()

	topicCh, err := GetOrCreateTopicChannel[int](hub, topic)
	if topicCh == nil {
		t.Fatal("create channel error", err)
	}
	t.Log(topicCh.Name(), topicCh.ID(), err)

	data := make([]int, vCount)

	for idx := 0; idx < vCount; idx++ {
		data[idx] = idx

		if err := topicCh.Publish(&data[idx], -1); err != nil {
			t.Error("publish error:", err)
		}
	}

	hub.Release()

	hub.Join()

	<-done
}
