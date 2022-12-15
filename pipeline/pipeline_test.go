package pipeline

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/frozenpine/msgqueue/core"
)

func TestMemoPipeline(t *testing.T) {
	line := NewMemoPipeLine(
		context.TODO(), "pipeline",
		func(s int, c core.Producer[float64]) error {
			o := float64(s) * 0.5

			return c.Publish(o, -1)
		},
	)

	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		subID, ch := line.Subscribe("test1", core.Quick)

		defer func() {
			line.UnSubscribe(subID)
			wg.Done()
		}()

		for v := range ch {
			t.Log("output:", v)
		}
	}()

	<-time.After(1 * time.Second)
	for idx := 0; idx < 100; idx++ {
		line.Publish(idx, -1)
	}

	line.Release()

	line.Join()

	wg.Wait()
}
