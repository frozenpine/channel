package hub

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
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

type Int int

func (v Int) FixSized() bool { return true }
func (v Int) GetSize() int   { return 4 }
func (v Int) Serialize() (out []byte) {
	binary.LittleEndian.PutUint32(out, uint32(v))
	return
}
func (v Int) Deserialize(data []byte) error {
	v = Int(binary.LittleEndian.Uint32(data))
	return nil
}

func TestRemoteHub(t *testing.T) {
	ctx, fn := context.WithCancel(context.Background())

	server := NewRemoteHub[Int](ctx, "server", 10)
	client := NewRemoteHub[Int](ctx, "client", 10)

	addr := "127.0.0.1:49152"

	if err := server.StartServer(addr); err != nil {
		t.Fatal(err)
	}

	if err := client.StartClient(addr); err != nil {
		t.Fatal(err)
	}

	go func() {
		for {
			if err := server.Publish("test", Int(rand.Intn(100)), -1); err != nil {
				t.Logf("Server pub error: %v", err)
			}
		}
	}()

	go func() {
		_, ch := client.Subscribe("test", "client01", Quick)

		for v := range ch {
			fmt.Printf("Client sub: %d", v)
		}
	}()

	go func() {
		<-time.After(time.Second * 60)
		fn()
	}()

	<-ctx.Done()
}
