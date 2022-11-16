package hub

import (
	"context"
	"log"
	"net"
	"sync"

	"github.com/frozenpine/channel/storage"
)

type RemoteHub[T storage.PersistentData] struct {
	MemoHub[T]

	connDone  chan struct{}
	clients   sync.Map
	topicConn sync.Map
}

func NewRemoteHub[T storage.PersistentData](ctx context.Context, name string, bufSize int) *RemoteHub[T] {
	if ctx == nil {
		ctx = context.Background()
	}

	hub := RemoteHub[T]{}

	hub.chanLen = bufSize
	hub.id = GenID(name)

	hub.initOnce.Do(func() {
		hub.runCtx, hub.cancelFn = context.WithCancel(ctx)
	})

	return &hub
}

func (hub *RemoteHub[T]) clientHandler(conn net.Conn) {
	defer hub.clients.Delete(conn.RemoteAddr())

	hub.clients.Store(conn.RemoteAddr(), conn)

	// buffer := make([]byte, 1500)

	for {
		// n, err := conn.Read(buffer)
	}
}

func (hub *RemoteHub[T]) StartServer(listen string) (err error) {
	var lsnr net.Listener

	if lsnr, err = (&net.ListenConfig{}).Listen(
		hub.runCtx, "tcp", listen,
	); err != nil {
		return
	}

	go func() {
		defer func() { hub.connDone <- struct{}{} }()

		for {
			conn, err := lsnr.Accept()

			if err != nil {
				log.Printf("Client accept faild: %+v", err)
				return
			}

			go hub.clientHandler(conn)
		}
	}()

	return nil
}

func (hub *RemoteHub[T]) StartClient(remote string) error {
	return nil
}

func (hub *RemoteHub[T]) Stop() error {
	hub.cancelFn()

	<-hub.connDone

	hub.clients.Range(func(key, value any) bool {
		remoteAddr := key.(net.Addr)
		clientConn := value.(net.Conn)

		log.Printf("Closing client[%s]: %+v", remoteAddr, clientConn.Close())

		return true
	})

	return nil
}
