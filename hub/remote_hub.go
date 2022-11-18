package hub

import (
	"context"
	"log"
	"net"
	"sync"

	"github.com/frozenpine/channel/hub/protocol"
	"github.com/frozenpine/channel/storage"
	"github.com/gofrs/uuid"
	"google.golang.org/grpc"
)

var (
	rtnDataPool = sync.Pool{New: func() any { return &protocol.RtnData{} }}
)

type clientSub struct {
	conn  protocol.HubService_SubscribeServer
	topic string
	subID uuid.UUID
}

type server[T storage.PersistentData] struct {
	protocol.UnimplementedHubServiceServer

	lsnr    net.Listener
	grpcSvr *grpc.Server

	topicSub sync.Map

	upstream Hub[T]
}

func newServer[T storage.PersistentData](listen string, upstream *RemoteHub[T]) (svr *server[T], err error) {
	svr = &server[T]{}
	if svr.lsnr, err = net.Listen("tcp", listen); err != nil {
		return
	}

	svr.grpcSvr = grpc.NewServer()
	protocol.RegisterHubServiceServer(svr.grpcSvr, svr)
	svr.upstream = upstream

	go func() {
		if err := svr.grpcSvr.Serve(svr.lsnr); err != nil {
			log.Printf("gRPC server error: %v", err)

			upstream.Stop()
		}
	}()

	return
}

func (svr *server[T]) Stop() {
	svr.grpcSvr.GracefulStop()
}

func (svr *server[T]) getTopicSub(topic string) *sync.Map {
	topicSub, _ := svr.topicSub.LoadOrStore(topic, &sync.Map{})

	return topicSub.(*sync.Map)
}

func (svr *server[T]) Subscribe(req *protocol.ReqSub, conn protocol.HubService_SubscribeServer) error {
	topicSub := svr.getTopicSub(req.Topic)

	if _, exist := topicSub.Load(req.Subscriber); !exist {
		subID, subChan := svr.upstream.Subscribe(req.Topic, req.Subscriber, ResumeType(req.ResumeType))

		topicSub.Store(subID.String(), &clientSub{
			conn:  conn,
			topic: req.Topic,
			subID: subID,
		})

		go func(topic string) {
			for sub := range subChan {
				rtn := rtnDataPool.Get().(*protocol.RtnData)

				rtn.Topic = topic
				// rtn.Seq
				rtn.Data = sub.Serialize()
				rtn.Len = uint32(len(rtn.Data))

				if err := conn.Send(rtn); err != nil {
					log.Printf("gRPC send[%s] data failed: %+v", topic, err)
					break
				}
			}
		}(req.Topic)
	}

	return ErrAlreadySubscribed
}

type RemoteHub[T storage.PersistentData] struct {
	MemoHub[T]

	server *server[T]
	client protocol.HubServiceClient
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

func (hub *RemoteHub[T]) StartServer(listen string) (err error) {
	hub.server, err = newServer(listen, hub)

	return
}

func (hub *RemoteHub[T]) StartClient(remote string) (err error) {
	var client *grpc.ClientConn
	client, err = grpc.DialContext(hub.runCtx, remote)
	if err != nil {
		return
	}

	hub.client = protocol.NewHubServiceClient(client)

	return
}

func (hub *RemoteHub[T]) Stop() error {
	hub.cancelFn()

	// <-hub.connDone

	// hub.clients.Range(func(key, value any) bool {
	// 	remoteAddr := key.(net.Addr)
	// 	clientConn := value.(net.Conn)

	// 	log.Printf("Closing client[%s]: %+v", remoteAddr, clientConn.Close())

	// 	return true
	// })

	return nil
}
