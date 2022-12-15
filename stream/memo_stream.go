package stream

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/frozenpine/msgqueue/core"
	"github.com/frozenpine/msgqueue/pipeline"
	"github.com/gofrs/uuid"
)

type MemoStream[
	IS, IV any,
	OS, OV any,
	KEY comparable,
] struct {
	name     string
	id       uuid.UUID
	runCtx   context.Context
	cancelFn context.CancelFunc

	initOnce, releaseOnce sync.Once

	pipeline pipeline.Pipeline[Sequence[IS, IV], Sequence[OS, OV]]

	windowCache []Window[IS, IV, OS, OV]
	currWindow  Window[IS, IV, OS, OV]

	aggregator Aggregator[IS, IV, OS, OV]
}

func NewMemoStream[
	IS, IV any,
	OS, OV any,
	KEY comparable,
](
	ctx context.Context, name string,
	initWin Window[IS, IV, OS, OV],
	agg Aggregator[IS, IV, OS, OV],
) (*MemoStream[IS, IV, OS, OV, KEY], error) {
	if agg == nil {
		return nil, errors.Wrap(ErrInvalidAggregator, "aggregator missing")
	}
	stream := MemoStream[IS, IV, OS, OV, KEY]{}

	log.Print("NewMemoStream")

	stream.Init(ctx, name, func() {
		log.Print("MemoStream extraInit")

		if initWin == nil {
			initWin = &DefaultWindow[IS, IV, OS, OV]{}
		}

		stream.pipeline = pipeline.NewMemoPipeLine(
			context.Background(),
			stream.name+"_pipeline", stream.convert)
		stream.currWindow = initWin
		stream.aggregator = agg
	})

	return &stream, nil
}

func (strm *MemoStream[IS, IV, OS, OV, KEY]) convert(inData Sequence[IS, IV], outChan core.Producer[Sequence[OS, OV]]) error {
	err := strm.currWindow.Push(inData)

	switch err {
	case ErrFutureTick:
		result, err := strm.aggregator(strm.currWindow)

		if err != nil {
			return err
		}

		if err = outChan.Publish(result, -1); err != nil {
			log.Printf("Stream out failed: +%v", err)
		}

		strm.windowCache = append(strm.windowCache, strm.currWindow)
		strm.currWindow = strm.currWindow.NextWindow()
	default:
		return err
	}

	return nil
}

func (strm *MemoStream[IS, IV, OS, OV, KEY]) ID() uuid.UUID {
	return strm.id
}

func (strm *MemoStream[IS, IV, OS, OV, KEY]) Name() string {
	return strm.name
}

func (strm *MemoStream[IS, IV, OS, OV, KEY]) Init(ctx context.Context, name string, extraInit func()) {
	strm.initOnce.Do(func() {
		log.Print("MemoStream Init")
		if ctx == nil {
			ctx = context.Background()
		}

		if name == "" {
			name = "MemoStream"
		}

		strm.runCtx, strm.cancelFn = context.WithCancel(ctx)
		strm.name = core.GenName(name)
		strm.id = core.GenID(strm.name)

		if extraInit != nil {
			extraInit()
		}
	})
}

func (strm *MemoStream[IS, IV, OS, OV, KEY]) Join() {
	<-strm.runCtx.Done()

	strm.pipeline.Join()

	// TODO: extra join
}

func (strm *MemoStream[IS, IV, OS, OV, KEY]) Release() {
	strm.releaseOnce.Do(func() {
		strm.cancelFn()

		strm.pipeline.Release()
		// TODO: extra release
	})
}

func (strm *MemoStream[IS, IV, OS, OV, KEY]) Publish(v Sequence[IS, IV], timeout time.Duration) error {
	return strm.pipeline.Publish(v, timeout)
}

func (strm *MemoStream[IS, IV, OS, OV, KEY]) Subscribe(name string, resume core.ResumeType) (uuid.UUID, <-chan Sequence[OS, OV]) {
	return strm.pipeline.Subscribe(name, resume)
}

func (strm *MemoStream[IS, IV, OS, OV, KEY]) UnSubscribe(subID uuid.UUID) error {
	return strm.pipeline.UnSubscribe(subID)
}

func (strm *MemoStream[IS, IV, OS, OV, KEY]) PipelineUpstream(src core.Consumer[Sequence[IS, IV]]) error {
	return strm.pipeline.PipelineUpStream(src)
}

func (strm *MemoStream[IS, IV, OS, OV, KEY]) PipelineDownStream(dst core.Upstream[Sequence[OS, OV]]) error {
	return strm.pipeline.PipelineDownStream(dst)
}
