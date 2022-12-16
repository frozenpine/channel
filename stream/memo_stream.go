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
	IDX comparable,
	IV, OV any,
	KEY comparable,
] struct {
	name     string
	id       uuid.UUID
	runCtx   context.Context
	cancelFn context.CancelFunc

	initOnce, releaseOnce sync.Once

	pipeline pipeline.Pipeline[Sequence[IDX, IV], Sequence[IDX, OV]]

	windowCache []Window[IDX, IV, OV]
	currWindow  Window[IDX, IV, OV]

	aggregator Aggregator[IDX, IV, OV]
}

func NewMemoStream[
	IDX comparable,
	IV, OV any,
	KEY comparable,
](
	ctx context.Context, name string,
	initWin Window[IDX, IV, OV],
	agg Aggregator[IDX, IV, OV],
) (*MemoStream[IDX, IV, OV, KEY], error) {
	if agg == nil {
		return nil, errors.Wrap(ErrInvalidAggregator, "aggregator missing")
	}
	stream := MemoStream[IDX, IV, OV, KEY]{}

	log.Print("NewMemoStream")

	stream.Init(ctx, name, func() {
		log.Print("MemoStream extraInit")

		if initWin == nil {
			initWin = &DefaultWindow[IDX, IV, OV]{}
		}

		stream.pipeline = pipeline.NewMemoPipeLine(
			context.Background(),
			stream.name+"_pipeline", stream.convert)
		stream.currWindow = initWin
		stream.aggregator = agg
	})

	return &stream, nil
}

func (strm *MemoStream[IDX, IV, OV, KEY]) convert(inData Sequence[IDX, IV], outChan core.Producer[Sequence[IDX, OV]]) error {
	err := strm.currWindow.Push(inData)

	switch {
	case errors.Is(err, ErrWindowClosed):
		result, err := strm.aggregator(strm.currWindow)

		if err != nil {
			return err
		}

		if err = outChan.Publish(result, -1); err != nil {
			log.Printf("Stream out failed: +%v", err)
			return err
		}

		strm.windowCache = append(strm.windowCache, strm.currWindow)
		strm.currWindow = strm.currWindow.NextWindow()

		return nil
	case errors.Is(err, ErrHistorySequence):
		log.Printf("History sequence[%v] arrived: %+v", inData.Index(), inData.Value())

		return nil
	default:
		return err
	}
}

func (strm *MemoStream[IDX, IV, OV, KEY]) ID() uuid.UUID {
	return strm.id
}

func (strm *MemoStream[IDX, IV, OV, KEY]) Name() string {
	return strm.name
}

func (strm *MemoStream[IDX, IV, OV, KEY]) Init(ctx context.Context, name string, extraInit func()) {
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

func (strm *MemoStream[IDX, IV, OV, KEY]) Join() {
	<-strm.runCtx.Done()

	strm.pipeline.Join()

	// TODO: extra join
}

func (strm *MemoStream[IDX, IV, OV, KEY]) Release() {
	strm.releaseOnce.Do(func() {
		strm.cancelFn()

		strm.pipeline.Release()
		// TODO: extra release
	})
}

func (strm *MemoStream[IDX, IV, OV, KEY]) Publish(v Sequence[IDX, IV], timeout time.Duration) error {
	return strm.pipeline.Publish(v, timeout)
}

func (strm *MemoStream[IDX, IV, OV, KEY]) Subscribe(name string, resume core.ResumeType) (uuid.UUID, <-chan Sequence[IDX, OV]) {
	return strm.pipeline.Subscribe(name, resume)
}

func (strm *MemoStream[IDX, IV, OV, KEY]) UnSubscribe(subID uuid.UUID) error {
	return strm.pipeline.UnSubscribe(subID)
}

func (strm *MemoStream[IDX, IV, OV, KEY]) PipelineUpstream(src core.Consumer[Sequence[IDX, IV]]) error {
	return strm.pipeline.PipelineUpStream(src)
}

func (strm *MemoStream[IDX, IV, OV, KEY]) PipelineDownStream(dst core.Upstream[Sequence[IDX, OV]]) error {
	return strm.pipeline.PipelineDownStream(dst)
}
