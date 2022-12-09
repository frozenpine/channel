package pipeline

import (
	"context"

	"github.com/frozenpine/msgqueue/core"
	"github.com/pkg/errors"
)

var (
	ErrFutureTick  = errors.New("future tick")
	ErrHistoryTick = errors.New("history tick")
)

type WaterMark interface {
	IsWaterMark() bool
}

type Sequence[S, V any] interface {
	Value() V
	Index() S

	Compare(than Sequence[S, V]) int

	WaterMark
}

type SequenceSlice[S, V any] interface {
	Push(d V) error
}

type BasePipe interface {
	core.QueueBase

	Release()
	Join()

	init(ctx context.Context, name string, extraInit func())
}

type Pipeline[
	IS, IV comparable,
	OS, OV comparable,
	KEY comparable,
] interface {
	BasePipe
	core.Producer[IV]
	core.Consumer[OV]
	core.Upstream[IV]
	core.Downstream[OV]
}

type Aggregatorable[
	IS, IV comparable,
	OS, OV comparable,
	KEY comparable,
] interface {
	WindowBy(<-chan WaterMark) Aggregatorable[IS, IV, OS, OV, KEY]
	FilterBy(func(Sequence[IS, IV]) bool) Aggregatorable[IS, IV, OS, OV, KEY]
	GroupBy(func(Sequence[IS, IV]) KEY) Aggregatorable[IS, IV, OS, OV, KEY]
	Action(func(SequenceSlice[IS, IV]) Sequence[OS, OV])
	GetGroupAggregator(KEY) Aggregatorable[IS, IV, OS, OV, KEY]
}
