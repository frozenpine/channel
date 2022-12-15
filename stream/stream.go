package stream

import (
	"github.com/pkg/errors"

	"github.com/frozenpine/msgqueue/pipeline"
)

var (
	ErrFutureTick        = errors.New("future tick")
	ErrHistoryTick       = errors.New("history tick")
	ErrInvalidAggregator = errors.New("invalid aggregator")
)

type Aggregator[
	IS, IV any,
	OS, OV any,
] func(Window[IS, IV, OS, OV]) (pipeline.Sequence[OS, OV], error)

type Window[
	IS, IV any,
	OS, OV any,
] interface {
	Indexs() []IS
	Values() []IV
	Series() []pipeline.Sequence[IS, IV]
	Push(pipeline.Sequence[IS, IV]) error
	NextWindow() Window[IS, IV, OS, OV]
}

type Stream[
	IS, IV any,
	OS, OV any,
	KEY comparable,
] interface {
	pipeline.Pipeline[IS, IV, OS, OV]

	WindowBy(func() <-chan pipeline.WaterMark) Stream[IS, IV, OS, OV, KEY]
	FilterBy(func(pipeline.Sequence[IS, IV]) bool) Stream[IS, IV, OS, OV, KEY]
	GroupBy(func(pipeline.Sequence[IS, IV]) KEY) map[KEY]Stream[IS, IV, OS, OV, KEY]

	// PreWindow get n count previous window
	// if n count <= 0, will return current window
	PreWindow(n int) Window[IS, IV, OS, OV]
	CurrWindow() Window[IS, IV, OS, OV]
}
