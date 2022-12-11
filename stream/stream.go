package stream

import (
	"github.com/pkg/errors"

	"github.com/frozenpine/msgqueue/pipeline"
)

var (
	ErrFutureTick  = errors.New("future tick")
	ErrHistoryTick = errors.New("history tick")
)

type Window[
	IS, IV any,
	OS, OV any,
] interface {
	Indexs() []IS
	Values() []IV
	Series() []pipeline.Sequence[IS, IV]
	Push(d IV) error
	pipeline.Converter[IS, IV, OS, OV]
}

type Aggregatorable[
	IS, IV any,
	OS, OV any,
	KEY comparable,
] interface {
	WindowBy(func() <-chan pipeline.WaterMark) Aggregatorable[IS, IV, OS, OV, KEY]
	FilterBy(func(pipeline.Sequence[IS, IV]) bool) Aggregatorable[IS, IV, OS, OV, KEY]
	GroupBy(func(pipeline.Sequence[IS, IV]) KEY) Aggregatorable[IS, IV, OS, OV, KEY]
	Groups() map[KEY]Aggregatorable[IS, IV, OS, OV, KEY]
}

type Stream[
	IS, IV any,
	OS, OV any,
	KEY comparable,
] interface {
	pipeline.Pipeline[IS, IV, OS, OV]
	Aggregatorable[IS, IV, OS, OV, KEY]
}
