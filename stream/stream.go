package stream

import "github.com/frozenpine/msgqueue/pipeline"

type Window[S, V any] interface {
	Indexs() []S
	Values() []V
	Series() []pipeline.Sequence[S, V]
	Push(d V) error
}

type Aggregatorable[
	IS, IV comparable,
	OS, OV comparable,
	KEY comparable,
] interface {
	WindowBy(func() <-chan pipeline.WaterMark) Aggregatorable[IS, IV, OS, OV, KEY]
	FilterBy(func(pipeline.Sequence[IS, IV]) bool) Aggregatorable[IS, IV, OS, OV, KEY]
	GroupBy(func(pipeline.Sequence[IS, IV]) KEY) Aggregatorable[IS, IV, OS, OV, KEY]
	Action(func(Window[IS, IV]) pipeline.Sequence[OS, OV])
	Groups() map[KEY]Aggregatorable[IS, IV, OS, OV, KEY]
}

type Stream[
	IS, IV comparable,
	OS, OV comparable,
	KEY comparable,
] interface {
	pipeline.Pipeline[IS, IV, OS, OV]
}
