package pipeline

import (
	"github.com/frozenpine/msgqueue/core"
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

type Aggregatorable[
	IS, IV any,
	OS, OV any,
	KEY comparable,
] interface {
	WindowBy(<-chan WaterMark) <-chan Aggregatorable[IS, IV, OS, OV, KEY]
	FilterBy(func(Sequence[IS, IV]) bool) Aggregatorable[IS, IV, OS, OV, KEY]
	GroupBy(func(Sequence[IS, IV]) KEY) map[KEY]Aggregatorable[IS, IV, OS, OV, KEY]
	Action(func([]Sequence[IS, IV]) Sequence[OS, OV])

	core.Producer[Sequence[IS, IV]]
	core.Consumer[Sequence[OS, OV]]
}
