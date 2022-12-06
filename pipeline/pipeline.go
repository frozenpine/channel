package pipeline

import "github.com/frozenpine/msgqueue/channel"

type Sequence[S, V any] interface {
	Value() V
	Index() S

	Compare(than Sequence[S, V]) int

	IsWaterMark() bool
}

type Aggregatorable[S, V any] interface {
	WindowBy(<-chan Sequence[S, V]) <-chan Aggregatorable[S, V]
	FilterBy(func(Sequence[S, V]) bool) Aggregatorable[S, V]
	GroupBy(func(Sequence[S, V]) any) map[any]Aggregatorable[S, V]

	channel.Channel[V]
}
