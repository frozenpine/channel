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

type WaterMark interface {
	IsWaterMark() bool
}

type Sequence[S, V any] interface {
	Value() V
	Index() S

	Compare(than Sequence[S, V]) int

	WaterMark
}

type Aggregator[
	IS, IV any,
	OS, OV any,
] func(Window[IS, IV, OS, OV]) (Sequence[OS, OV], error)

type Window[
	IS, IV any,
	OS, OV any,
] interface {
	Indexs() []IS
	Values() []IV
	Series() []Sequence[IS, IV]
	Push(Sequence[IS, IV]) error
	NextWindow() Window[IS, IV, OS, OV]
}

type Stream[
	IS, IV any,
	OS, OV any,
	KEY comparable,
] interface {
	pipeline.Pipeline[Sequence[IS, IV], Sequence[OS, OV]]

	WindowBy(func() <-chan WaterMark) Stream[IS, IV, OS, OV, KEY]
	FilterBy(func(Sequence[IS, IV]) bool) Stream[IS, IV, OS, OV, KEY]
	GroupBy(func(Sequence[IS, IV]) KEY) map[KEY]Stream[IS, IV, OS, OV, KEY]

	// PreWindow get n count previous window
	// if n count <= 0, will return current window
	PreWindow(n int) Window[IS, IV, OS, OV]
	CurrWindow() Window[IS, IV, OS, OV]
}

type DefaultWindow[
	IS, IV any,
	OS, OV any,
] struct {
	sequence []Sequence[IS, IV]
}

func (win *DefaultWindow[IS, IV, OS, OV]) Indexs() []IS {
	index := make([]IS, len(win.sequence))

	for idx, v := range win.sequence {
		index[idx] = v.Index()
	}

	return index
}

func (win *DefaultWindow[IS, IV, OS, OV]) Values() []IV {
	values := make([]IV, len(win.sequence))

	for idx, v := range win.sequence {
		values[idx] = v.Value()
	}

	return values
}

func (win *DefaultWindow[IS, IV, OS, OV]) Series() []Sequence[IS, IV] {
	return win.sequence
}

func (win *DefaultWindow[IS, IV, OS, OV]) Push(seq Sequence[IS, IV]) error {
	win.sequence = append(win.sequence, seq)

	if seq.IsWaterMark() {
		return ErrFutureTick
	}

	return nil
}

func (win *DefaultWindow[IS, IV, OS, OV]) NextWindow() Window[IS, IV, OS, OV] {
	return &DefaultWindow[IS, IV, OS, OV]{}
}
