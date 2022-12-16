package stream

import (
	"github.com/pkg/errors"

	"github.com/frozenpine/msgqueue/pipeline"
)

var (
	ErrFutureTick        = errors.New("future tick")
	ErrHistoryTick       = errors.New("history tick")
	ErrInvalidAggregator = errors.New("invalid aggregator")
	ErrWindowClosed      = errors.New("window closed")
)

type Sequence[IDX comparable, V any] interface {
	Value() V
	Index() IDX

	Compare(than Sequence[IDX, V]) int

	IsWaterMark() bool
}

type Aggregator[
	IDX comparable,
	IV, OV any,
] func(Window[IDX, IV, OV]) (Sequence[IDX, OV], error)

type Window[
	IDX comparable,
	IV, OV any,
] interface {
	Indexs() []IDX
	Values() []IV
	Series() []Sequence[IDX, IV]
	Push(Sequence[IDX, IV]) error
	PreWindow() Window[IDX, IV, OV]
	NextWindow() Window[IDX, IV, OV]
}

type Stream[
	IDX comparable,
	IV, OV any,
	KEY comparable,
] interface {
	pipeline.Pipeline[Sequence[IDX, IV], Sequence[IDX, OV]]

	FilterBy(func(Sequence[IDX, IV]) bool) Stream[IDX, IV, OV, KEY]
	GroupBy(func(Sequence[IDX, IV]) KEY) map[KEY]Stream[IDX, IV, OV, KEY]

	// PreWindow get n count previous window
	// if n count <= 0, will return current window
	PreWindow(n int) Window[IDX, IV, OV]
	CurrWindow() Window[IDX, IV, OV]
}

type DefaultWindow[
	IDX comparable,
	IV, OV any,
] struct {
	pre      Window[IDX, IV, OV]
	sequence []Sequence[IDX, IV]
}

func (win *DefaultWindow[IDX, IV, OV]) Indexs() []IDX {
	index := make([]IDX, len(win.sequence))

	for idx, v := range win.sequence {
		index[idx] = v.Index()
	}

	return index
}

func (win *DefaultWindow[IDX, IV, OV]) Values() []IV {
	values := make([]IV, len(win.sequence))

	for idx, v := range win.sequence {
		values[idx] = v.Value()
	}

	return values
}

func (win *DefaultWindow[IDX, IV, OV]) Series() []Sequence[IDX, IV] {
	return win.sequence
}

func (win *DefaultWindow[IDX, IV, OV]) Push(seq Sequence[IDX, IV]) error {
	if seq.IsWaterMark() {
		return ErrWindowClosed
	}

	win.sequence = append(win.sequence, seq)

	return nil
}

func (win *DefaultWindow[IDX, IV, OV]) PreWindow() Window[IDX, IV, OV] {
	return win.pre
}

func (win *DefaultWindow[IDX, IV, OV]) NextWindow() Window[IDX, IV, OV] {
	return &DefaultWindow[IDX, IV, OV]{
		pre: win,
	}
}
