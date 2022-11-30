package flow

import (
	"github.com/frozenpine/msgqueue/core"
	"github.com/frozenpine/msgqueue/storage"
)

type FlowItem struct {
	Epoch    uint64
	Sequence uint64
	Data     storage.PersistentData
}

func (v *FlowItem) Less(than core.Item) bool {
	right, ok := than.(*FlowItem)

	if !ok {
		return false
	}

	if v.Epoch < right.Epoch {
		return true
	}

	if v.Epoch > right.Epoch {
		return false
	}

	return v.Sequence < right.Sequence
}

type BaseFlow interface {
	StartSequence() uint64
	EndSequence() uint64
	TotalDataSize() uint64
}

type Flow[T storage.PersistentData] interface {
	BaseFlow

	Write(data T) (seq uint64, err error)
	ReadAt(seq uint64) (T, error)
	ReadFrom(seq uint64) (<-chan T, error)
	ReadAll() (<-chan T, error)
}
