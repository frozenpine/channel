package flow

import (
	"sync"

	"github.com/frozenpine/msgqueue/core"
)

type PersistentData interface {
	Len() int
	Serialize() []byte
	Deserialize([]byte) error
}

var (
	typeCache     = []sync.Pool{}
	typeCacheLock = sync.RWMutex{}
)

type TID int

func RegisterType(newFn func() PersistentData) TID {
	typeCacheLock.Lock()
	defer typeCacheLock.Unlock()

	typeCache = append(typeCache, sync.Pool{New: func() any { return newFn() }})
	return TID(len(typeCache) - 1)
}

func NewTypeValue(tid TID) PersistentData {
	if tid < 0 || int(tid) >= len(typeCache) {
		return nil
	}

	return typeCache[tid].Get().(PersistentData)
}

type FlowItem struct {
	Epoch    uint64
	Sequence uint64
	TID      TID
	Data     PersistentData
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

type Flow[T PersistentData] interface {
	BaseFlow

	Write(data T) (seq uint64, err error)
	ReadAt(seq uint64) (T, error)
	ReadFrom(seq uint64) (<-chan T, error)
	ReadAll() (<-chan T, error)
}
