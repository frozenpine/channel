package flow

import (
	"errors"
	"runtime"
	"sync"

	"github.com/frozenpine/msgqueue/core"
)

type PersistentData interface {
	Serialize() []byte
	Deserialize([]byte) error
}

var (
	typeCache = []sync.Pool{}
	// typeCacheLock = sync.RWMutex{}
)

type TID int

// RegisterType register PersistentData type.
// TID present unique identity for PersistentData
// and type register action must always in same order
func RegisterType(newFn func() PersistentData) TID {
	// typeCacheLock.Lock()
	// defer typeCacheLock.Unlock()

	typeCache = append(typeCache, sync.Pool{New: func() any { return newFn() }})
	return TID(len(typeCache) - 1)
}

// NewTypeValue create PersistentData type.
// TID is unique identity for type creation
// from persistent storage
func NewTypeValue(tid TID) (PersistentData, error) {
	if tid < 0 || int(tid) >= len(typeCache) {
		return nil, errors.New("TID out of range")
	}

	data := typeCache[tid].Get().(PersistentData)

	// RAII for put back data to pool
	runtime.SetFinalizer(data, typeCache[tid].Put)

	return data, nil
}

func ReturnTypeValue(tid TID, v PersistentData) {
	if tid < 0 || int(tid) >= len(typeCache) {
		return
	}

	typeCache[tid].Put(v)
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
