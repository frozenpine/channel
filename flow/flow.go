package flow

import (
	"fmt"
	"log"
	"reflect"
	"runtime"
	"sync"

	"github.com/frozenpine/msgqueue/core"
)

type PersistentData interface {
	Serialize() []byte
	Deserialize([]byte) error
}

var (
	typeList  []sync.Pool
	typeCache map[reflect.Type]TID
)

type TID int

// RegisterType register PersistentData type.
// TID present unique identity for PersistentData
// and type register action must always in same order
func RegisterType(t PersistentData, newFn func() PersistentData) (tid TID) {
	if typeCache == nil {
		typeCache = make(map[reflect.Type]TID)
	}

	typ := reflect.Indirect(reflect.ValueOf(t)).Type()

	var exist bool

	if tid, exist = typeCache[typ]; !exist {
		typeList = append(typeList, sync.Pool{New: func() any { return newFn() }})
		tid = TID(len(typeList) - 1)
		typeCache[typ] = tid
		log.Printf("Type[%s] registered with TID[%d]", typ, tid)
	}

	return
}

// NewTypeValue create PersistentData type.
// TID is unique identity for type creation
// from persistent storage
func NewTypeValue(tid TID) (PersistentData, error) {
	if tid < 0 || int(tid) >= len(typeList) {
		return nil, fmt.Errorf("TID[%d] out of range", tid)
	}

	data := typeList[tid].Get().(PersistentData)

	// RAII for put back data to pool
	runtime.SetFinalizer(data, typeList[tid].Put)

	return data, nil
}

func ReturnTypeValue(tid TID, v PersistentData) {
	if tid < 0 || int(tid) >= len(typeList) {
		return
	}

	typeList[tid].Put(v)
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
