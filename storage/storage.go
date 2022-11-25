package storage

import "github.com/frozenpine/msgqueue/flow"

var (
	tagTypeCache = make(map[flow.TagType]flow.PersistentData)
)

type BaseStorage interface {
	Open() error
	Close() error
	Flush() error
}

type Storage interface {
	BaseStorage

	Write(*flow.FlowItem) error
	Read(*flow.FlowItem) error
	ReadAll() (<-chan *flow.FlowItem, error)
}
