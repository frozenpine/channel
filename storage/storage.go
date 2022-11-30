package storage

import "github.com/frozenpine/msgqueue/flow"

type BaseStorage interface {
	Open(int) error
	Close() error
	Flush() error
}

type Storage interface {
	BaseStorage

	Write(*flow.FlowItem) error
	Read() (*flow.FlowItem, error)
}
