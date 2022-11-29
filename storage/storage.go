package storage

import "github.com/frozenpine/msgqueue/flow"

type Mode uint8

const (
	RDOnly Mode = 1 << iota
	WROnly
	RDWR Mode = RDOnly | WROnly
)

type BaseStorage interface {
	Open(Mode) error
	Close() error
	Flush() error
}

type Storage interface {
	BaseStorage

	Write(*flow.FlowItem) error
	Read() (*flow.FlowItem, error)
}
