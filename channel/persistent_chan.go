package channel

import (
	"context"

	"github.com/frozenpine/msgqueue/storage"
)

type PersistentChan[T storage.PersistentData] struct {
	MemoChannel[T]

	storage storage.Storage[T]
}

func NewPersistentChan[T storage.PersistentData](ctx context.Context, name string, bufSize int) *PersistentChan[T] {
	ch := PersistentChan[T]{}

	ch.init(ctx, name, bufSize, nil)

	return &ch
}

func (ch *PersistentChan[T]) init(ctx context.Context, name string, bufSize int, extraInit func()) {
	ch.MemoChannel.init(ctx, name, bufSize, func() {
		if extraInit != nil {
			extraInit()
		}

		// TODO: self extra init
	})
}
