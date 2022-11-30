package flow

import (
	"os"

	"github.com/frozenpine/msgqueue/core"
	"github.com/frozenpine/msgqueue/storage"
)

type FileFlow[T storage.PersistentData] struct {
	flowDIR os.DirEntry

	flowCache *core.RBTree

	flowEpoch uint64
	flowSeq   uint64
}

func NewFileFlow[T storage.PersistentData](dir string) *FileFlow[T] {
	return &FileFlow[T]{}
}
