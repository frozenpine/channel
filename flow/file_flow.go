package flow

import (
	"os"

	"github.com/frozenpine/msgqueue/chanio"
	"github.com/frozenpine/msgqueue/core"
)

type FileFlow[T chanio.PersistentData] struct {
	flowDIR os.DirEntry

	flowCache *core.RBTree

	flowEpoch uint64
	flowSeq   uint64
}

func NewFileFlow[T chanio.PersistentData](dir string) *FileFlow[T] {
	return &FileFlow[T]{}
}
