package flow

import (
	"os"

	"github.com/frozenpine/msgqueue/core"
)

type FileFlow[T PersistentData] struct {
	flowDIR os.DirEntry

	flowCache *core.RBTree
}
