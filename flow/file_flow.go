package flow

import "os"

type FileFlow[T PersistentData] struct {
	flowDIR os.DirEntry
}
