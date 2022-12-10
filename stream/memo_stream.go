package stream

import "github.com/frozenpine/msgqueue/pipeline"

type MemoStream[
	IS, IV comparable,
	OS, OV comparable,
	KEY comparable,
] struct {
	pipeline.MemoPipeLine[IS, IV, OS, OV]
}
