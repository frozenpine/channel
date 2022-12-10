package stream

import "github.com/frozenpine/msgqueue/pipeline"

type MemoStream[
	IS, IV comparable,
	OS, OV comparable,
	KEY comparable,
] struct {
	pipeline.MemoPipeLine[IS, IV, OS, OV]

	aggregator Aggregatorable[IS, IV, OS, OV, KEY]
}

func (strm *MemoStream[IS, IV, OS, OV, KEY]) WindowBy(fn func() <-chan pipeline.WaterMark) Aggregatorable[IS, IV, OS, OV, KEY] {
	strm.aggregator = strm.aggregator.WindowBy(fn)

	return strm
}

func (strm *MemoStream[IS, IV, OS, OV, KEY]) FilterBy(fn func(pipeline.Sequence[IS, IV]) bool) Aggregatorable[IS, IV, OS, OV, KEY] {
	strm.aggregator = strm.aggregator.FilterBy(fn)

	return strm
}

func (strm *MemoStream[IS, IV, OS, OV, KEY]) GroupBy(fn func(pipeline.Sequence[IS, IV]) KEY) Aggregatorable[IS, IV, OS, OV, KEY] {
	strm.aggregator = strm.aggregator.GroupBy(fn)

	return strm
}

func (strm *MemoStream[IS, IV, OS, OV, KEY]) Action(fn func(Window[IS, IV]) pipeline.Sequence[OS, OV]) {
	strm.aggregator.Action(fn)
}

func (strm *MemoStream[IS, IV, OS, OV, KEY]) Groups() map[KEY]Aggregatorable[IS, IV, OS, OV, KEY] {
	return strm.aggregator.Groups()
}
