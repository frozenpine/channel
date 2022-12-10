package pipeline

import (
	"context"

	"github.com/frozenpine/msgqueue/core"
)

type WaterMark interface {
	IsWaterMark() bool
}

type Sequence[S, V any] interface {
	Value() V
	Index() S

	Compare(than Sequence[S, V]) int

	WaterMark
}

type BasePipe interface {
	core.QueueBase

	init(ctx context.Context, name string, extraInit func())
}

type Converter[
	IS, IV comparable,
	OS, OV comparable,
] interface {
	Convert(Sequence[IS, IV]) Sequence[OS, OV]
}

type Pipeline[
	IS, IV comparable,
	OS, OV comparable,
] interface {
	BasePipe
	core.Producer[Sequence[IS, IV]]
	core.Consumer[Sequence[OS, OV]]
	core.Upstream[Sequence[IS, IV]]
	core.Downstream[Sequence[OS, OV]]
}

func NewPipeline[
	IS, IV comparable,
	OS, OV comparable,
](ctx context.Context, name string) (Pipeline[IS, IV, OS, OV], error) {
	var typ core.Type = core.Memory

	if ctx == nil {
		ctx = context.Background()
	} else if v := ctx.Value(core.CtxQueueType); v != nil {
		if t, ok := v.(core.Type); ok {
			typ = t
		}
	}

	switch typ {
	case core.Memory:
		return NewMemoPipeLine[IS, IV, OS, OV](ctx, name, nil), nil
	}

	return nil, core.ErrInvalidType
}
