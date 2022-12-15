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

type Converter[
	IS, IV any,
	OS, OV any,
] func(Sequence[IS, IV], core.Producer[Sequence[OS, OV]]) error

type Pipeline[
	IS, IV any,
	OS, OV any,
] interface {
	core.QueueBase
	core.Producer[Sequence[IS, IV]]
	core.Consumer[Sequence[OS, OV]]
	core.Upstream[Sequence[IS, IV]]
	core.Downstream[Sequence[OS, OV]]
}

func NewPipeline[
	IS, IV any,
	OS, OV any,
](ctx context.Context, name string, cvt Converter[IS, IV, OS, OV]) (Pipeline[IS, IV, OS, OV], error) {
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
		return NewMemoPipeLine(ctx, name, cvt), nil
	}

	return nil, core.ErrInvalidType
}
