package pipeline

import (
	"context"

	"github.com/frozenpine/msgqueue/core"
)

type Converter[
	IV, OV any,
] func(IV, core.Producer[OV]) error

type Pipeline[
	IV, OV any,
] interface {
	core.QueueBase
	core.Producer[IV]
	core.Consumer[OV]
	core.Upstream[IV]
	core.Downstream[OV]
}

func NewPipeline[
	IS, IV any,
	OS, OV any,
](ctx context.Context, name string, cvt Converter[IV, OV]) (Pipeline[IV, OV], error) {
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
