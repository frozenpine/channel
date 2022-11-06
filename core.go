package channel

import (
	"context"

	"github.com/frozenpine/channel/hub"
)

const defaultHubLen = 10

// NewHub create channel hub.
//
// bufSize: pub & sub chan buffer size, if bufSize < 0, use default size 10
func NewHub[T any](ctx context.Context, bufSize int) hub.Hub[T] {
	var (
		result  hub.Hub[T]
		typ     hub.HubType
		success bool
	)

	if ctx == nil {
		ctx = context.Background()
		typ = hub.MemoHubType
	} else {
		typ, success = ctx.Value(hub.HubTypeKey).(hub.HubType)
		if !success {
			typ = hub.MemoHubType
		}
	}

	if bufSize < 0 {
		bufSize = defaultHubLen
	}

	switch typ {
	case hub.MemoHubType:
		result = hub.NewMemoHub[T](ctx, bufSize)
	}

	return result
}
