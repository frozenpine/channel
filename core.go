package channel

import (
	"context"

	"github.com/frozenpine/channel/hub"
)

// NewHub create channel hub.
//
// chanSize: pub & sub chan buffer size, if chanSize < 0, use hub default size
func NewHub[T any](ctx context.Context, name string, chanSize int) hub.Hub[T] {
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

	switch typ {
	case hub.MemoHubType:
		result = hub.NewMemoHub[T](ctx, "", chanSize)
	}

	return result
}
