package core_test

import (
	"testing"

	"github.com/frozenpine/msgqueue/core"
	"github.com/frozenpine/msgqueue/flow"
)

func TestTreeItem(t *testing.T) {
	var v core.Item = &flow.FlowItem{}
	t.Log(v)
}
