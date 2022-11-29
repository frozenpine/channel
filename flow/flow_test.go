package flow_test

import (
	"testing"
)

func TestValue(t *testing.T) {
	var v1 int = -1

	var v2 uint64 = uint64(v1)

	t.Log(v1, v2, int(v2))
}
