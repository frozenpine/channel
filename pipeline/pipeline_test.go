package pipeline

import (
	"testing"

	"github.com/pkg/errors"
)

func TestErrCheck(t *testing.T) {
	err := errors.Wrap(ErrFutureTick, "test")

	if !errors.Is(err, ErrFutureTick) {
		t.Fatal(err)
	}
}
