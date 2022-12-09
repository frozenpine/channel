package core

import "github.com/pkg/errors"

var (
	ErrNoSubcriber       = errors.New("no subscriber")
	ErrPubTimeout        = errors.New("pub timeout")
	ErrPipeline          = errors.New("pipeline upstream is nil")
	ErrAlreadySubscribed = errors.New("already subscribed")
)
