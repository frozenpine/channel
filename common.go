package msgqueue

import (
	"errors"
	"math/rand"
	"time"
	"unsafe"

	"github.com/gofrs/uuid"
)

func GenID(name string) uuid.UUID {
	if name == "" {
		name = GenName()
	}

	return uuid.NewV5(uuid.NamespaceDNS, name)
}

const (
	letters      = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	randNameSize = 10
	// 6 bits to represent a letter index
	letterIdBits = 6
	// All 1-bits as many as letterIdBits
	letterIdMask = 1<<letterIdBits - 1
	letterIdMax  = 63 / letterIdBits
)

var (
	src = rand.NewSource(time.Now().UnixNano())
)

func GenName() string {
	b := make([]byte, randNameSize)
	// A rand.Int63() generates 63 random bits, enough for letterIdMax letters!
	for i, cache, remain := randNameSize-1, src.Int63(), letterIdMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdMax
		}
		if idx := int(cache & letterIdMask); idx < len(letters) {
			b[i] = letters[idx]
			i--
		}
		cache >>= letterIdBits
		remain--
	}
	return *(*string)(unsafe.Pointer(&b))
}

type Type uint8

type CtxTypeKey string

const CtxQueueType CtxTypeKey = "queue_type"

var ErrInvalidType = errors.New("invalid type")

const (
	Memory Type = 1 << iota
	Persistent
	Remote
)
