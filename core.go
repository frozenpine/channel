package msgqueue

import (
	"context"
	"math/rand"
	"time"
	"unsafe"

	"github.com/frozenpine/msgqueue/hub"
	"github.com/gofrs/uuid"
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
