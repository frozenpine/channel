package msgqueue

import (
	"encoding/binary"
	"errors"
	"io"
	"math/rand"
	"sync"
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

var vintBuffer = sync.Pool{New: func() any { return make([]byte, 0, 10) }}

type Int interface {
	~int8 | ~int16 | ~int | ~int32 | ~int64
}

type Uint interface {
	~uint8 | ~uint16 | ~uint | ~uint32 | ~uint64
}

func SerializeVint[T Int](v T, wr io.Writer) (int, error) {
	buf := GetVintBuffer()

	result := binary.AppendVarint(buf, int64(v))
	ReturnVintBuffer(buf)

	return wr.Write(result)
}

func DeserializeVint[T Int](rd io.ByteReader) (T, error) {
	v, err := binary.ReadVarint(rd)
	return T(v), err
}

func SerializeUVint[T Uint](v T, wr io.Writer) (int, error) {
	buf := GetVintBuffer()

	result := binary.AppendUvarint(buf, uint64(v))
	ReturnVintBuffer(buf)

	return wr.Write(result)
}

func DeserializeUVint[T Uint](rd io.ByteReader) (T, error) {
	v, err := binary.ReadVarint(rd)
	return T(v), err
}

func GetVintBuffer() []byte {
	return vintBuffer.Get().([]byte)
}

func ReturnVintBuffer(buf []byte) {
	vintBuffer.Put(buf[:0])
}
