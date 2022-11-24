package flow

type PersistentData interface {
	Tag() string
	Size() int
	Serialize() []byte
	Deserialize([]byte) error
}

type BaseFlow interface {
	FixedSize() uint32
	StartSequence() uint64
	EndSequence() uint64
	TotalDataSize() uint64
}

type Flow[T PersistentData] interface {
	BaseFlow

	Write(data T) (seq uint64, err error)
	ReadAt(seq uint64) (T, error)
	ReadFrom(seq uint64) (<-chan T, error)
	ReadAll() (<-chan T, error)
}
