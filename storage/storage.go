package storage

type PersistentData interface {
	FixSized() bool
	GetSize() int
	Serialize() []byte
	Deserialize([]byte) error
}

type Storage[T PersistentData] interface {
	Sink(data T) error
	UnSink(offset int64) <-chan T
}
