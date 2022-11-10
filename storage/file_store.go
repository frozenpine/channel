package storage

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
)

const (
	maxFlowDataSize = 4 * 1024 * 1024 * 1024
)

var (
	ErrInsufficientData = errors.New("insufficient data in flow file")
)

type FlowFileMeta struct {
	FixedSize     uint32
	StartSequence uint32
	EndSequence   uint32
	TotalDataSize uint32
}

func (meta *FlowFileMeta) ResetOffset(f *os.File) (err error) {
	if _, err = f.Seek(0, 0); err != nil {
		return
	}

	return
}

func (meta *FlowFileMeta) ReadFromFile(f *os.File) (err error) {
	if err = meta.ResetOffset(f); err != nil {
		return
	}

	if err = binary.Read(f, binary.LittleEndian, &meta.FixedSize); err != nil {
		return
	}

	if err = binary.Read(f, binary.LittleEndian, &meta.StartSequence); err != nil {
		return
	}

	if err = binary.Read(f, binary.LittleEndian, &meta.EndSequence); err != nil {
		return
	}

	if err = binary.Read(f, binary.LittleEndian, &meta.TotalDataSize); err != nil {
		return
	}

	return
}

func (meta *FlowFileMeta) WriteToFile(f *os.File) (err error) {
	if err = meta.ResetOffset(f); err != nil {
		return
	}

	if err = binary.Write(f, binary.LittleEndian, meta.FixedSize); err != nil {
		return
	}

	if err = binary.Write(f, binary.LittleEndian, meta.StartSequence); err != nil {
		return
	}

	if err = binary.Write(f, binary.LittleEndian, meta.EndSequence); err != nil {
		return
	}

	if err = binary.Write(f, binary.LittleEndian, meta.TotalDataSize); err != nil {
		return
	}

	return
}

type FlowFile[T PersistentData] struct {
	head        *FlowFile[T]
	left, right *FlowFile[T]

	fileName string
	handler  *os.File
	Meta     *FlowFileMeta
	data     []T
}

func (f *FlowFile[T]) IsHead() bool {
	return f.left == nil
}

func (f *FlowFile[T]) IsTail() bool {
	return f.right == nil
}

func (f *FlowFile[T]) GetHead() *FlowFile[T] {
	return f.head
}

func (f *FlowFile[T]) ReadData() ([]T, error) {
	dataCount := int(f.Meta.EndSequence - f.Meta.StartSequence + 1)

	if cacheLen := len(f.data); cacheLen != dataCount {
		f.data = make([]T, dataCount)
	} else {
		return f.data, nil
	}

	var err error

	if f.handler == nil {
		if f.right != nil {
			f.handler, err = os.OpenFile(f.fileName, os.O_RDONLY, os.ModePerm)
		} else {
			f.handler, err = os.OpenFile(f.fileName, os.O_RDWR|os.O_CREATE, os.ModePerm)
		}

		if err != nil {
			return nil, err
		}
	}

	if err := f.Meta.ReadFromFile(f.handler); err != nil {
		return nil, err
	}

	// empty flow
	if f.Meta.TotalDataSize == 0 {
		if f.left != nil {
			f.Meta.FixedSize = f.left.Meta.FixedSize
			f.Meta.StartSequence = f.left.Meta.EndSequence + 1
		}

		return []T{}, nil
	} else {
		for seq := f.Meta.StartSequence; seq <= f.Meta.EndSequence; seq++ {
			size := f.Meta.FixedSize
			if size <= 0 {
				binary.Read(f.handler, binary.LittleEndian, &size)
			}
			data := make([]byte, size)

			n, err := f.handler.Read(data)

			if err != nil {
				return nil, err
			}

			if n != int(size) {
				return nil, ErrInsufficientData
			}

			if err := f.data[int(seq-f.Meta.StartSequence)].Deserialize(data); err != nil {
				return nil, err
			}
		}
	}

	return f.data, err
}

func (f *FlowFile[T]) AppendData(data T) (int, error) {
	dataSize := data.GetSize()

	if int(f.Meta.TotalDataSize)+dataSize > maxFlowDataSize {
		if err := f.Meta.WriteToFile(f.handler); err != nil {
			return 0, err
		}

		return 0, io.EOF
	}

	if !data.FixSized() {
		if err := binary.Write(f.handler, binary.LittleEndian, uint32(dataSize)); err != nil {
			return 0, err
		}
	} else if f.Meta.FixedSize <= 0 {
		f.Meta.FixedSize = uint32(dataSize)
	}

	if n, err := f.handler.Write(data.Serialize()); err != nil {
		return 0, err
	} else {
		f.Meta.TotalDataSize += uint32(n)
		f.Meta.EndSequence++

		return n, nil
	}
}

func (f *FlowFile[T]) GetData(offset uint32) PersistentData {
	if offset < f.Meta.StartSequence || offset > f.Meta.EndSequence {
		return nil
	}

	return f.data[offset-f.Meta.StartSequence]
}

type FileStore[T PersistentData] struct {
	topic   string
	flowDir string
}

func NewFileStore[T PersistentData](flowDir, topic string) *FileStore[T] {
	store := FileStore[T]{
		topic:   topic,
		flowDir: flowDir,
	}

	return &store
}

func (stor *FileStore[T]) Sink(data T) error {
	// dataSize := data.GetSize()

	return nil
}
