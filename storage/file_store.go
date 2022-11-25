package storage

import (
	"bytes"
	"encoding/binary"
	originErr "errors"
	"os"

	"github.com/frozenpine/msgqueue/flow"
	"github.com/pkg/errors"
)

var (
	ErrFSAlreadyOpened = originErr.New("file store already opened")
	ErrFSAlreadyClosed = originErr.New("file store already closed")
	ErrUnknownTag      = originErr.New("unkonwn tag type")
)

type FileStorage struct {
	filePath string
	file     *os.File
}

func (f *FileStorage) Open() (err error) {
	if f.file != nil {
		return ErrFSAlreadyOpened
	}

	f.file, err = os.OpenFile(f.filePath, os.O_CREATE|os.O_RDWR, os.ModePerm)

	return
}

func (f *FileStorage) Flush() error {
	if f.file == nil {
		return ErrFSAlreadyClosed
	}

	return f.file.Sync()
}

func (f *FileStorage) Close() (err error) {
	if f.file == nil {
		return ErrFSAlreadyClosed
	}

	if err = f.Flush(); err != nil {
		return
	}

	return f.file.Close()
}

func (f *FileStorage) Write(v *flow.FlowItem) error {
	wr := bytes.NewBuffer(make([]byte, 1+v.Data.Len(), 1+4+v.Data.Len()))

	tag := v.Data.Tag()

	if err := wr.WriteByte(byte(tag)); err != nil {
		return errors.Wrap(err, "append tag failed")
	}

	switch tag {
	case flow.Size8_T:
		fallthrough
	case flow.Size16_T:
		fallthrough
	case flow.Size32_T:
		fallthrough
	case flow.Size64_T:
	case flow.FixedSize_T:
	case flow.VariantSize_T:
		if err := binary.Write(wr, binary.LittleEndian, v.Data.Len()); err != nil {
			return errors.Wrap(err, "append data len failed")
		}
	default:
		return errors.Wrap(ErrUnknownTag, tag.String())
	}

	if _, err := wr.Write(v.Data.Serialize()); err != nil {
		return errors.Wrap(err, "append data failed")
	}

	if _, err := f.file.Write(wr.Bytes()); err != nil {
		return errors.Wrap(err, "write to file failed")
	} else {
		return f.Flush()
	}
}
