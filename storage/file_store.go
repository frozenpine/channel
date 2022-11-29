package storage

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"os"
	"sync"

	"github.com/frozenpine/msgqueue/flow"
	"github.com/pkg/errors"
)

const (
	MaxVarintLen64 = 10

	defaultBufferLen = 4096
)

var (
	ErrEmptyData       = errors.New("empty flow data")
	ErrFSAlreadyOpened = errors.New("file store already opened")
	ErrFSAlreadyClosed = errors.New("file store already closed")
	ErrUnknownTag      = errors.New("unkonwn tag type")
	ErrSizeMismatch    = errors.New("data size mismatch")
	ErrOverflow        = errors.New("varint overflows a 64-bit integer")

	writeBuffer = sync.Pool{New: func() any { return bytes.NewBuffer(make([]byte, 0, defaultBufferLen)) }}
)

type FileStorage struct {
	filePath string
	file     *os.File
	rd       *bufio.Reader
	wLen     int
}

func NewFileStore(path string) *FileStorage {
	store := FileStorage{
		filePath: path,
	}

	return &store
}

func (f *FileStorage) Open(mode Mode) (err error) {
	if f.file != nil {
		return ErrFSAlreadyOpened
	}

	var opMode int

	switch mode {
	case RDOnly:
		opMode = os.O_RDONLY
	case WROnly:
		opMode = os.O_CREATE | os.O_WRONLY | os.O_TRUNC
	case RDWR:
		opMode = os.O_CREATE | os.O_RDWR
	}

	f.file, err = os.OpenFile(f.filePath, opMode, os.ModePerm)

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

func (f *FileStorage) getBuffer() *bytes.Buffer {
	return writeBuffer.Get().(*bytes.Buffer)
}

func (f *FileStorage) returnBuffer(buf *bytes.Buffer) {
	buf.Reset()
	writeBuffer.Put(buf)
}

func (f *FileStorage) Write(v *flow.FlowItem) error {
	if v == nil {
		return ErrEmptyData
	}

	wr := f.getBuffer()
	defer f.returnBuffer(wr)

	bufWLen := 0

	if n, err := wr.Write(
		binary.AppendUvarint(make([]byte, 0, 1), v.Epoch),
	); err != nil {
		return errors.Wrap(err, "append epoch failed")
	} else {
		bufWLen += n
	}

	if n, err := wr.Write(
		binary.AppendUvarint(make([]byte, 0, 1), v.Sequence),
	); err != nil {
		return errors.Wrap(err, "append sequence failed")
	} else {
		bufWLen += n
	}

	if err := wr.WriteByte(byte(v.Tag)); err != nil {
		return errors.Wrap(err, "append tag failed")
	} else {
		bufWLen++
	}

	switch v.Tag {
	case flow.Size8_T:
		fallthrough
	case flow.Size16_T:
		fallthrough
	case flow.Size32_T:
		fallthrough
	case flow.Size64_T:
		// skip append data length for up case
	case flow.FixedSize_T:
		fallthrough
	case flow.VariantSize_T:
		if n, err := wr.Write(
			binary.AppendUvarint(make([]byte, 0, 1), uint64(len(v.Data))),
		); err != nil {
			return errors.Wrap(err, "append data len failed")
		} else {
			bufWLen += n
		}
	default:
		return errors.Wrap(ErrUnknownTag, v.Tag.String())
	}

	if n, err := wr.Write(v.Data); err != nil {
		return errors.Wrap(err, "append data failed")
	} else {
		bufWLen += n
	}

	if n, err := f.file.Write(wr.Bytes()); err != nil {
		return errors.Wrap(err, "write to file failed")
	} else if n != bufWLen {
		return errors.New("buffer write size mismatch with file write")
	} else {
		f.wLen += n
		return f.Flush()
	}
}

func (f *FileStorage) Read() (*flow.FlowItem, error) {
	if f.rd == nil {
		f.rd = bufio.NewReader(f.file)
	}

	v := flow.FlowItem{}

	if d, err := binary.ReadUvarint(f.rd); err != nil {
		return nil, errors.Wrap(err, "decode epoch failed")
	} else {
		v.Epoch = d
	}

	if d, err := binary.ReadUvarint(f.rd); err != nil {
		return nil, errors.Wrap(err, "decode epoch failed")
	} else {
		v.Sequence = d
	}

	if tag, err := f.rd.ReadByte(); err != nil {
		return nil, errors.Wrap(err, "decode tag failed")
	} else {
		v.Tag = flow.TagType(tag)
	}

	var len int

	switch v.Tag {
	case flow.Size8_T:
		len = 1
	case flow.Size16_T:
		len = 2
	case flow.Size32_T:
		len = 4
	case flow.Size64_T:
		len = 8
	case flow.FixedSize_T:
		fallthrough
	case flow.VariantSize_T:
		if d, err := binary.ReadUvarint(f.rd); err != nil {
			return nil, errors.Wrap(err, "decode data len failed")
		} else {
			len = int(d)
		}
	default:
		return nil, errors.Wrap(ErrUnknownTag, v.Tag.String())
	}

	v.Data = make([]byte, len)
	if _, err := f.rd.Read(v.Data); err != nil {
		return nil, errors.Wrap(err, "read data payload failed")
	}

	return &v, nil
}
