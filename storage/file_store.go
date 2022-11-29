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

	if n, err := wr.Write(
		binary.AppendUvarint(make([]byte, 0, 1), v.TID.ZigZag()),
	); err != nil {
		return errors.Wrap(err, "append tag failed")
	} else {
		bufWLen += n
	}

	data := v.Data.Serialize()

	if n, err := wr.Write(
		binary.AppendUvarint(make([]byte, 0, 1), uint64(len(data))),
	); err != nil {
		return errors.Wrap(err, "append data len failed")
	} else {
		bufWLen += n
	}

	if n, err := wr.Write(data); err != nil {
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

	if epoch, err := binary.ReadUvarint(f.rd); err != nil {
		return nil, errors.Wrap(err, "decode epoch failed")
	} else {
		v.Epoch = epoch
	}

	if seq, err := binary.ReadUvarint(f.rd); err != nil {
		return nil, errors.Wrap(err, "decode epoch failed")
	} else {
		v.Sequence = seq
	}

	if tid, err := binary.ReadUvarint(f.rd); err != nil {
		return nil, errors.Wrap(err, "decode tag failed")
	} else {
		v.TID = flow.TID(tid)
	}

	v.Data = flow.NewTypeValue(v.TID)

	var len int
	if d, err := binary.ReadUvarint(f.rd); err != nil {
		return nil, errors.Wrap(err, "decode data len failed")
	} else {
		len = int(d)
	}

	data := make([]byte, len)
	if _, err := f.rd.Read(data); err != nil {
		return nil, errors.Wrap(err, "read data payload failed")
	}

	if err := v.Data.Deserialize(data); err != nil {
		return nil, errors.Wrap(err, "parse data payload failed")
	}

	return &v, nil
}
