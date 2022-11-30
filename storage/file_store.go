package storage

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"runtime"
	"sync"

	"github.com/frozenpine/msgqueue/flow"
	"github.com/pkg/errors"
)

const (
	MaxVarintLen64 = 10

	defaultBufferLen = 4096
	commitSize       = 4096 * 10
)

var (
	ErrInvalidMode     = errors.New("invalid mode action")
	ErrEmptyData       = errors.New("empty flow data")
	ErrFSAlreadyOpened = errors.New("file store already opened")
	ErrFSAlreadyClosed = errors.New("file store already closed")
	ErrUnknownTag      = errors.New("unkonwn tag type")
	ErrSizeMismatch    = errors.New("data size mismatch")
	ErrVintOverflow    = errors.New("varint overflows a 64-bit integer")

	flowBuffer = sync.Pool{New: func() any { return &flow.FlowItem{} }}
)

type FileStorage struct {
	filePath   string
	mode       int
	file       *os.File
	rd         *bufio.Reader
	wr         *bufio.Writer
	wLen       int
	uncommited int
}

func NewFileStore(path string) *FileStorage {
	store := FileStorage{
		filePath: path,
	}

	return &store
}

func (f *FileStorage) Open(mode int) (err error) {
	if f.file != nil {
		return ErrFSAlreadyOpened
	}

	f.file, err = os.OpenFile(f.filePath, mode, os.ModePerm)

	f.mode = mode

	switch f.mode & (os.O_RDONLY | os.O_WRONLY | os.O_RDWR) {
	case os.O_RDONLY:
		f.rd = bufio.NewReaderSize(f.file, defaultBufferLen)
	case os.O_WRONLY:
		f.wr = bufio.NewWriterSize(f.file, defaultBufferLen)
	case os.O_RDWR:
		f.rd = bufio.NewReaderSize(f.file, defaultBufferLen)
		f.wr = bufio.NewWriterSize(f.file, defaultBufferLen)
	default:

	}

	return
}

func (f *FileStorage) Flush() error {
	if f.file == nil {
		return ErrFSAlreadyClosed
	}

	if f.wr != nil {
		return f.wr.Flush()
	}

	return nil
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

func (f *FileStorage) NewFlowItem() (fl *flow.FlowItem) {
	fl = flowBuffer.Get().(*flow.FlowItem)

	runtime.SetFinalizer(fl, flowBuffer.Put)

	return fl
}

func (f *FileStorage) Write(v *flow.FlowItem) error {
	if f.wr == nil {
		return errors.Wrap(ErrInvalidMode, "can not write to readonly store")
	}
	if v == nil {
		return ErrEmptyData
	}

	wr := bytes.NewBuffer(make([]byte, 0, defaultBufferLen))

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
		binary.AppendVarint(make([]byte, 0, 1), int64(v.TID)),
	); err != nil {
		return errors.Wrap(err, "append tid failed")
	} else {
		bufWLen += n
	}

	data := v.Data.Serialize()

	if n, err := wr.Write(
		binary.AppendVarint(make([]byte, 0, 1), int64(len(data))),
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
		f.uncommited += n
		if f.uncommited >= commitSize {
			f.uncommited = 0
			return f.Flush()
		}
		return nil
	}
}

func (f *FileStorage) Read() (*flow.FlowItem, error) {
	if f.rd == nil {
		return nil, errors.Wrap(ErrInvalidMode, "can not read from write only store")
	}

	fl := f.NewFlowItem()

	if epoch, err := binary.ReadUvarint(f.rd); err != nil {
		return nil, errors.Wrap(err, "decode epoch failed")
	} else {
		fl.Epoch = epoch
	}

	if seq, err := binary.ReadUvarint(f.rd); err != nil {
		return nil, errors.Wrap(err, "decode sequence failed")
	} else {
		fl.Sequence = seq
	}

	if tid, err := binary.ReadVarint(f.rd); err != nil {
		return nil, errors.Wrap(err, "decode TID failed")
	} else {
		fl.TID = flow.TID(tid)
	}

	var err error
	if fl.Data, err = flow.NewTypeValue(fl.TID); err != nil {
		return nil, errors.Wrap(err, "create flow data failed")
	}

	var len int
	if v, err := binary.ReadVarint(f.rd); err != nil {
		return nil, errors.Wrap(err, "decode data len failed")
	} else {
		len = int(v)
	}

	data := make([]byte, len)
	if n, err := io.ReadFull(f.rd, data); err != nil || n != len {
		return nil, errors.Wrap(err, "read data payload failed")
	}

	if err := fl.Data.Deserialize(data); err != nil {
		return nil, errors.Wrap(err, "parse data payload failed")
	}

	return fl, nil
}
