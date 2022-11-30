package storage

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"

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

func (f *FileStorage) Write(tid TID, data PersistentData) error {
	if f.wr == nil {
		return errors.Wrap(ErrInvalidMode, "can not write to readonly store")
	}
	if data == nil {
		return ErrEmptyData
	}

	v := data.Serialize()

	if n, err := f.wr.Write(
		binary.AppendVarint(
			make([]byte, 0, 1),
			int64(tid)),
	); err != nil {
		return errors.Wrap(err, "write TID failed")
	} else {
		f.wLen += n
		f.uncommited += n
	}

	if n, err := f.wr.Write(
		binary.AppendVarint(
			make([]byte, 0, 1),
			int64(len(v))),
	); err != nil {
		return errors.Wrap(err, "write data len failed")
	} else {
		f.wLen += n
		f.uncommited += n
	}

	if n, err := f.wr.Write(v); err != nil {
		return errors.Wrap(err, "write data failed")
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

func (f *FileStorage) Read() (v PersistentData, err error) {
	if f.rd == nil {
		return nil, errors.Wrap(ErrInvalidMode, "can not read from write only store")
	}

	var tid int64

	tid, err = binary.ReadVarint(f.rd)

	if err != nil {
		return nil, errors.Wrap(err, "decode TID failed")
	}

	if v, err = NewTypeValue(TID(tid)); err != nil {
		return nil, errors.Wrap(err, "create data failed")
	}

	var len int
	if v, err := binary.ReadVarint(f.rd); err != nil {
		return nil, errors.Wrap(err, "decode data len failed")
	} else {
		len = int(v)
	}

	data := make([]byte, len)
	if _, err := io.ReadFull(f.rd, data); err != nil {
		return nil, errors.Wrap(err, "read data payload failed")
	}

	if err = v.Deserialize(data); err != nil {
		return nil, errors.Wrap(err, "parse data payload failed")
	}

	return
}
