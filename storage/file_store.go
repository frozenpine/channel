package storage

import (
	"bufio"
	"io"
	"os"

	"github.com/frozenpine/msgqueue/core"
	"github.com/pkg/errors"
)

const (
	MaxVarintLen64 = 10

	defaultBufferLen = 4096
	commitSize       = 4096 * 10
	batchSize        = 100
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
	filePath     string
	mode         int
	file         *os.File
	rd           *bufio.Reader
	wr           *bufio.Writer
	wrSize       int
	uncommitSize int
	batchSize    int
}

func NewFileStore(path string) *FileStorage {
	store := FileStorage{
		filePath: path,
	}

	return &store
}

func (stor *FileStorage) Open(mode int) (err error) {
	if stor.file != nil {
		return ErrFSAlreadyOpened
	}

	stor.file, err = os.OpenFile(stor.filePath, mode, os.ModePerm)

	stor.mode = mode

	switch stor.mode & (os.O_RDONLY | os.O_WRONLY | os.O_RDWR) {
	case os.O_RDONLY:
		stor.rd = bufio.NewReaderSize(stor.file, defaultBufferLen)
	case os.O_WRONLY:
		stor.wr = bufio.NewWriterSize(stor.file, defaultBufferLen)
	case os.O_RDWR:
		stor.rd = bufio.NewReaderSize(stor.file, defaultBufferLen)
		stor.wr = bufio.NewWriterSize(stor.file, defaultBufferLen)
	default:

	}

	return
}

func (stor *FileStorage) Flush() error {
	if stor.file == nil {
		return ErrFSAlreadyClosed
	}

	if stor.wr != nil {
		return stor.file.Sync()
	}

	return nil
}

func (stor *FileStorage) Close() (err error) {
	if stor.file == nil {
		return ErrFSAlreadyClosed
	}

	if err = stor.Flush(); err != nil {
		return
	}

	return stor.file.Close()
}

func (stor *FileStorage) Write(tid TID, data PersistentData) error {
	if stor.wr == nil {
		return errors.Wrap(ErrInvalidMode, "can not write to readonly store")
	}
	if data == nil {
		return ErrEmptyData
	}

	v := data.Serialize()

	if n, err := core.SerializeVint(tid, stor.wr); err != nil {
		return errors.Wrap(err, "write TID failed")
	} else {
		stor.wrSize += n
		stor.uncommitSize += n
	}

	if n, err := core.SerializeVint(len(v), stor.wr); err != nil {
		return errors.Wrap(err, "write data len failed")
	} else {
		stor.wrSize += n
		stor.uncommitSize += n
	}

	if n, err := stor.wr.Write(v); err != nil {
		return errors.Wrap(err, "write data failed")
	} else {
		stor.wrSize += n
		stor.uncommitSize += n
		stor.batchSize++

		if stor.uncommitSize >= commitSize || stor.batchSize >= batchSize {
			stor.uncommitSize = 0
			stor.batchSize = 0
			return stor.Flush()
		}

		return stor.wr.Flush()
	}
}

func (stor *FileStorage) Read() (v PersistentData, err error) {
	if stor.rd == nil {
		return nil, errors.Wrap(ErrInvalidMode, "can not read from write only store")
	}

	var (
		tid     TID
		dataLen int
	)

	if tid, err = core.DeserializeVint[TID](stor.rd); err != nil {
		return nil, errors.Wrap(err, "decode TID failed")
	}

	if v, err = NewTypeValue(tid); err != nil {
		return nil, errors.Wrap(err, "create data failed")
	}

	if dataLen, err = core.DeserializeVint[int](stor.rd); err != nil {
		return nil, errors.Wrap(err, "decode data len failed")
	}

	data := make([]byte, dataLen)
	if _, err := io.ReadFull(stor.rd, data); err != nil {
		return nil, errors.Wrap(err, "read data payload failed")
	}

	if err = v.Deserialize(data); err != nil {
		return nil, errors.Wrap(err, "parse data payload failed")
	}

	return
}
