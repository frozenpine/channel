package storage

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"

	"github.com/frozenpine/msgqueue/flow"
	"github.com/pkg/errors"
)

const (
	MaxVarintLen64 = 10
)

var (
	ErrEmptyData       = errors.New("empty flow data")
	ErrFSAlreadyOpened = errors.New("file store already opened")
	ErrFSAlreadyClosed = errors.New("file store already closed")
	ErrUnknownTag      = errors.New("unkonwn tag type")
	ErrOverflow        = errors.New("binary: varint overflows a 64-bit integer")
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
	if v == nil {
		return ErrEmptyData
	}

	// min len: epoch(8 bytes)+sequence(8 bytes)+tag(1 byte)+data len
	// max len: epoch(8 bytes)+sequence(8 bytes)+tag(1 byte)+length(4 bytes)+data len
	wr := bytes.NewBuffer(make([]byte, 17+v.Data.Len(), 17+4+v.Data.Len()))

	if err := binary.Write(
		wr, binary.LittleEndian,
		binary.AppendUvarint(make([]byte, 1), v.Epoch),
	); err != nil {
		return errors.Wrap(err, "append epoch failed")
	}

	if err := binary.Write(
		f.file, binary.LittleEndian,
		binary.AppendUvarint(make([]byte, 1), v.Sequence),
	); err != nil {
		return errors.Wrap(err, "append sequence failed")
	}

	tag := v.Data.Tag()

	if err := wr.WriteByte(byte(tag)); err != nil {
		return errors.Wrap(err, "append tag failed")
	}

	switch tag {
	case flow.Size8_T:
	case flow.Size16_T:
	case flow.Size32_T:
	case flow.Size64_T:
		// break to bypass write data length for up case
	case flow.FixedSize_T:
		fallthrough
	case flow.VariantSize_T:
		if err := binary.Write(
			wr, binary.LittleEndian,
			binary.AppendUvarint(make([]byte, 1), uint64(v.Data.Len())),
		); err != nil {
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

func (f *FileStorage) decodeUvariant(rd io.Reader) (uint64, error) {
	var x uint64
	var s uint
	var data = make([]byte, 1)

	for i := 0; i < MaxVarintLen64; i++ {
		_, err := rd.Read(data)
		if err != nil {
			return x, err
		}
		if data[0] < 0x80 {
			if i == MaxVarintLen64-1 && data[0] > 1 {
				return x, ErrOverflow
			}
			return x | uint64(data[0])<<s, nil
		}
		x |= uint64(data[0]&0x7f) << s
		s += 7
	}

	return x, ErrOverflow
}

func (f *FileStorage) Read(v *flow.FlowItem) error {
	if v == nil {
		return ErrEmptyData
	}

	if d, err := f.decodeUvariant(f.file); err != nil {
		return errors.Wrap(err, "decode epoch failed")
	} else {
		v.Epoch = d
	}

	if d, err := f.decodeUvariant(f.file); err != nil {
		return errors.Wrap(err, "decode epoch failed")
	} else {
		v.Sequence = d
	}

	var tag flow.TagType

	if err := binary.Read(f.file, binary.LittleEndian, &tag); err != nil {
		return errors.Wrap(err, "decode tag failed")
	}

	var len int

	switch tag {
	case flow.Size8_T:
		len = 1
	case flow.Size16_T:
		len = 2
	case flow.Size32_T:
		len = 3
	case flow.Size64_T:
		len = 4
	case flow.FixedSize_T:
		fallthrough
	case flow.VariantSize_T:
		if d, err := f.decodeUvariant(f.file); err != nil {
			return errors.Wrap(err, "decode data len failed")
		} else {
			len = int(d)
		}
	default:
		return errors.Wrap(ErrUnknownTag, tag.String())
	}

	data := make([]byte, len)
	if _, err := f.file.Read(data); err != nil {
		return errors.Wrap(err, "read data payload failed")
	}

	if err := v.Data.Deserialize(data); err != nil {
		return errors.Wrap(err, "decode data failed")
	}

	return nil
}
