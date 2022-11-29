package storage_test

import (
	"encoding/binary"
	"errors"
	"io"
	"testing"

	"github.com/frozenpine/msgqueue/flow"
	"github.com/frozenpine/msgqueue/storage"
)

type Int struct {
	int
}

func (v Int) Tag() flow.TagType {
	return flow.Size32_T
}

func (v Int) Len() int {
	return 4
}

func (v Int) Serialize() []byte {
	result := make([]byte, 4)

	binary.LittleEndian.PutUint32(result, uint32(v.int))

	return result
}

func (v *Int) Deserialize(data []byte) error {
	result := binary.LittleEndian.Uint32(data)
	v.int = int(result)
	return nil
}

type Varaint struct {
	name string
	data Int
}

func (v Varaint) Tag() flow.TagType {
	return flow.VariantSize_T
}

func (v Varaint) Len() int {
	return -1
}

func (v Varaint) Serialize() []byte {
	result := []byte(v.name)

	result = append(result, v.data.Serialize()...)

	return result
}

func (v *Varaint) Deserialize(data []byte) error {
	len := len(data)
	v.data.Deserialize(data[len-4:])
	v.name = string(data[0 : len-4])
	return nil
}

func TestFileStore(t *testing.T) {
	flowFile := "flow.dat"

	store := storage.NewFileStore(flowFile)

	if err := store.Open(storage.WROnly); err != nil {
		t.Fatal("store open failed:", err)
	}

	v1 := Int{100}
	v2 := Varaint{
		name: "testtest",
		data: Int{200},
	}

	item1 := flow.FlowItem{
		Epoch:    0,
		Sequence: 1,
		Tag:      flow.Size32_T,
		Data:     v1.Serialize(),
	}

	item2 := flow.FlowItem{
		Epoch:    0,
		Sequence: 2,
		Tag:      flow.VariantSize_T,
		Data:     v2.Serialize(),
	}

	if err := store.Write(&item1); err != nil {
		t.Fatal(err)
	}

	if err := store.Write(&item2); err != nil {
		t.Fatal(err)
	}

	if err := store.Close(); err != nil {
		t.Fatal("store close failed:", err)
	}

	store = storage.NewFileStore(flowFile)
	if err := store.Open(storage.RDOnly); err != nil {
		t.Fatal("store open failed:", err)
	}

	if rd1, err := store.Read(); err != nil {
		t.Fatal(err)
	} else {
		t.Log(rd1)
	}

	if rd2, err := store.Read(); err != nil {
		t.Fatal(err)
	} else {
		t.Log(rd2)
	}

	if _, err := store.Read(); errors.Is(err, io.EOF) {
		t.Log("end of flow file")
	} else {
		t.Fatal(err)
	}

	store.Close()
}
