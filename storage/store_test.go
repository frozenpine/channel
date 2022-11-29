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

func (v Int) Serialize() []byte {
	result := make([]byte, 0, 4)

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

	t1 := flow.RegisterType(func() flow.PersistentData {
		return &Int{}
	})
	t2 := flow.RegisterType(func() flow.PersistentData {
		return &Varaint{}
	})

	v1 := Int{100}
	v2 := Varaint{
		name: "testtest",
		data: Int{200},
	}

	item1 := flow.FlowItem{
		Epoch:    0,
		Sequence: 1,
		TID:      t1,
		Data:     &v1,
	}

	item2 := flow.FlowItem{
		Epoch:    0,
		Sequence: 2,
		TID:      t2,
		Data:     &v2,
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
		t.Log(rd1, *rd1.Data.(*Int))
	}

	if rd2, err := store.Read(); err != nil {
		t.Fatal(err)
	} else {
		t.Log(rd2, *rd2.Data.(*Varaint))
	}

	if _, err := store.Read(); errors.Is(err, io.EOF) {
		t.Log("end of flow file")
	} else {
		t.Fatal(err)
	}

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkFileStoreWR(b *testing.B) {
	tid := flow.RegisterType(func() flow.PersistentData {
		return &Varaint{}
	})

	flowFile := "flow_bench.dat"

	store := storage.NewFileStore(flowFile)
	store.Open(storage.WROnly)

	for i := 0; i < b.N; i++ {
		v := Varaint{
			name: "testtest",
			data: Int{i},
		}

		if err := store.Write(&flow.FlowItem{
			Epoch:    0,
			Sequence: uint64(i),
			TID:      tid,
			Data:     &v,
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFileStoreRD(b *testing.B) {
	flow.RegisterType(func() flow.PersistentData {
		return &Varaint{}
	})

	flowFile := "flow_bench.dat"

	store := storage.NewFileStore(flowFile)
	store.Open(storage.RDOnly)

	for {
		v, err := store.Read()

		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			b.Fatal(err)
		}

		b.Log(v)
	}

}
