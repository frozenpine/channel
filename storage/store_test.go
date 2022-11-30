package storage_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/frozenpine/msgqueue/storage"
)

type Int struct {
	int
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

func (v Varaint) Serialize() []byte {
	result := []byte(v.name)

	result = append(result, v.data.Serialize()...)

	return result
}

func (v *Varaint) Deserialize(data []byte) error {
	len := len(data)

	if err := v.data.Deserialize(data[len-4:]); err != nil {
		return err
	}

	v.name = string(data[0 : len-4])
	return nil
}

func TestFileStore(t *testing.T) {
	flowFile := "flow.dat"

	store := storage.NewFileStore(flowFile)

	if err := store.Open(os.O_WRONLY | os.O_CREATE | os.O_TRUNC); err != nil {
		t.Fatal("store open failed:", err)
	}

	t1 := storage.RegisterType(&Int{}, func() storage.PersistentData {
		return &Int{}
	})
	t2 := storage.RegisterType(&Varaint{}, func() storage.PersistentData {
		return &Varaint{}
	})

	v1 := Int{100}
	v2 := Varaint{
		name: "testtest",
		data: Int{200},
	}

	if err := store.Write(t1, &v1); err != nil {
		t.Fatal(err)
	}

	if err := store.Write(t2, &v2); err != nil {
		t.Fatal(err)
	}

	if err := store.Close(); err != nil {
		t.Fatal("store close failed:", err)
	}

	store = storage.NewFileStore(flowFile)
	if err := store.Open(os.O_RDONLY); err != nil {
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

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRegisterType(t *testing.T) {
	var (
		count   = 5
		tidList = make([]storage.TID, count)
	)

	for i := 0; i < count; i++ {
		tidList[i] = storage.RegisterType(&Varaint{}, func() storage.PersistentData {
			return &Varaint{}
		})
	}

	for i := 1; i < count-1; i++ {
		if tidList[i-1] != tidList[i+1] {
			t.Fatal("tid mismatch", tidList)
		}
	}
}

func TestDecode(t *testing.T) {
	data := []byte{0x00, 0xf8, 0x01, 0x00, 0x18, 0x74, 0x65, 0x73, 0x74, 0x74, 0x65, 0x73, 0x74, 0xf8, 0x00, 0x00, 0x00}

	rd := bytes.NewReader(data)

	if epoch, err := binary.ReadUvarint(rd); err != nil {
		t.Fatal(err)
	} else {
		t.Log(epoch)
	}

	if seq, err := binary.ReadUvarint(rd); err != nil {
		t.Fatal(err)
	} else {
		t.Log(seq)
	}

	if tid, err := binary.ReadVarint(rd); err != nil {
		t.Fatal(err)
	} else {
		t.Log(tid)
	}

	var l int
	if v, err := binary.ReadVarint(rd); err != nil {
		t.Fatal(err)
	} else {
		t.Log(v)
		l = int(v)
	}

	v := make([]byte, l)
	if n, err := rd.Read(v); err != nil || n != l {
		t.Fatal(err, n)
	} else {
		s := string(v[0 : n-4])
		d := binary.LittleEndian.Uint32(v[n-4:])
		t.Log(s, d)
	}
}

func BenchmarkFileStoreWR(b *testing.B) {
	tid := storage.RegisterType(&Varaint{}, func() storage.PersistentData {
		return &Varaint{}
	})

	store := storage.NewFileStore("flow_bench.dat")
	store.Open(os.O_WRONLY | os.O_CREATE | os.O_TRUNC)

	v := Varaint{}

	b.ResetTimer()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		v.name = "testtest"
		v.data.int = i

		if err := store.Write(tid, &v); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFileStoreRD(b *testing.B) {
	storage.RegisterType(&Varaint{}, func() storage.PersistentData {
		return &Varaint{}
	})

	store := storage.NewFileStore("flow_bench.dat")
	store.Open(os.O_RDONLY)

	b.ResetTimer()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		v, err := store.Read()

		if errors.Is(err, io.EOF) {
			b.Log("flow end", b.N)
			break
		}

		if err != nil {
			b.Fatal(err, v)
		}
	}

	store.Close()
}
