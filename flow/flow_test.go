package flow_test

import (
	"encoding/binary"
	"testing"

	"github.com/frozenpine/msgqueue/flow"
)

type Int int

func (v Int) Tag() flow.TagType {
	return flow.Size32_T
}

func (v Int) Len() int {
	return 4
}

func (v Int) Serialize() []byte {
	result := make([]byte, 4)

	binary.LittleEndian.PutUint32(result, uint32(v))

	return result
}

func (v *Int) Deserialize(data []byte) error {
	result := binary.LittleEndian.Uint32(data)
	*v = Int(result)
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

func TestType(t *testing.T) {
	var v Int = 255

	t.Log(v.Tag(), v.Len())

	data := v.Serialize()

	t.Log(data)

	var n flow.PersistentData = new(Int)

	if err := n.Deserialize(data); err != nil {
		t.Fatal(err)
	}

	if *(n.(*Int)) != v {
		t.Fatal("not equal")
	}

	t1 := Varaint{
		name: "testtest",
		data: 32687,
	}

	t1_data := t1.Serialize()

	t.Log(t1.Tag(), t1_data)

	var t2 Varaint

	t2.Deserialize(t1_data)

	t.Logf("%+v", t2)
}
