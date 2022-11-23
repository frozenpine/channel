package hub

import (
	"encoding/binary"
	"testing"
)

func TestMemoHub(t *testing.T) {

}

type Int int

func (v Int) FixSized() bool { return true }
func (v Int) GetSize() int   { return 4 }
func (v Int) Serialize() (out []byte) {
	binary.LittleEndian.PutUint32(out, uint32(v))
	return
}
func (v *Int) Deserialize(data []byte) error {
	*v = Int(binary.LittleEndian.Uint32(data))
	return nil
}
