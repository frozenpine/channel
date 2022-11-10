package storage

import (
	"os"
	"testing"
)

func TestFileStore(t *testing.T) {
	fileName := "test.flow"
	handler, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, os.ModePerm)

	if err != nil {
		t.Fatal(err)
	}

	meta := FlowFileMeta{
		FixedSize:     3,
		StartSequence: 1000,
		EndSequence:   2345,
	}

	if err = meta.WriteToFile(handler); err != nil {
		t.Fatal(err)
	}
	handler.Close()

	rd, err := os.OpenFile(fileName, os.O_RDONLY, os.ModePerm)
	if err != nil {
		t.Fatal(err)
	}

	rMeta := FlowFileMeta{}

	if err = rMeta.ReadFromFile(rd); err != nil {
		t.Fatal(err)
	}

	if meta != rMeta {
		t.Fatal("mismatch")
	} else {
		t.Logf("%+v, %+v", meta, rMeta)
	}
}

func TestFlowSink(t *testing.T) {

}
