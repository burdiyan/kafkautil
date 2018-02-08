package kafkautil

import (
	"encoding/binary"
	"io"
	"math/rand"
	"testing"

	"github.com/pkg/errors"
)

type fakeRecord struct{}

func (*fakeRecord) Schema() string {
	return "fake-schema"
}

func (*fakeRecord) Serialize(w io.Writer) error {
	_, err := w.Write([]byte("serialized-fake-record"))
	return err
}

type fakeCodec struct{}

func (*fakeCodec) Encode(value interface{}) ([]byte, error) {
	panic("this should not be called when wrapped")
}

func (*fakeCodec) Decode(data []byte) (interface{}, error) {
	if string(data) != "serialized-fake-record" {
		return nil, errors.New("serialized-fake-record expected")
	}

	return &fakeRecord{}, nil
}

type fakeRegisterer struct {
	calls  int
	lastID int32
}

func (f *fakeRegisterer) RegisterNewSchema(subject, schema string) (int, error) {
	f.calls++
	f.lastID = rand.Int31()

	return int(f.lastID), nil
}

func TestWrapCodec(t *testing.T) {
	r := new(fakeRegisterer)

	c := WrapCodec(new(fakeCodec), r, "test-subject")

	data, err := c.Encode(new(fakeRecord))
	if err != nil {
		t.Error(err)
	}

	if data[0] != magicByte {
		t.Errorf("missing magic byte %v", data)
	}

	if id := binary.BigEndian.Uint32(data[1:5]); int32(id) != r.lastID {
		t.Errorf("expected schema ID: %d, got: %d", r.lastID, id)
	}

	if string(data[5:]) != "serialized-fake-record" {
		t.Errorf("expected text 'serialized-fake-record', got: %s", string(data[5:]))
	}

	v, err := c.Decode(data)
	if err != nil {
		t.Error(err)
	}

	if !(*(v.(*fakeRecord)) == fakeRecord{}) {
		t.Errorf("record types don't match")
	}

	// Testing schema cache

	c.Encode(new(fakeRecord))

	if r.calls > 1 {
		t.Errorf("registerer should not be called if schema is cached, calls: %d", r.calls)
	}
}
