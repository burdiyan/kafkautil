package kafkautil

import (
	"hash"
	"testing"
)

func TestHashInterface(t *testing.T) {
	var _ hash.Hash32 = MurmurHasher()
}

func TestMurmur2(t *testing.T) {
	// Test cases are generated offline using JVM Kafka client for version 1.0.0.
	cases := []struct {
		Input            []byte
		Expected         int32
		ExpectedPositive uint32
	}{
		{[]byte("21"), -973932308, 1173551340},
		{[]byte("foobar"), -790332482, 1357151166},
		{[]byte{12, 42, 56, 24, 109, 111}, 274204207, 274204207},
		{[]byte("a-little-bit-long-string"), -985981536, 1161502112},
		{[]byte("a-little-bit-longer-string"), -1486304829, 661178819},
		{[]byte("lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8"), -58897971, 2088585677},
		{[]byte{'a', 'b', 'c'}, 479470107, 479470107},
	}

	hasher := MurmurHasher()

	for _, c := range cases {
		if res := murmur2(c.Input); res != c.Expected {
			t.Errorf("for %q expected: %d, got: %d", c.Input, c.Expected, res)
		}

		hasher.Reset()
		hasher.Write(c.Input)

		if res2 := hasher.Sum32(); res2 != uint32(c.ExpectedPositive) {
			t.Errorf("hasher: for %q expected: %d, got: %d", c.Input, c.ExpectedPositive, res2)
		}
	}
}
