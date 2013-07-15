package chord

import (
	"bytes"
	"crypto/sha1"
	"testing"
	"time"
)

func makeVnode() *localVnode {
	min := time.Duration(10 * time.Second)
	max := time.Duration(30 * time.Second)
	conf := &Config{
		StabilizeMin: min,
		StabilizeMax: max,
		HashFunc:     sha1.New}
	ring := &Ring{config: conf}
	return &localVnode{ring: ring}
}

func TestVnodeInit(t *testing.T) {
	vn := makeVnode()
	vn.init(0)
	if vn.Id == nil {
		t.Fatalf("unexpected nil")
	}
	if vn.successors == nil {
		t.Fatalf("unexpected nil")
	}
	if vn.finger == nil {
		t.Fatalf("unexpected nil")
	}
	if vn.timer != nil {
		t.Fatalf("unexpected timer")
	}
}

func TestVnodeSchedule(t *testing.T) {
	vn := makeVnode()
	vn.schedule()
	if vn.timer == nil {
		t.Fatalf("unexpected nil")
	}
}

func TestGenId(t *testing.T) {
	vn := makeVnode()
	var ids [][]byte
	for i := 0; i < 16; i++ {
		vn.genId(uint16(i))
		ids = append(ids, vn.Id)
	}

	for idx, val := range ids {
		for i := 0; i < len(ids); i++ {
			if idx != i && bytes.Compare(ids[i], val) == 0 {
				t.Fatalf("unexpected id collision!")
			}
		}
	}
}

func TestVnodeStabilizeShutdown(t *testing.T) {
	vn := makeVnode()
	vn.schedule()
	vn.ring.shutdown = true
	vn.stabilize()

	if vn.timer != nil {
		t.Fatalf("unexpected timer")
	}
	if !vn.stabilized.IsZero() {
		t.Fatalf("unexpected time")
	}
}

func TestRandStabilize(t *testing.T) {
	min := time.Duration(10 * time.Second)
	max := time.Duration(30 * time.Second)
	conf := &Config{
		StabilizeMin: min,
		StabilizeMax: max}

	var times []time.Duration
	for i := 0; i < 1000; i++ {
		after := randStabilize(conf)
		times = append(times, after)
		if after < min {
			t.Fatalf("after below min")
		}
		if after > max {
			t.Fatalf("after above max")
		}
	}

	collisions := 0
	for idx, val := range times {
		for i := 0; i < len(times); i++ {
			if idx != i && times[i] == val {
				collisions += 1
			}
		}
	}

	if collisions > 3 {
		t.Fatalf("too many collisions! %d", collisions)
	}
}

func TestBetween(t *testing.T) {
	t1 := []byte{0, 0, 0, 0}
	t2 := []byte{1, 0, 0, 0}
	k := []byte{0, 0, 5, 0}
	if !between(t1, t2, k) {
		t.Fatalf("expected k between!")
	}
	if between(t1, t2, t1) {
		t.Fatalf("dont expect t1 between!")
	}
	if between(t1, t2, t2) {
		t.Fatalf("dont expect t1 between!")
	}

	k = []byte{2, 0, 0, 0}
	if between(t1, t2, k) {
		t.Fatalf("dont expect k between!")
	}
}

func TestBetweenRightIncl(t *testing.T) {
	t1 := []byte{0, 0, 0, 0}
	t2 := []byte{1, 0, 0, 0}
	k := []byte{1, 0, 0, 0}
	if !betweenRightIncl(t1, t2, k) {
		t.Fatalf("expected k between!")
	}
}

func TestPowerOffset(t *testing.T) {
	id := []byte{0, 0, 0, 0}
	exp := 30
	mod := 32
	val := powerOffset(id, exp, mod)
	if val[0] != 64 {
		t.Fatalf("unexpected val! %v", val)
	}

	// 0-7, 8-15, 16-23, 24-31
	id = []byte{0, 0xff, 0xff, 0xff}
	exp = 23
	val = powerOffset(id, exp, mod)
	if val[0] != 1 || val[1] != 0x7f || val[2] != 0xff || val[3] != 0xff {
		t.Fatalf("unexpected val! %v", val)
	}
}
