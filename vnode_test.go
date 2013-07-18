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
		NumSuccessors: 8,
		StabilizeMin:  min,
		StabilizeMax:  max,
		HashFunc:      sha1.New}
	trans := InitLocalTransport(nil)
	ring := &Ring{config: conf, transport: trans}
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

func TestVnodeStabilizeResched(t *testing.T) {
	vn := makeVnode()
	vn.init(1)
	vn.successors[0] = &vn.Vnode
	vn.schedule()
	vn.stabilize()

	if vn.timer == nil {
		t.Fatalf("expected timer")
	}
	if vn.stabilized.IsZero() {
		t.Fatalf("expected time")
	}
	vn.timer.Stop()
}

func TestVnodeKnownSucc(t *testing.T) {
	vn := makeVnode()
	vn.init(0)
	if vn.knownSuccessors() != 0 {
		t.Fatalf("wrong num known!")
	}
	vn.successors[0] = &Vnode{Id: []byte{1}}
	if vn.knownSuccessors() != 1 {
		t.Fatalf("wrong num known!")
	}
}

// Checks panic if no successors
func TestVnodeCheckNewSuccAlivePanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic!")
		}
	}()
	vn1 := makeVnode()
	vn1.init(1)
	vn1.checkNewSuccessor()
}

// Checks pinging a live successor with no changes
func TestVnodeCheckNewSuccAlive(t *testing.T) {
	vn1 := makeVnode()
	vn1.init(1)

	vn2 := makeVnode()
	vn2.ring = vn1.ring
	vn2.init(2)
	vn2.predecessor = &vn1.Vnode
	vn1.successors[0] = &vn2.Vnode

	if pred, _ := vn2.GetPredecessor(); pred != &vn1.Vnode {
		t.Fatalf("expected vn1 as predecessor")
	}

	if err := vn1.checkNewSuccessor(); err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	if vn1.successors[0] != &vn2.Vnode {
		t.Fatalf("unexpected successor!")
	}
}
