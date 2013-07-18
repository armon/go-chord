package chord

import (
	"bytes"
	"crypto/sha1"
	"sort"
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

// Checks pinging a dead successor with no alternates
func TestVnodeCheckNewSuccDead(t *testing.T) {
	vn1 := makeVnode()
	vn1.init(1)
	vn1.successors[0] = &Vnode{Id: []byte{0}}

	if err := vn1.checkNewSuccessor(); err == nil {
		t.Fatalf("err!", err)
	}

	if vn1.successors[0].String() != "00" {
		t.Fatalf("unexpected successor!")
	}
}

// Checks pinging a dead successor with alternate
func TestVnodeCheckNewSuccDeadAlternate(t *testing.T) {
	r := makeRing()
	sort.Sort(r)

	vn1 := r.vnodes[0]
	vn2 := r.vnodes[1]
	vn3 := r.vnodes[2]

	vn1.successors[0] = &vn2.Vnode
	vn1.successors[1] = &vn3.Vnode
	vn2.predecessor = &vn1.Vnode
	vn3.predecessor = &vn2.Vnode

	// Remove vn2
	(r.transport.(*LocalTransport)).Deregister(&vn2.Vnode)

	// Should not get an error
	if err := vn1.checkNewSuccessor(); err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	// Should become vn3
	if vn1.successors[0] != &vn3.Vnode {
		t.Fatalf("unexpected successor!")
	}
}

// Checks pinging a dead successor with all dead alternates
func TestVnodeCheckNewSuccAllDeadAlternates(t *testing.T) {
	r := makeRing()
	sort.Sort(r)

	vn1 := r.vnodes[0]
	vn2 := r.vnodes[1]
	vn3 := r.vnodes[2]

	vn1.successors[0] = &vn2.Vnode
	vn1.successors[1] = &vn3.Vnode
	vn2.predecessor = &vn1.Vnode
	vn3.predecessor = &vn2.Vnode

	// Remove vn2
	(r.transport.(*LocalTransport)).Deregister(&vn2.Vnode)
	(r.transport.(*LocalTransport)).Deregister(&vn3.Vnode)

	// Should get an error
	if err := vn1.checkNewSuccessor(); err.Error() != "All known successors dead!" {
		t.Fatalf("unexpected err %s", err)
	}

	// Should just be vn3
	if vn1.successors[0] != &vn3.Vnode {
		t.Fatalf("unexpected successor!")
	}
}

// Checks pinging a successor, and getting a new successor
func TestVnodeCheckNewSuccNewSucc(t *testing.T) {
	r := makeRing()
	sort.Sort(r)

	vn1 := r.vnodes[0]
	vn2 := r.vnodes[1]
	vn3 := r.vnodes[2]

	vn1.successors[0] = &vn3.Vnode
	vn2.predecessor = &vn1.Vnode
	vn3.predecessor = &vn2.Vnode

	// vn3 pred is vn2
	if pred, _ := vn3.GetPredecessor(); pred != &vn2.Vnode {
		t.Fatalf("expected vn2 as predecessor")
	}

	// Should not get an error
	if err := vn1.checkNewSuccessor(); err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	// Should become vn2
	if vn1.successors[0] != &vn2.Vnode {
		t.Fatalf("unexpected successor! %s", vn1.successors[0])
	}

	// 2nd successor should become vn3
	if vn1.successors[1] != &vn3.Vnode {
		t.Fatalf("unexpected 2nd successor!")
	}
}

// Checks pinging a successor, and getting a new successor
// which is not alive
func TestVnodeCheckNewSuccNewSuccDead(t *testing.T) {
	r := makeRing()
	sort.Sort(r)

	vn1 := r.vnodes[0]
	vn2 := r.vnodes[1]
	vn3 := r.vnodes[2]

	vn1.successors[0] = &vn3.Vnode
	vn2.predecessor = &vn1.Vnode
	vn3.predecessor = &vn2.Vnode

	// Remove vn2
	(r.transport.(*LocalTransport)).Deregister(&vn2.Vnode)

	// Should not get an error
	if err := vn1.checkNewSuccessor(); err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	// Should stay vn3
	if vn1.successors[0] != &vn3.Vnode {
		t.Fatalf("unexpected successor!")
	}
}

// Test notifying a successor successfully
func TestVnodeNotifySucc(t *testing.T) {
	r := makeRing()
	sort.Sort(r)

	s1 := &Vnode{Id: []byte{1}}
	s2 := &Vnode{Id: []byte{2}}
	s3 := &Vnode{Id: []byte{3}}

	vn1 := r.vnodes[0]
	vn2 := r.vnodes[1]
	vn1.successors[0] = &vn2.Vnode
	vn2.predecessor = &vn1.Vnode
	vn2.successors[0] = s1
	vn2.successors[1] = s2
	vn2.successors[2] = s3

	// Should get no error
	if err := vn1.notifySuccessor(); err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	// Successor list should be updated
	if vn1.successors[1] != s1 {
		t.Fatalf("bad succ 1")
	}
	if vn1.successors[2] != s2 {
		t.Fatalf("bad succ 2")
	}
	if vn1.successors[3] != s3 {
		t.Fatalf("bad succ 3")
	}

	// Predecessor should not updated
	if vn2.predecessor != &vn1.Vnode {
		t.Fatalf("bad predecessor")
	}
}

// Test notifying a dead successor
func TestVnodeNotifySuccDead(t *testing.T) {
	r := makeRing()
	sort.Sort(r)

	vn1 := r.vnodes[0]
	vn2 := r.vnodes[1]
	vn1.successors[0] = &vn2.Vnode
	vn2.predecessor = &vn1.Vnode

	// Remove vn2
	(r.transport.(*LocalTransport)).Deregister(&vn2.Vnode)

	// Should get error
	if err := vn1.notifySuccessor(); err == nil {
		t.Fatalf("expected err!")
	}
}

func TestVnodeNotifySamePred(t *testing.T) {
	r := makeRing()
	sort.Sort(r)

	s1 := &Vnode{Id: []byte{1}}
	s2 := &Vnode{Id: []byte{2}}
	s3 := &Vnode{Id: []byte{3}}

	vn1 := r.vnodes[0]
	vn2 := r.vnodes[1]
	vn1.successors[0] = &vn2.Vnode
	vn2.predecessor = &vn1.Vnode
	vn2.successors[0] = s1
	vn2.successors[1] = s2
	vn2.successors[2] = s3

	succs, err := vn2.Notify(&vn1.Vnode)
	if err != nil {
		t.Fatalf("unexpected error! %s", err)
	}
	if succs[0] != s1 {
		t.Fatalf("unexpected succ 0")
	}
	if succs[1] != s2 {
		t.Fatalf("unexpected succ 1")
	}
	if succs[2] != s3 {
		t.Fatalf("unexpected succ 2")
	}
	if vn2.predecessor != &vn1.Vnode {
		t.Fatalf("unexpected pred")
	}
}

func TestVnodeNotifyNoPred(t *testing.T) {
	r := makeRing()
	sort.Sort(r)

	s1 := &Vnode{Id: []byte{1}}
	s2 := &Vnode{Id: []byte{2}}
	s3 := &Vnode{Id: []byte{3}}

	vn1 := r.vnodes[0]
	vn2 := r.vnodes[1]
	vn2.successors[0] = s1
	vn2.successors[1] = s2
	vn2.successors[2] = s3

	succs, err := vn2.Notify(&vn1.Vnode)
	if err != nil {
		t.Fatalf("unexpected error! %s", err)
	}
	if succs[0] != s1 {
		t.Fatalf("unexpected succ 0")
	}
	if succs[1] != s2 {
		t.Fatalf("unexpected succ 1")
	}
	if succs[2] != s3 {
		t.Fatalf("unexpected succ 2")
	}
	if vn2.predecessor != &vn1.Vnode {
		t.Fatalf("unexpected pred")
	}
}

func TestVnodeNotifyNewPred(t *testing.T) {
	r := makeRing()
	sort.Sort(r)

	vn1 := r.vnodes[0]
	vn2 := r.vnodes[1]
	vn3 := r.vnodes[2]
	vn3.predecessor = &vn1.Vnode

	_, err := vn3.Notify(&vn2.Vnode)
	if err != nil {
		t.Fatalf("unexpected error! %s", err)
	}
	if vn3.predecessor != &vn2.Vnode {
		t.Fatalf("unexpected pred")
	}
}
