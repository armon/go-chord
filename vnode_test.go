package chord

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"testing"
	"time"
)

type MockTransport struct {
}

// Ping a Vnode, check for liveness
func (*MockTransport) Ping(*Vnode) (bool, error) {
	return false, fmt.Errorf("Not supported")
}

// Request a nodes predecessor
func (*MockTransport) GetPredecessor(*Vnode) (*Vnode, error) {
	return nil, fmt.Errorf("Not supported")
}

// Notify our successor of ourselves
func (*MockTransport) Notify(target, self *Vnode) ([]*Vnode, error) {
	return nil, fmt.Errorf("Not supported")
}

// Find a successor
func (*MockTransport) FindSuccessors(*Vnode, int, []byte) ([]*Vnode, error) {
	return nil, fmt.Errorf("Not supported")

}

// Register for an RPC callbacks
func (*MockTransport) Register(*Vnode, VnodeRPC) {

}

func makeVnode() *localVnode {
	min := time.Duration(10 * time.Second)
	max := time.Duration(30 * time.Second)
	conf := &Config{
		StabilizeMin: min,
		StabilizeMax: max,
		HashFunc:     sha1.New}
	mockTrans := &MockTransport{}
	ring := &Ring{config: conf, transport: mockTrans}
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
