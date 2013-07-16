package chord

import (
	"bytes"
	"testing"
)

type MockVnodeRPC struct {
	err       error
	pred      *Vnode
	not_pred  *Vnode
	succ_list []*Vnode
	key       []byte
	succ      *Vnode
}

func (mv *MockVnodeRPC) GetPredecessor() (*Vnode, error) {
	return mv.pred, mv.err
}
func (mv *MockVnodeRPC) Notify(vn *Vnode) ([]*Vnode, error) {
	mv.not_pred = vn
	return mv.succ_list, mv.err
}
func (mv *MockVnodeRPC) FindSuccessor(key []byte) (*Vnode, error) {
	mv.key = key
	return mv.succ, mv.err
}

func makeLocal() *LocalTransport {
	mockTrans := &MockTransport{}
	return InitLocalTransport(mockTrans).(*LocalTransport)
}

func TestInitLocalTransport(t *testing.T) {
	mockTrans := &MockTransport{}
	local := InitLocalTransport(mockTrans).(*LocalTransport)
	if local.remote != mockTrans {
		t.Fatalf("bad remote")
	}
	if local.local == nil {
		t.Fatalf("missing map")
	}
}

func TestLocalPing(t *testing.T) {
	l := makeLocal()
	vn := &Vnode{}
	mockVN := &MockVnodeRPC{}
	l.Register(vn, mockVN)
	if res, err := l.Ping(vn); !res || err != nil {
		t.Fatalf("local ping failed")
	}
}

func TestLocalMissingPing(t *testing.T) {
	l := makeLocal()
	vn := &Vnode{}
	mockVN := &MockVnodeRPC{}
	l.Register(vn, mockVN)

	// Print some random node
	vn2 := &Vnode{}
	if res, err := l.Ping(vn2); res || err == nil {
		t.Fatalf("ping succeeded")
	}
}

func TestLocalGetPredecessor(t *testing.T) {
	l := makeLocal()
	pred := &Vnode{}
	vn := &Vnode{}
	mockVN := &MockVnodeRPC{pred: pred, err: nil}
	l.Register(vn, mockVN)

	res, err := l.GetPredecessor(vn)
	if err != nil {
		t.Fatalf("local GetPredecessor failed")
	}
	if res != pred {
		t.Fatalf("got wrong predecessor")
	}
}

func TestLocalNotify(t *testing.T) {
	l := makeLocal()
	suc1 := &Vnode{}
	suc2 := &Vnode{}
	suc3 := &Vnode{}
	succ_list := []*Vnode{suc1, suc2, suc3}

	mockVN := &MockVnodeRPC{succ_list: succ_list, err: nil}
	vn := &Vnode{}
	l.Register(vn, mockVN)

	self := &Vnode{}
	res, err := l.Notify(vn, self)
	if err != nil {
		t.Fatalf("local notify failed")
	}
	if res == nil || res[0] != suc1 || res[1] != suc2 || res[2] != suc3 {
		t.Fatalf("got wrong successor list")
	}
	if mockVN.not_pred != self {
		t.Fatalf("didn't get notified correctly!")
	}
}

func TestLocalFindSucc(t *testing.T) {
	l := makeLocal()
	suc := &Vnode{}

	mockVN := &MockVnodeRPC{succ: suc, err: nil}
	vn := &Vnode{}
	l.Register(vn, mockVN)

	key := []byte("test")
	res, err := l.FindSuccessor(vn, key)
	if err != nil {
		t.Fatalf("local FindSuccessor failed")
	}
	if res != suc {
		t.Fatalf("got wrong successor")
	}
	if bytes.Compare(mockVN.key, key) != 0 {
		t.Fatalf("didn't get key correctly!")
	}
}
