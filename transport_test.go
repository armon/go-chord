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
	succ      []*Vnode
}

func (mv *MockVnodeRPC) GetPredecessor() (*Vnode, error) {
	return mv.pred, mv.err
}
func (mv *MockVnodeRPC) Notify(vn *Vnode) ([]*Vnode, error) {
	mv.not_pred = vn
	return mv.succ_list, mv.err
}
func (mv *MockVnodeRPC) FindSuccessors(n int, key []byte) ([]*Vnode, error) {
	mv.key = key
	return mv.succ, mv.err
}

func makeLocal() *LocalTransport {
	return InitLocalTransport(nil).(*LocalTransport)
}

func TestInitLocalTransport(t *testing.T) {
	local := InitLocalTransport(nil).(*LocalTransport)
	if local.remote == nil {
		t.Fatalf("bad remote")
	}
	if local.local == nil {
		t.Fatalf("missing map")
	}
}

func TestLocalPing(t *testing.T) {
	l := makeLocal()
	vn := &Vnode{Id: []byte{1}}
	mockVN := &MockVnodeRPC{}
	l.Register(vn, mockVN)
	if res, err := l.Ping(vn); !res || err != nil {
		t.Fatalf("local ping failed")
	}
}

func TestLocalMissingPing(t *testing.T) {
	l := makeLocal()
	vn := &Vnode{Id: []byte{2}}
	mockVN := &MockVnodeRPC{}
	l.Register(vn, mockVN)

	// Print some random node
	vn2 := &Vnode{Id: []byte{3}}
	if res, _ := l.Ping(vn2); res {
		t.Fatalf("ping succeeded")
	}
}

func TestLocalGetPredecessor(t *testing.T) {
	l := makeLocal()
	pred := &Vnode{Id: []byte{10}}
	vn := &Vnode{Id: []byte{42}}
	mockVN := &MockVnodeRPC{pred: pred, err: nil}
	l.Register(vn, mockVN)

	vn2 := &Vnode{Id: []byte{42}}
	res, err := l.GetPredecessor(vn2)
	if err != nil {
		t.Fatalf("local GetPredecessor failed")
	}
	if res != pred {
		t.Fatalf("got wrong predecessor")
	}

	unknown := &Vnode{Id: []byte{1}}
	res, err = l.GetPredecessor(unknown)
	if err == nil {
		t.Fatalf("expected error!")
	}
}

func TestLocalNotify(t *testing.T) {
	l := makeLocal()
	suc1 := &Vnode{Id: []byte{10}}
	suc2 := &Vnode{Id: []byte{20}}
	suc3 := &Vnode{Id: []byte{30}}
	succ_list := []*Vnode{suc1, suc2, suc3}

	mockVN := &MockVnodeRPC{succ_list: succ_list, err: nil}
	vn := &Vnode{Id: []byte{0}}
	l.Register(vn, mockVN)

	self := &Vnode{Id: []byte{60}}
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

	unknown := &Vnode{Id: []byte{1}}
	res, err = l.Notify(unknown, self)
	if err == nil {
		t.Fatalf("remote notify should fail")
	}
}

func TestLocalFindSucc(t *testing.T) {
	l := makeLocal()
	suc := []*Vnode{&Vnode{Id: []byte{40}}}

	mockVN := &MockVnodeRPC{succ: suc, err: nil}
	vn := &Vnode{Id: []byte{12}}
	l.Register(vn, mockVN)

	key := []byte("test")
	res, err := l.FindSuccessors(vn, 1, key)
	if err != nil {
		t.Fatalf("local FindSuccessor failed")
	}
	if res[0] != suc[0] {
		t.Fatalf("got wrong successor")
	}
	if bytes.Compare(mockVN.key, key) != 0 {
		t.Fatalf("didn't get key correctly!")
	}

	unknown := &Vnode{Id: []byte{1}}
	res, err = l.FindSuccessors(unknown, 1, key)
	if err == nil {
		t.Fatalf("remote find should fail")
	}
}

func TestBHPing(t *testing.T) {
	bh := BlackholeTransport{}
	vn := &Vnode{Id: []byte{12}}
	res, err := bh.Ping(vn)
	if res || err != nil {
		t.Fatalf("expected fail")
	}
}

func TestBHGetPred(t *testing.T) {
	bh := BlackholeTransport{}
	vn := &Vnode{Id: []byte{12}}
	_, err := bh.GetPredecessor(vn)
	if err.Error()[:18] != "Failed to connect!" {
		t.Fatalf("expected fail")
	}
}

func TestBHNotify(t *testing.T) {
	bh := BlackholeTransport{}
	vn := &Vnode{Id: []byte{12}}
	vn2 := &Vnode{Id: []byte{42}}
	_, err := bh.Notify(vn, vn2)
	if err.Error()[:18] != "Failed to connect!" {
		t.Fatalf("expected fail")
	}
}

func TestBHFindSuccessors(t *testing.T) {
	bh := BlackholeTransport{}
	vn := &Vnode{Id: []byte{12}}
	_, err := bh.FindSuccessors(vn, 1, []byte("test"))
	if err.Error()[:18] != "Failed to connect!" {
		t.Fatalf("expected fail")
	}
}

func TestBHRegister(t *testing.T) {
	bh := BlackholeTransport{}
	vn := &localVnode{}
	vn.Id = []byte{12}
	bh.Register(&vn.Vnode, vn)
}
