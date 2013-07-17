package chord

import (
	"math/big"
	"testing"
)

func TestNextClosest(t *testing.T) {
	// Make the vnodes on the ring (mod 64)
	v1 := &Vnode{Id: []byte{1}}
	v2 := &Vnode{Id: []byte{10}}
	//v3 := &Vnode{Id: []byte{20}}
	v4 := &Vnode{Id: []byte{32}}
	//v5 := &Vnode{Id: []byte{40}}
	//v6 := &Vnode{Id: []byte{48}}
	v7 := &Vnode{Id: []byte{62}}

	// Make a vnode
	vn := &localVnode{}
	vn.Id = []byte{54}
	vn.successors = []*Vnode{v7}
	vn.finger = []*Vnode{nil, nil, nil, v1, v2, v4}
	vn.ring = &Ring{}
	vn.ring.config = &Config{HashBits: 6}

	// Make an iterator
	k := []byte{32}
	cp := &ClosestPreceedingVnodeIterator{}
	cp.init(vn, k)

	// Iterate until we are done
	s1, err := cp.Next()
	if s1 != v2 || err != nil {
		t.Fatalf("Expect v2. %v", s1)
	}

	s2, err := cp.Next()
	if s2 != v1 || err != nil {
		t.Fatalf("Expect v1. %v", s2)
	}

	s3, err := cp.Next()
	if s3 != v7 || err != nil {
		t.Fatalf("Expect v7. %v", s3)
	}

	s4, err := cp.Next()
	if s4 != nil || err != nil {
		t.Fatalf("Expect nil. %v", s4)
	}
}

func TestClosest(t *testing.T) {
	a := &Vnode{Id: []byte{128}}
	b := &Vnode{Id: []byte{32}}
	k := []byte{45}
	c := closest_preceeding_vnode(a, b, k, 8)
	if c != b {
		t.Fatalf("expect b to be closer!")
	}
}

func TestDistance(t *testing.T) {
	a := []byte{63}
	b := []byte{3}
	d := distance(a, b, 6) // Ring size of 64
	if d.Cmp(big.NewInt(4)) != 0 {
		t.Fatalf("expect distance 4! %v", d)
	}

	a = []byte{0}
	b = []byte{65}
	d = distance(a, b, 7) // Ring size of 128
	if d.Cmp(big.NewInt(65)) != 0 {
		t.Fatalf("expect distance 65! %v", d)
	}

	a = []byte{1}
	b = []byte{255}
	d = distance(a, b, 8) // Ring size of 256
	if d.Cmp(big.NewInt(254)) != 0 {
		t.Fatalf("expect distance 254! %v", d)
	}
}
