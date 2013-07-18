package chord

import (
	"bytes"
	"crypto/sha1"
	"sort"
	"testing"
)

func makeRing() *Ring {
	conf := &Config{
		NumVnodes:     5,
		NumSuccessors: 8,
		HashFunc:      sha1.New}

	vnodes := make([]*localVnode, conf.NumVnodes)
	trans := InitLocalTransport(nil)
	ring := &Ring{config: conf, vnodes: vnodes, transport: trans}
	for i := 0; i < conf.NumVnodes; i++ {
		vn := &localVnode{}
		vnodes[i] = vn
		vn.ring = ring
		vn.init(i)
	}
	return ring
}

func TestRingLen(t *testing.T) {
	ring := makeRing()
	if ring.Len() != 5 {
		t.Fatalf("wrong len")
	}
}

func TestRingSort(t *testing.T) {
	ring := makeRing()
	sort.Sort(ring)
	if bytes.Compare(ring.vnodes[0].Id, ring.vnodes[1].Id) != -1 {
		t.Fatalf("bad sort")
	}
	if bytes.Compare(ring.vnodes[1].Id, ring.vnodes[2].Id) != -1 {
		t.Fatalf("bad sort")
	}
	if bytes.Compare(ring.vnodes[2].Id, ring.vnodes[3].Id) != -1 {
		t.Fatalf("bad sort")
	}
	if bytes.Compare(ring.vnodes[3].Id, ring.vnodes[4].Id) != -1 {
		t.Fatalf("bad sort")
	}
}

func TestRingNearest(t *testing.T) {
	ring := makeRing()
	ring.vnodes[0].Id = []byte{2}
	ring.vnodes[1].Id = []byte{4}
	ring.vnodes[2].Id = []byte{7}
	ring.vnodes[3].Id = []byte{10}
	ring.vnodes[4].Id = []byte{14}
	key := []byte{6}

	near := ring.nearestVnode(key)
	if near != ring.vnodes[1] {
		t.Fatalf("got wrong node back!")
	}

	key = []byte{0}
	near = ring.nearestVnode(key)
	if near != ring.vnodes[4] {
		t.Fatalf("got wrong node back!")
	}
}
