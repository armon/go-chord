package chord

import (
	"bytes"
	"crypto/sha1"
	"sort"
	"testing"
)

func makeRing() *Ring {
	conf := &Config{
		NumVnodes: 3,
		HashFunc:  sha1.New}

	vnodes := make([]localVnode, conf.NumVnodes)
	ring := &Ring{config: conf, vnodes: vnodes}
	for i := 0; i < conf.NumVnodes; i++ {
		vn := &vnodes[i]
		vn.ring = ring
		vn.init(i)
	}
	return ring
}

func TestRingLen(t *testing.T) {
	ring := makeRing()
	if ring.Len() != 3 {
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
	if bytes.Compare(ring.vnodes[0].Id, ring.vnodes[2].Id) != -1 {
		t.Fatalf("bad sort")
	}
}
