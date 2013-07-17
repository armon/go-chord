package chord

import (
	"math/big"
)

type ClosestPreceedingVnodeIterator struct {
	key           []byte
	vn            *localVnode
	finger_idx    int
	successor_idx int
}

func (cp *ClosestPreceedingVnodeIterator) init(vn *localVnode, key []byte) {
	cp.key = key
	cp.vn = vn
	cp.finger_idx = len(vn.successors)
	cp.successor_idx = len(vn.finger)
}

func (cp *ClosestPreceedingVnodeIterator) Next() (*Vnode, error) {
	// Try to find each node
	var successor_node *Vnode
	var finger_node *Vnode

	// Scan to find the next successor
	vn := cp.vn
	var i int
	for i = cp.successor_idx; i >= 0; i-- {
		if vn.successors[i] == nil {
			continue
		}
		if between(vn.Id, cp.key, vn.successors[i].Id) {
			successor_node = vn.successors[i]
			break
		}
	}
	cp.successor_idx = i

	// Scan to find the next finger
	for i = cp.finger_idx; i >= 0; i-- {
		if vn.finger[i] == nil {
			continue
		}
		if between(vn.Id, cp.key, vn.finger[i].Id) {
			finger_node = vn.finger[i]
			break
		}
	}
	cp.finger_idx = i

	// Determine which node is better
	if successor_node != nil && finger_node != nil {
		// Determine the closer node
		hb := cp.vn.ring.config.HashBits
		closest := closest_vnode(successor_node, finger_node, cp.key, hb)
		if closest == successor_node {
			cp.successor_idx--
		} else {
			cp.finger_idx--
		}
		return closest, nil

	} else if successor_node != nil {
		cp.successor_idx--
		return successor_node, nil

	} else if finger_node != nil {
		cp.finger_idx--
		return finger_node, nil
	}

	return nil, nil
}

// Returns the closest Vnode to the key
func closest_vnode(a, b *Vnode, key []byte, bits int) *Vnode {
	a_dist := distance(a.Id, key, bits)
	b_dist := distance(b.Id, key, bits)
	if a_dist.Cmp(b_dist) <= 0 {
		return a
	} else {
		return b
	}
}

// Computes the minimum distance between two keys modulus a ring size
func distance(a, b []byte, bits int) *big.Int {
	// Get the ring size
	var ring big.Int
	ring.Exp(big.NewInt(2), big.NewInt(int64(bits)), nil)

	// Convert to int
	var a_int, b_int big.Int
	(&a_int).SetBytes(a)
	(&b_int).SetBytes(b)

	// Compute the distances
	var dist_1, dist_2 big.Int
	(&dist_1).Sub(&a_int, &b_int)
	(&dist_2).Sub(&b_int, &a_int)

	// Distance modulus ring size
	(&dist_1).Mod(&dist_1, &ring)
	(&dist_2).Mod(&dist_2, &ring)

	// Take the smaller distance
	if (&dist_1).Cmp(&dist_2) <= 0 {
		return &dist_1
	} else {
		return &dist_2
	}
}
