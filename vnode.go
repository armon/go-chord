package chord

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"math/big"
	"math/rand"
	"time"
)

// Initializes a local vnode
func (vn *localVnode) init(idx int) error {
	// Generate an ID
	vn.genId(uint16(idx))

	// Initialize all state
	vn.successors = make([]*Vnode, vn.ring.config.NumSuccessors)
	vn.finger = make([]*Vnode, vn.ring.config.HashBits)
	return nil
}

// Schedules the Vnode to do regular maintenence
func (vn *localVnode) schedule() {
	// Setup our stabilize timer
	vn.timer = time.AfterFunc(randStabilize(vn.ring.config), vn.stabilize)
}

// Generates an ID for the node
func (vn *localVnode) genId(idx uint16) {
	// Use the hash funciton
	conf := vn.ring.config
	hash := conf.HashFunc()
	hash.Write([]byte(conf.Hostname))
	binary.Write(hash, binary.BigEndian, idx)

	// Use the hash as the ID
	vn.Id = hash.Sum(nil)
}

// Called to periodically stabilize the vnode
func (vn *localVnode) stabilize() {
	// Clear the timer
	vn.timer = nil

	// Check for shutdown
	if vn.ring.shutdown {
		return
	}

	// Setup the next stabilize timer
	defer vn.schedule()

	// Check for new successor
	if err := vn.checkNewSuccessor(); err != nil {
		log.Printf("[ERR] Error checking for new successor: %s", err)
	}

	// Notify the successor
	if err := vn.notifySuccessor(); err != nil {
		log.Printf("[ERR] Error notifying successor: %s", err)
	}

	// Finger table fix up
	if err := vn.fixFingerTable(); err != nil {
		log.Printf("[ERR] Error fixing finger table: %s", err)
	}

	// Check the predecessor
	if err := vn.checkPredecessor(); err != nil {
		log.Printf("[ERR] Error checking predecessor: %s", err)
	}

	// Set the last stabilized time
	vn.stabilized = time.Now()
}

// Generates a random stabilization time
func randStabilize(conf *Config) time.Duration {
	min := conf.StabilizeMin
	max := conf.StabilizeMax
	r := rand.Float64()
	return time.Duration((r * float64(max-min)) + float64(min))
}

// Checks for a new successor
func (vn *localVnode) checkNewSuccessor() error {
	// Ask our successor for it's predecessor
	succ := vn.successors[0]
	maybe_suc, err := vn.ring.transport.GetPredecessor(succ)
	if err != nil {
		return err
	}

	// Check if we should replace our successor
	if maybe_suc != nil && between(vn.Id, succ.Id, maybe_suc.Id) {
		vn.successors[0] = maybe_suc
	}
	return nil
}

// RPC: Invoked to return out predecessor
func (vn *localVnode) getPredecessor() (*Vnode, error) {
	return vn.predecessor, nil
}

// Notifies our successor of us, updates successor list
func (vn *localVnode) notifySuccessor() error {
	// Notify successor
	succ := vn.successors[0]
	succ_list, err := vn.ring.transport.Notify(succ, &vn.Vnode)
	if err != nil {
		return err
	}

	// Trim the successors list if too long
	max_succ := vn.ring.config.NumSuccessors
	if len(succ_list) > max_succ-1 {
		succ_list = succ_list[:max_succ-1]
	}

	// Update local successors list
	for idx, s := range succ_list {
		vn.successors[idx+1] = s
	}
	return nil
}

// RPC: notified is invoked when a Vnode gets notified
func (vn *localVnode) notified(maybe_pred *Vnode) ([]*Vnode, error) {
	// Check if we should update our predecessor
	if vn.predecessor == nil || between(vn.predecessor.Id, vn.Id, maybe_pred.Id) {
		vn.predecessor = maybe_pred
	}

	// Return our successors list
	return vn.successors, nil
}

// Fixes up the finger table
func (vn *localVnode) fixFingerTable() error {
	// Increment to the index to repair
	hb := vn.ring.config.HashBits
	if vn.last_finger+1 == hb {
		vn.last_finger = 0
	} else {
		vn.last_finger++
	}

	// Determine the offset
	offset := powerOffset(vn.Id, vn.last_finger, hb)

	// Find the successor
	node, err := vn.findSuccessor(offset)
	if err != nil {
		return err
	}

	// Update the finger table
	vn.finger[vn.last_finger] = node

	// Try to skip as many finger entries as possible
	for {
		next := (vn.last_finger + 1) % hb
		offset := powerOffset(vn.Id, next, hb)

		// While the node is the successor, update the finger entries
		if bytes.Compare(node.Id, offset) == 1 {
			vn.finger[next] = node
			vn.last_finger = next
		} else {
			break
		}
	}
	return nil
}

// Checks the health of our predecessor
func (vn *localVnode) checkPredecessor() error {
	// Check predecessor
	if vn.predecessor != nil {
		res, err := vn.ring.transport.Ping(vn.predecessor)
		if err != nil {
			return nil
		}

		// Predecessor is dead
		if !res {
			vn.predecessor = nil
		}
	}
	return nil
}

func (vn *localVnode) findSuccessor(key []byte) (*Vnode, error) {
	// Check if the ID is between us and our successor
	if betweenRightIncl(vn.Id, vn.successors[0].Id, key) {
		return vn.successors[0], nil
	} else {
		closest, err := vn.closestPreceeding(key)
		if err != nil {
			return nil, err
		}
		return vn.ring.transport.FindSuccessor(closest, key)
	}
}

func (vn *localVnode) closestPreceeding(key []byte) (*Vnode, error) {
	// Scan the successors list
	for i := len(vn.successors) - 1; i >= 0; i-- {
		if vn.successors[i] == nil {
			continue
		}
		if between(vn.Id, key, vn.successors[i].Id) {
			return vn.successors[i], nil
		}
	}

	for i := len(vn.finger) - 1; i >= 0; i-- {
		if vn.finger[i] == nil {
			continue
		}
		if between(vn.Id, key, vn.finger[i].Id) {
			return vn.finger[i], nil
		}
	}
	return nil, fmt.Errorf("Failed to find a closer node!")
}

// Checks if a key is STRICTLY between two ID's exclusively
func between(id1, id2, key []byte) bool {
	// Check for ring wrap around
	if bytes.Compare(id1, id2) == 1 {
		return bytes.Compare(id1, key) == -1 ||
			bytes.Compare(id2, key) == 1
	}

	// Handle the normal case
	return bytes.Compare(id1, key) == -1 &&
		bytes.Compare(id2, key) == 1
}

// Checks if a key is between two ID's, right inclusive
func betweenRightIncl(id1, id2, key []byte) bool {
	// Check for ring wrap around
	if bytes.Compare(id1, id2) == 1 {
		return bytes.Compare(id1, key) == -1 ||
			bytes.Compare(id2, key) >= 0
	}

	return bytes.Compare(id1, key) == -1 &&
		bytes.Compare(id2, key) >= 0
}

// Computes the offset by (n + 2^exp) % (2^mod)
func powerOffset(id []byte, exp int, mod int) []byte {
	// Copy the existing slice
	off := make([]byte, len(id))
	copy(off, id)

	// Convert the ID to a bigint
	idInt := big.Int{}
	idInt.SetBytes(id)

	// Get the offset
	two := big.NewInt(2)
	offset := big.Int{}
	offset.Exp(two, big.NewInt(int64(exp)), nil)

	// Sum
	sum := big.Int{}
	sum.Add(&idInt, &offset)

	// Get the ceiling
	ceil := big.Int{}
	ceil.Exp(two, big.NewInt(int64(mod)), nil)

	// Apply the mod
	idInt.Mod(&sum, &ceil)

	// Add together
	return idInt.Bytes()
}
