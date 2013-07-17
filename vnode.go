package chord

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"log"
	"math/big"
	"math/rand"
	"time"
)

// Converts the ID to string
func (vn *Vnode) String() string {
	return hex.Dump(vn.Id)
}

// Initializes a local vnode
func (vn *localVnode) init(idx int) error {
	// Generate an ID
	vn.genId(uint16(idx))

	// Initialize all state
	vn.successors = make([]*Vnode, vn.ring.config.NumSuccessors)
	vn.finger = make([]*Vnode, vn.ring.config.HashBits)

	// Register with the RPC mechanism
	vn.ring.transport.Register(&vn.Vnode, vn)
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
	trans := vn.ring.transport
	succ := vn.successors[0]
	maybe_suc, err := trans.GetPredecessor(succ)
	if err != nil {
		// Handle a dead successor
		if alive, _ := trans.Ping(succ); !alive {
			// Advance the successors list past the dead one...
			copy(vn.successors[0:], vn.successors[1:])
			vn.successors[len(vn.successors)-1] = nil
			return nil
		}
		return err
	}

	// Check if we should replace our successor
	if maybe_suc != nil && between(vn.Id, succ.Id, maybe_suc.Id) {
		// Check if new successor is alive before switching
		alive, err := trans.Ping(maybe_suc)
		if alive && err == nil {
			vn.successors[0] = maybe_suc
		} else {
			return err
		}
	}
	return nil
}

// RPC: Invoked to return out predecessor
func (vn *localVnode) GetPredecessor() (*Vnode, error) {
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
		if s == nil {
			break
		}
		vn.successors[idx+1] = s
	}
	return nil
}

// RPC: Notify is invoked when a Vnode gets notified
func (vn *localVnode) Notify(maybe_pred *Vnode) ([]*Vnode, error) {
	// Check if we should update our predecessor
	if vn.predecessor == nil || between(vn.predecessor.Id, vn.Id, maybe_pred.Id) {
		vn.predecessor = maybe_pred
	}

	// Return our successors list
	return vn.successors, nil
}

// Fixes up the finger table
func (vn *localVnode) fixFingerTable() error {
	// Determine the offset
	hb := vn.ring.config.HashBits
	offset := powerOffset(vn.Id, vn.last_finger, hb)

	// Find the successor
	node, err := vn.FindSuccessor(offset)
	if err != nil {
		return err
	}

	// Update the finger table
	vn.finger[vn.last_finger] = node

	// Try to skip as many finger entries as possible
	for {
		next := vn.last_finger + 1
		if next >= hb {
			break
		}
		offset := powerOffset(vn.Id, next, hb)

		// While the node is the successor, update the finger entries
		if betweenRightIncl(vn.Id, node.Id, offset) {
			vn.finger[next] = node
			vn.last_finger = next
		} else {
			break
		}
	}

	// Increment to the index to repair
	if vn.last_finger+1 == hb {
		vn.last_finger = 0
	} else {
		vn.last_finger++
	}

	return nil
}

// Checks the health of our predecessor
func (vn *localVnode) checkPredecessor() error {
	// Check predecessor
	if vn.predecessor != nil {
		res, err := vn.ring.transport.Ping(vn.predecessor)
		if err != nil {
			return err
		}

		// Predecessor is dead
		if !res {
			vn.predecessor = nil
		}
	}
	return nil
}

// Finds a successor, also used in RPC
func (vn *localVnode) FindSuccessor(key []byte) (*Vnode, error) {
	// Check if the ID is between us and our successor
	if betweenRightIncl(vn.Id, vn.successors[0].Id, key) {
		return vn.successors[0], nil
	} else {
		cp := ClosestPreceedingVnodeIterator{}
		cp.init(vn, key)
		for {
			// Get the next closest node
			closest, err := cp.Next()
			if closest == nil {
				return nil, fmt.Errorf("Exhausted all preceeding nodes!")
			} else if err != nil {
				return nil, err
			}

			// Try that node, break on success
			res, err := vn.ring.transport.FindSuccessor(closest, key)
			if err == nil {
				return res, nil
			} else {
				log.Printf("[ERR] Failed to contact %s. Got %s", closest.String(), err)
			}
		}
	}
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
