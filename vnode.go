package chord

import (
	"bytes"
	"encoding/binary"
	"log"
	"math/rand"
	"time"
)

// Initializes a local vnode
func (vn *localVnode) init(idx int) error {
	// Generate an ID
	vn.genId(uint16(idx))

	// Initialize all state
	vn.successors = make([]*Vnode, vn.ring.config.NumSuccessors)
	vn.finger = make([]*Vnode, 16)
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
	// TODO: Check for a new successor
	return nil
}

// Notifies our successor of us, updates successor list
func (vn *localVnode) notifySuccessor() error {
	// TODO: Notify successor

	// TODO: Update local successors list
	return nil
}

// Fixes up the finger table
func (vn *localVnode) fixFingerTable() error {
	// TODO: Fix finger table
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

// Checks if a key is STRICTLY between two ID's exclusively
func between(id1, id2, key []byte) bool {
	if bytes.Compare(id1, key) >= 0 {
		return false
	}
	if bytes.Compare(id2, key) <= 0 {
		return false
	}
	return true
}
