package chord

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"time"
)

var (
	errAllKnownSuccDead      = errors.New("all known successors dead")
	errExhaustedProceedNodes = errors.New("exhausted all preceeding nodes")
)

// Converts the ID to string
func (vn *Vnode) String() string {
	return fmt.Sprintf("%x", vn.ID)
}

// Initializes a local vnode
func (vn *localVnode) init(idx int) {
	// Generate an ID
	vn.genID(uint16(idx))

	// Set our host
	vn.Host = vn.ring.config.Hostname

	// Initialize all state
	vn.successors = make([]*Vnode, vn.ring.config.NumSuccessors)
	vn.finger = make([]*Vnode, vn.ring.config.hashBits)

	// Register with the RPC mechanism
	vn.ring.transport.Register(&vn.Vnode, vn)
}

// Schedules the Vnode to do regular maintenence
func (vn *localVnode) schedule() {
	// Setup our stabilize timer
	vn.timer = time.AfterFunc(randStabilize(vn.ring.config), vn.stabilize)
}

// Generates an ID for the node
func (vn *localVnode) genID(idx uint16) {
	// Use the hash funciton
	conf := vn.ring.config
	hash := conf.HashFunc()
	hash.Write([]byte(conf.Hostname))
	binary.Write(hash, binary.BigEndian, idx)

	// Use the hash as the ID
	vn.ID = hash.Sum(nil)
}

// Called to periodically stabilize the vnode
func (vn *localVnode) stabilize() {
	// Clear the timer
	vn.timer = nil

	// Check for shutdown
	if vn.ring.shutdown != nil {
		vn.ring.shutdown <- true
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

// Checks for a new successor
func (vn *localVnode) checkNewSuccessor() error {
	// Ask our successor for it's predecessor
	trans := vn.ring.transport

CHECK_NEW_SUC:
	succ := vn.successors[0]
	if succ == nil {
		panic("Node has no successor!")
	}
	maybeSuc, err := trans.GetPredecessor(succ)
	if err != nil {
		// Check if we have succ list, try to contact next live succ
		known := vn.knownSuccessors()
		if known > 1 {
			for i := 0; i < known; i++ {
				if alive, _ := trans.Ping(vn.successors[0]); !alive {
					// Don't eliminate the last successor we know of
					if i+1 == known {
						return errAllKnownSuccDead
					}

					// Advance the successors list past the dead one
					copy(vn.successors[0:], vn.successors[1:])
					vn.successors[known-1-i] = nil
				} else {
					// Found live successor, check for new one
					goto CHECK_NEW_SUC
				}
			}
		}
		return err
	}

	// Check if we should replace our successor
	if maybeSuc != nil && between(vn.ID, succ.ID, maybeSuc.ID) {
		// Check if new successor is alive before switching
		alive, err := trans.Ping(maybeSuc)
		if alive && err == nil {
			copy(vn.successors[1:], vn.successors[0:len(vn.successors)-1])
			vn.successors[0] = maybeSuc
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
	succList, err := vn.ring.transport.Notify(succ, &vn.Vnode)
	if err != nil {
		return err
	}

	// Trim the successors list if too long
	maxSucc := vn.ring.config.NumSuccessors
	if len(succList) > maxSucc-1 {
		succList = succList[:maxSucc-1]
	}

	// Update local successors list
	for idx, s := range succList {
		if s == nil {
			break
		}
		// Ensure we don't set ourselves as a successor!
		if s == nil || s.String() == vn.String() {
			break
		}
		vn.successors[idx+1] = s
	}
	return nil
}

// RPC: Notify is invoked when a Vnode gets notified
func (vn *localVnode) Notify(maybePred *Vnode) ([]*Vnode, error) {
	// Check if we should update our predecessor
	if vn.predecessor == nil || between(vn.predecessor.ID, vn.ID, maybePred.ID) {
		// Inform the delegate
		conf := vn.ring.config
		old := vn.predecessor
		vn.ring.invokeDelegate(func() {
			conf.Delegate.NewPredecessor(&vn.Vnode, maybePred, old)
		})

		vn.predecessor = maybePred
	}

	// Return our successors list
	return vn.successors, nil
}

// Fixes up the finger table
func (vn *localVnode) fixFingerTable() error {
	// Determine the offset
	hb := vn.ring.config.hashBits
	offset := powerOffset(vn.ID, vn.lastFinger, hb)

	// Find the successor
	nodes, err := vn.FindSuccessors(1, offset)
	if nodes == nil || len(nodes) == 0 || err != nil {
		return err
	}
	node := nodes[0]

	// Update the finger table
	vn.finger[vn.lastFinger] = node

	// Try to skip as many finger entries as possible
	for {
		next := vn.lastFinger + 1
		if next >= hb {
			break
		}
		offset := powerOffset(vn.ID, next, hb)

		// While the node is the successor, update the finger entries
		if betweenRightIncl(vn.ID, node.ID, offset) {
			vn.finger[next] = node
			vn.lastFinger = next
		} else {
			break
		}
	}

	// Increment to the index to repair
	if vn.lastFinger+1 == hb {
		vn.lastFinger = 0
	} else {
		vn.lastFinger++
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

// Finds next N successors. N must be <= NumSuccessors
func (vn *localVnode) FindSuccessors(n int, key []byte) ([]*Vnode, error) {
	// Check if we are the immediate predecessor
	if betweenRightIncl(vn.ID, vn.successors[0].ID, key) {
		return vn.successors[:n], nil
	}

	// Try the closest preceeding nodes
	cp := closestPreceedingVnodeIterator{}
	cp.init(vn, key)
	for {
		// Get the next closest node
		closest := cp.Next()
		if closest == nil {
			break
		}

		// Try that node, break on success
		res, err := vn.ring.transport.FindSuccessors(closest, n, key)
		if err == nil {
			return res, nil
		}
		log.Printf("[ERR] Failed to contact %s. Got %s", closest.String(), err)
	}

	// Determine how many successors we know of
	successors := vn.knownSuccessors()

	// Check if the ID is between us and any non-immediate successors
	for i := 1; i <= successors-n; i++ {
		if betweenRightIncl(vn.ID, vn.successors[i].ID, key) {
			remain := vn.successors[i:]
			if len(remain) > n {
				remain = remain[:n]
			}
			return remain, nil
		}
	}

	// Checked all closer nodes and our successors!
	return nil, errExhaustedProceedNodes
}

// Instructs the vnode to leave
func (vn *localVnode) leave() error {
	// Inform the delegate we are leaving
	conf := vn.ring.config
	pred := vn.predecessor
	succ := vn.successors[0]
	vn.ring.invokeDelegate(func() {
		conf.Delegate.Leaving(&vn.Vnode, pred, succ)
	})

	// Notify predecessor to advance to their next successor
	var err error
	trans := vn.ring.transport
	if vn.predecessor != nil {
		err = trans.SkipSuccessor(vn.predecessor, &vn.Vnode)
	}

	// Notify successor to clear old predecessor
	err = mergeErrors(err, trans.ClearPredecessor(vn.successors[0], &vn.Vnode))
	return err
}

// Used to clear our predecessor when a node is leaving
func (vn *localVnode) ClearPredecessor(p *Vnode) error {
	if vn.predecessor != nil && vn.predecessor.String() == p.String() {
		// Inform the delegate
		conf := vn.ring.config
		old := vn.predecessor
		vn.ring.invokeDelegate(func() {
			conf.Delegate.PredecessorLeaving(&vn.Vnode, old)
		})
		vn.predecessor = nil
	}
	return nil
}

// Used to skip a successor when a node is leaving
func (vn *localVnode) SkipSuccessor(s *Vnode) error {
	// Skip if we have a match
	if vn.successors[0].String() == s.String() {
		// Inform the delegate
		conf := vn.ring.config
		old := vn.successors[0]
		vn.ring.invokeDelegate(func() {
			conf.Delegate.SuccessorLeaving(&vn.Vnode, old)
		})

		known := vn.knownSuccessors()
		copy(vn.successors[0:], vn.successors[1:])
		vn.successors[known-1] = nil
	}
	return nil
}

// Determine how many successors we know of
func (vn *localVnode) knownSuccessors() (successors int) {
	for i := 0; i < len(vn.successors); i++ {
		if vn.successors[i] != nil {
			successors = i + 1
		}
	}
	return
}
