// Package chord provides an implementation of the Chord network protocol.
package chord

import (
	"crypto/sha1"
	"fmt"
	"hash"
	"time"
)

// Transport mplements the methods needed for a Chord ring
type Transport interface {
	// Gets a list of the vnodes on the box
	ListVnodes(string) ([]*Vnode, error)

	// Ping a Vnode, check for liveness
	Ping(*Vnode) (bool, error)

	// Request a nodes predecessor
	GetPredecessor(*Vnode) (*Vnode, error)

	// Notify our successor of ourselves
	Notify(target, self *Vnode) ([]*Vnode, error)

	// Find a successor
	FindSuccessors(*Vnode, int, []byte) ([]*Vnode, error)

	// Clears a predecessor if it matches a given vnode. Used to leave.
	ClearPredecessor(target, self *Vnode) error

	// Instructs a node to skip a given successor. Used to leave.
	SkipSuccessor(target, self *Vnode) error

	// Register for an RPC callbacks
	Register(*Vnode, VnodeRPC)
}

// VnodeRPC implements methods to invoke on the registered vnodes
type VnodeRPC interface {
	GetPredecessor() (*Vnode, error)
	Notify(*Vnode) ([]*Vnode, error)
	FindSuccessors(int, []byte) ([]*Vnode, error)
	ClearPredecessor(*Vnode) error
	SkipSuccessor(*Vnode) error
}

// Delegate to notify on ring events
type Delegate interface {
	NewPredecessor(local, remoteNew, remotePrev *Vnode)
	Leaving(local, pred, succ *Vnode)
	PredecessorLeaving(local, remote *Vnode)
	SuccessorLeaving(local, remote *Vnode)
	Shutdown()
}

// Config holds the configuration for Chord nodes
type Config struct {
	Hostname      string           // Local host name
	NumVnodes     int              // Number of vnodes per physical node
	HashFunc      func() hash.Hash // Hash function to use
	StabilizeMin  time.Duration    // Minimum stabilization time
	StabilizeMax  time.Duration    // Maximum stabilization time
	NumSuccessors int              // Number of successors to maintain
	Delegate      Delegate         // Invoked to handle ring events
	hashBits      int              // Bit size of the hash function
}

// Vnode epresents an Vnode, local or remote
type Vnode struct {
	ID   []byte // Virtual ID
	Host string // Host identifier
}

// Represents a local Vnode
type localVnode struct {
	Vnode
	ring        *Ring
	successors  []*Vnode
	finger      []*Vnode
	lastFinger  int
	predecessor *Vnode
	stabilized  time.Time
	timer       *time.Timer
}

// Ring stores the state required for a Chord ring
type Ring struct {
	config     *Config
	transport  Transport
	vnodes     []*localVnode
	delegateCh chan func()
	shutdown   chan bool
}

// DefaultConfig returns the default Ring configuration
func DefaultConfig(hostname string) *Config {
	return &Config{
		hostname,
		8,        // 8 vnodes
		sha1.New, // SHA1
		time.Duration(15 * time.Second),
		time.Duration(45 * time.Second),
		8,   // 8 successors
		nil, // No delegate
		160, // 160bit hash function
	}
}

// Create creates a new Chord ring given the config and transport
func Create(conf *Config, trans Transport) (*Ring, error) {
	// Initialize the hash bits
	conf.hashBits = conf.HashFunc().Size() * 8

	// Create and initialize a ring
	ring := &Ring{}
	ring.init(conf, trans)
	ring.setLocalSuccessors()
	ring.schedule()
	return ring, nil
}

// Join joins an existing Chord ring
func Join(conf *Config, trans Transport, existing string) (*Ring, error) {
	// Initialize the hash bits
	conf.hashBits = conf.HashFunc().Size() * 8

	// Request a list of Vnodes from the remote host
	hosts, err := trans.ListVnodes(existing)
	if err != nil {
		return nil, err
	}
	if hosts == nil || len(hosts) == 0 {
		return nil, fmt.Errorf("remote host has no vnodes")
	}

	// Create a ring
	ring := &Ring{}
	ring.init(conf, trans)

	// Acquire a live successor for each Vnode
	for _, vn := range ring.vnodes {
		// Get the nearest remote vnode
		nearest := nearestVnodeToKey(hosts, vn.ID)

		// Query for a list of successors to this Vnode
		succs, err := trans.FindSuccessors(nearest, conf.NumSuccessors, vn.ID)
		if err != nil {
			return nil, fmt.Errorf("failed to find successor for vnodes: %v", err)
		}
		if succs == nil || len(succs) == 0 {
			return nil, fmt.Errorf("failed to find successor for vnodes")
		}

		// Assign the successors
		for idx, s := range succs {
			vn.successors[idx] = s
		}
	}

	// Start delegate handler
	if ring.config.Delegate != nil {
		go ring.delegateHandler()
	}

	// Do a fast stabilization, will schedule regular execution
	for _, vn := range ring.vnodes {
		vn.stabilize()
	}
	return ring, nil
}

// Leave leaves a given Chord ring and shuts down the local vnodes
func (r *Ring) Leave() error {
	// Shutdown the vnodes first to avoid further stabilization runs
	r.stopVnodes()

	// Instruct each vnode to leave
	var err error
	for _, vn := range r.vnodes {
		err = mergeErrors(err, vn.leave())
	}

	// Wait for the delegate callbacks to complete
	r.stopDelegate()
	return err
}

// Shutdown shuts down the local processes in a given Chord ring
// Blocks until all the vnodes terminate.
func (r *Ring) Shutdown() {
	r.stopVnodes()
	r.stopDelegate()
}

// Lookup does a key lookup for up to N successors of a key
func (r *Ring) Lookup(n int, key []byte) ([]*Vnode, error) {
	// Ensure that n is sane
	if n > r.config.NumSuccessors {
		return nil, fmt.Errorf("cannot ask for more successors than configured")
	}

	// Hash the key
	h := r.config.HashFunc()
	h.Write(key)
	keyHash := h.Sum(nil)

	// Find the nearest local vnode
	nearest := r.nearestVnode(keyHash)

	// Use the nearest node for the lookup
	successors, err := nearest.FindSuccessors(n, keyHash)
	if err != nil {
		return nil, err
	}

	// Trim the nil successors
	for successors[len(successors)-1] == nil {
		successors = successors[:len(successors)-1]
	}
	return successors, nil
}
