/**
This package is used to provide an implementation of the
Chord network protocol. It can be used to provide a DHT
which is tolerant to churn in the member ndoes.
*/
package chord

import (
	"crypto/sha1"
	"fmt"
	"hash"
	"time"
)

// Implements the methods needed for a Chord ring
type Transport interface {
	// Ping a Vnode, check for liveness
	Ping(*Vnode) (bool, error)

	// Request a nodes predecessor
	GetPredecessor(*Vnode) (*Vnode, error)

	// Notify our successor of ourselves
	Notify(target, self *Vnode) ([]*Vnode, error)

	// Find a successor
	FindSuccessors(*Vnode, int, []byte) ([]*Vnode, error)

	// Register for an RPC callbacks
	Register(*Vnode, VnodeRPC)
}

// These are the methods to invoke on the registered vnodes
type VnodeRPC interface {
	GetPredecessor() (*Vnode, error)
	Notify(*Vnode) ([]*Vnode, error)
	FindSuccessors(int, []byte) ([]*Vnode, error)
}

// Delegate to notify on ring events
type Delegate interface {
	NewSuccessor(local *Vnode, remoteNew *Vnode, remotePrev *Vnode)
	NewPredecessor(local *Vnode, remoteNew *Vnode, remotePrev *Vnode)
	PredecessorLeaving(local *Vnode, remote *Vnode)
	SuccessorLeaving(local *Vnode, remote *Vnode)
}

// Configuration for Chord nodes
type Config struct {
	Hostname      string           // Local host name
	NumVnodes     int              // Number of vnodes per physical node
	HashFunc      func() hash.Hash // Hash function to use
	HashBits      int              // Bit size of the hash function
	StabilizeMin  time.Duration    // Minimum stabilization time
	StabilizeMax  time.Duration    // Maximum stabilization time
	NumSuccessors int              // Number of successors to maintain
	Delegate      Delegate         // Invoked to handle ring events
}

// Represents an Vnode, local or remote
type Vnode struct {
	Id   []byte // Virtual ID
	Host string // Host identifier
}

// Represents a local Vnode
type localVnode struct {
	Vnode
	ring        *Ring
	successors  []*Vnode
	finger      []*Vnode
	last_finger int
	predecessor *Vnode
	stabilized  time.Time
	timer       *time.Timer
}

// Stores the state required for a Chord ring
type Ring struct {
	config    *Config
	transport Transport
	vnodes    []*localVnode
	shutdown  bool
}

// Returns the default Ring configuration
func DefaultConfig(hostname string) *Config {
	return &Config{
		hostname,
		8,        // 8 vnodes
		sha1.New, // SHA1
		160,      // 160bit hash function
		time.Duration(15 * time.Second),
		time.Duration(45 * time.Second),
		8,   // 8 successors
		nil, // No delegate
	}
}

// Creates a new Chord ring given the config and transport
func Create(conf *Config, trans Transport) (*Ring, error) {
	// Create and initialize a ring
	ring := &Ring{}
	if err := ring.init(conf, trans); err != nil {
		return nil, err
	}
	ring.setLocalSuccessors()
	ring.schedule()
	return ring, nil
}

// Joins an existing Chord ring
func Join(conf *Config, trans Transport, existing string) (*Ring, error) {
	return nil, nil
}

// Leaves a given Chord ring
func (*Ring) Leave() error {
	return nil
}

// Shutdown shuts down the local processes in a given Chord ring
func (r *Ring) Shutdown() error {
	r.shutdown = true
	return nil
}

// Does a key lookup for up to N successors of a key
func (r *Ring) Lookup(n int, key []byte) ([]*Vnode, error) {
	// Ensure that n is sane
	if n > r.config.NumSuccessors {
		return nil, fmt.Errorf("Cannot ask for more successors than NumSuccessors!")
	}

	// Hash the key
	h := r.config.HashFunc()
	h.Write(key)
	key_hash := h.Sum(nil)

	// Find the nearest local vnode
	nearest := r.nearestVnode(key_hash)

	// Use the nearest node for the lookup
	successors, err := nearest.FindSuccessors(n, key_hash)
	if err != nil {
		return nil, err
	}

	// Trim the nil successors
	for successors[len(successors)-1] == nil {
		successors = successors[:len(successors)-1]
	}
	return successors, nil
}
