/**
This package is used to provide an implementation of the
Chord network protocol. It can be used to provide a DHT
which is tolerant to churn in the member ndoes.
*/
package chord

import (
	"crypto/sha1"
	"hash"
	"sort"
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
	FindSuccessor(*Vnode, []byte) (*Vnode, error)

	// Register for an RPC callbacks
	Register(*Vnode, VnodeRPC)
}

// These are the methods to invoke on the registered vnodes
type VnodeRPC interface {
	GetPredecessor() (*Vnode, error)
	Notify(*Vnode) ([]*Vnode, error)
	FindSuccessor([]byte) (*Vnode, error)
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
	vnodes    []localVnode
	shutdown  bool
}

// Creates an iterator over Vnodes
type VnodeIterator interface {
	Next() (*Vnode, error) // Returns the next vnode
	Done() bool            // Returns true if all vnodes exhausted
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
	vnodes := make([]localVnode, conf.NumVnodes)
	trans = InitLocalTransport(trans)
	ring := &Ring{conf, trans, vnodes, false}
	for i := 0; i < conf.NumVnodes; i++ {
		vn := &vnodes[i]
		vn.ring = ring
		if err := vn.init(i); err != nil {
			return nil, err
		}
	}

	// Sort the vnodes
	sort.Sort(ring)

	// Setup each vnode successors
	for i := 0; i < conf.NumVnodes; i++ {
		if i == len(vnodes)-1 {
			vnodes[i].successors[0] = &vnodes[0].Vnode
		} else {
			vnodes[i].successors[0] = &vnodes[i+1].Vnode
		}
	}

	// Schedule each Vnode
	for i := 0; i < conf.NumVnodes; i++ {
		vnodes[i].schedule()
	}
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

// Does a key lookup
func (r *Ring) Lookup(key []byte) (VnodeIterator, error) {
	// Hash the key
	h := r.config.HashFunc()
	h.Write(key)
	key_hash := h.Sum(nil)

	// Find the nearest local vnode
	nearest := r.nearestVnode(key_hash)

	// Use the nearest node for the lookup
	_, err := nearest.FindSuccessor(key_hash)
	if err != nil {
		return nil, err
	}

	// TODO: Iterator over results
	return nil, nil
}

// Does a key lookup, returning only the primary Vnode
func (r *Ring) LookupPrimary(key []byte) (*Vnode, error) {
	iter, err := r.Lookup(key)
	if err != nil {
		return nil, err
	}
	prim, err := iter.Next()
	if err != nil {
		return nil, err
	}
	return prim, nil
}
