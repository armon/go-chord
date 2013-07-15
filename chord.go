/**
This package is used to provide an implementation of the
Chord network protocol. It can be used to provide a DHT
which is tolerant to churn in the member ndoes.
*/
package chord

import (
	"hash"
	"time"
)

// Implements the methods needed for a Chord ring
type Transport interface {
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
	NumVnodes    int              // Number of vnodes per physical node
	HashFunc     func() hash.Hash // Hash function to use
	StabilizeMin time.Duration    // Minimum stabilization time
	StabilizeMax time.Duration    // Maximum stabilization time
	Delegate     Delegate         // Invoked to handle ring events
}

// Represents an Vnode, local or remote
type Vnode struct {
	Id   []byte // Virtual ID
	Host string // Host identifier
}

// Represents a local Vnode
type localVnode struct {
	Vnode
	successors  []*Vnode
	finger      []*Vnode
	predecessor *Vnode
	stabilized  time.Time
}

// Stores the state required for a Chord ring
type Ring struct {
	config    *Config
	transport Transport
	vnodes    []*localVnode
}

// Creates an iterator over Vnodes
type VnodeIterator interface {
	Next() (*Vnode, error) // Returns the next vnode
	Done() bool            // Returns true if all vnodes exhausted
}

// Creates a new Chord ring given the config and transport
func Create(conf *Config, trans Transport) (*Ring, error) {
	return nil, nil
}

// Joins an existing Chord ring
func Join(conf *Config, trans Transport, existing string) (*Ring, error) {
	return nil, nil
}

// Leaves a given Chord ring
func (*Ring) Leave() error {
	return nil
}

// Does a key lookup
func (*Ring) Lookup(key []byte) (VnodeIterator, error) {
	return nil, nil
}

// Does a key lookup, returning only the primary Vnode
func (*Ring) LookupPrimary(key []byte) (*Vnode, error) {
	return nil, nil
}
