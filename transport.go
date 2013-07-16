package chord

import (
	"fmt"
	"sync"
)

// Provides fast routing to local Vnodes, uses another transport
// for access to remove Vnodes
type LocalTransport struct {
	remote Transport
	lock   sync.RWMutex
	local  map[*Vnode]VnodeRPC
}

// Creates a local transport to wrap a remote transport
func InitLocalTransport(remote Transport) Transport {
	// Replace a nil transport with black hole
	if remote == nil {
		remote = &BlackholeTransport{}
	}

	local := make(map[*Vnode]VnodeRPC)
	return &LocalTransport{remote: remote, local: local}
}

// Ping a Vnode, check for liveness
func (lt *LocalTransport) Ping(vn *Vnode) (bool, error) {
	// Look for it locally
	lt.lock.RLock()
	_, ok := lt.local[vn]
	lt.lock.RUnlock()

	// If it exists locally, handle it
	if ok {
		return true, nil
	}

	// Pass onto remote
	return lt.remote.Ping(vn)
}

// Request a nodes predecessor
func (lt *LocalTransport) GetPredecessor(vn *Vnode) (*Vnode, error) {
	// Look for it locally
	lt.lock.RLock()
	obj, ok := lt.local[vn]
	lt.lock.RUnlock()

	// If it exists locally, handle it
	if ok {
		return obj.GetPredecessor()
	}

	// Pass onto remote
	return lt.remote.GetPredecessor(vn)
}

// Notify our successor of ourselves
func (lt *LocalTransport) Notify(vn, self *Vnode) ([]*Vnode, error) {
	// Look for it locally
	lt.lock.RLock()
	obj, ok := lt.local[vn]
	lt.lock.RUnlock()

	// If it exists locally, handle it
	if ok {
		return obj.Notify(self)
	}

	// Pass onto remote
	return lt.remote.Notify(vn, self)
}

// Find a successor
func (lt *LocalTransport) FindSuccessor(vn *Vnode, key []byte) (*Vnode, error) {
	// Look for it locally
	lt.lock.RLock()
	obj, ok := lt.local[vn]
	lt.lock.RUnlock()

	// If it exists locally, handle it
	if ok {
		return obj.FindSuccessor(key)
	}

	// Pass onto remote
	return lt.remote.FindSuccessor(vn, key)

}

// Register for an RPC callbacks
func (lt *LocalTransport) Register(v *Vnode, o VnodeRPC) {
	// Register local instance
	lt.lock.Lock()
	defer lt.lock.Unlock()
	lt.local[v] = o

	// Register with remote transport
	lt.remote.Register(v, o)
}

// Used to blackhole traffic
type BlackholeTransport struct {
}

func (*BlackholeTransport) Ping(vn *Vnode) (bool, error) {
	return false, nil
}

func (*BlackholeTransport) GetPredecessor(vn *Vnode) (*Vnode, error) {
	return nil, fmt.Errorf("Failed to connect!")
}

func (*BlackholeTransport) Notify(vn, self *Vnode) ([]*Vnode, error) {
	return nil, fmt.Errorf("Failed to connect!")
}

// Find a successor
func (*BlackholeTransport) FindSuccessor(vn *Vnode, key []byte) (*Vnode, error) {
	return nil, fmt.Errorf("Failed to connect!")
}

// Register for an RPC callbacks
func (*BlackholeTransport) Register(v *Vnode, o VnodeRPC) {
}
