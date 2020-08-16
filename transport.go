package chord

import (
	"fmt"
	"sync"
)

// Wraps vnode and object
type localRPC struct {
	vnode *Vnode
	obj   VnodeRPC
}

// LocalTransport is used to provides fast routing to Vnodes running
// locally using direct method calls. For any non-local vnodes, the
// request is passed on to another transport.
type LocalTransport struct {
	host   string
	remote Transport
	lock   sync.RWMutex
	local  map[string]*localRPC
}

// InitLocalTransport creates a local transport to wrap a remote transport
func InitLocalTransport(remote Transport) Transport {
	// Replace a nil transport with black hole
	if remote == nil {
		remote = &BlackholeTransport{}
	}

	local := make(map[string]*localRPC)
	return &LocalTransport{remote: remote, local: local}
}

// Checks for a local vnode
func (lt *LocalTransport) get(vn *Vnode) (VnodeRPC, bool) {
	key := vn.String()
	lt.lock.RLock()
	defer lt.lock.RUnlock()
	w, ok := lt.local[key]
	if ok {
		return w.obj, ok
	}
	return nil, ok
}

// ListVnodes .
func (lt *LocalTransport) ListVnodes(host string) ([]*Vnode, error) {
	// Check if this is a local host
	if host == lt.host {
		// Generate all the local clients
		res := make([]*Vnode, 0, len(lt.local))

		// Build list
		lt.lock.RLock()
		for _, v := range lt.local {
			res = append(res, v.vnode)
		}
		lt.lock.RUnlock()

		return res, nil
	}

	// Pass onto remote
	return lt.remote.ListVnodes(host)
}

// Ping .
func (lt *LocalTransport) Ping(vn *Vnode) (bool, error) {
	// Look for it locally
	_, ok := lt.get(vn)

	// If it exists locally, handle it
	if ok {
		return true, nil
	}

	// Pass onto remote
	return lt.remote.Ping(vn)
}

// GetPredecessor satisifies the Transport interface
func (lt *LocalTransport) GetPredecessor(vn *Vnode) (*Vnode, error) {
	// Look for it locally
	obj, ok := lt.get(vn)

	// If it exists locally, handle it
	if ok {
		return obj.GetPredecessor()
	}

	// Pass onto remote
	return lt.remote.GetPredecessor(vn)
}

// Notify satisifies the Transport interface
func (lt *LocalTransport) Notify(vn, self *Vnode) ([]*Vnode, error) {
	// Look for it locally
	obj, ok := lt.get(vn)

	// If it exists locally, handle it
	if ok {
		return obj.Notify(self)
	}

	// Pass onto remote
	return lt.remote.Notify(vn, self)
}

// FindSuccessors satisifies the Transport interface
func (lt *LocalTransport) FindSuccessors(vn *Vnode, n int, key []byte) ([]*Vnode, error) {
	// Look for it locally
	obj, ok := lt.get(vn)

	// If it exists locally, handle it
	if ok {
		return obj.FindSuccessors(n, key)
	}

	// Pass onto remote
	return lt.remote.FindSuccessors(vn, n, key)
}

// ClearPredecessor satisifies the Transport interface
func (lt *LocalTransport) ClearPredecessor(target, self *Vnode) error {
	// Look for it locally
	obj, ok := lt.get(target)

	// If it exists locally, handle it
	if ok {
		return obj.ClearPredecessor(self)
	}

	// Pass onto remote
	return lt.remote.ClearPredecessor(target, self)
}

// SkipSuccessor satisifies the Transport interface
func (lt *LocalTransport) SkipSuccessor(target, self *Vnode) error {
	// Look for it locally
	obj, ok := lt.get(target)

	// If it exists locally, handle it
	if ok {
		return obj.SkipSuccessor(self)
	}

	// Pass onto remote
	return lt.remote.SkipSuccessor(target, self)
}

// Register satisifies the Transport interface
func (lt *LocalTransport) Register(v *Vnode, o VnodeRPC) {
	// Register local instance
	key := v.String()
	lt.lock.Lock()
	lt.host = v.Host
	lt.local[key] = &localRPC{v, o}
	lt.lock.Unlock()

	// Register with remote transport
	lt.remote.Register(v, o)
}

// Deregister ...
func (lt *LocalTransport) Deregister(v *Vnode) {
	key := v.String()
	lt.lock.Lock()
	delete(lt.local, key)
	lt.lock.Unlock()
}

var (
	blackHoleConnectErrStr = "blackhole transport failed to connect: %s"
)

// BlackholeTransport is used to provide an implemenation of the Transport that
// does not actually do anything. Any operation will result in an error.
type BlackholeTransport struct {
}

// ListVnodes satisifies the Transport interface
func (*BlackholeTransport) ListVnodes(host string) ([]*Vnode, error) {
	return nil, fmt.Errorf(blackHoleConnectErrStr, host)
}

// Ping satisifies the Transport interface
func (*BlackholeTransport) Ping(vn *Vnode) (bool, error) {
	return false, nil
}

// GetPredecessor atisifies the Transport interface
func (*BlackholeTransport) GetPredecessor(vn *Vnode) (*Vnode, error) {
	return nil, fmt.Errorf(blackHoleConnectErrStr, vn.String())
}

// Notify satisifies the Transport interface
func (*BlackholeTransport) Notify(vn, self *Vnode) ([]*Vnode, error) {
	return nil, fmt.Errorf(blackHoleConnectErrStr, vn.String())
}

// FindSuccessors satisifies the Transport interface
func (*BlackholeTransport) FindSuccessors(vn *Vnode, n int, key []byte) ([]*Vnode, error) {
	return nil, fmt.Errorf(blackHoleConnectErrStr, vn.String())
}

// ClearPredecessor satisifies the Transport interface
func (*BlackholeTransport) ClearPredecessor(target, self *Vnode) error {
	return fmt.Errorf(blackHoleConnectErrStr, target.String())
}

// SkipSuccessor satisifies the Transport interface
func (*BlackholeTransport) SkipSuccessor(target, self *Vnode) error {
	return fmt.Errorf(blackHoleConnectErrStr, target.String())
}

// Register satisifies the Transport interface
func (*BlackholeTransport) Register(v *Vnode, o VnodeRPC) {
}
