package chord

import (
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	context "golang.org/x/net/context"
	"google.golang.org/grpc"
)

type rpcOutConn struct {
	host   string
	conn   *grpc.ClientConn
	client ChordClient
	used   time.Time
}

type GRPCTransport struct {
	sock     *net.TCPListener
	server   *grpc.Server
	lock     sync.RWMutex
	local    map[string]*localRPC
	poolLock sync.Mutex
	pool     map[string][]*rpcOutConn
	shutdown int32
	timeout  time.Duration
	maxIdle  time.Duration
}

func InitGRPCTransport(listen string, timeout time.Duration) (*GRPCTransport, error) {
	// Try to start the listener
	sock, err := net.Listen("tcp", listen)
	if err != nil {
		return nil, err
	}

	cs := &GRPCTransport{
		sock:    sock.(*net.TCPListener),
		server:  grpc.NewServer(),
		local:   map[string]*localRPC{},
		pool:    map[string][]*rpcOutConn{},
		maxIdle: time.Duration(300 * time.Second),
		timeout: timeout,
	}

	RegisterChordServer(cs.server, cs)

	go cs.listen()
	go cs.reapOld()

	return cs, nil
}

func (cs *GRPCTransport) listen() {
	if err := cs.server.Serve(cs.sock); err != nil {
		log.Println("ERR", err)
	}
}

// Closes old outbound connections
func (cs *GRPCTransport) reapOld() {
	for {
		if atomic.LoadInt32(&cs.shutdown) == 1 {
			return
		}
		time.Sleep(30 * time.Second)
		cs.reapOnce()
	}
}

func (cs *GRPCTransport) reapOnce() {
	cs.poolLock.Lock()
	defer cs.poolLock.Unlock()
	for host, conns := range cs.pool {
		max := len(conns)
		for i := 0; i < max; i++ {
			if time.Since(conns[i].used) > cs.maxIdle {
				conns[i].conn.Close()
				conns[i], conns[max-1] = conns[max-1], nil
				max--
				i--
			}
		}
		// Trim any idle conns
		cs.pool[host] = conns[:max]
	}
}

func (cs *GRPCTransport) Register(v *Vnode, o VnodeRPC) {
	key := v.StringID()
	cs.lock.Lock()
	cs.local[key] = &localRPC{v, o}
	cs.lock.Unlock()
}

// Gets a list of the vnodes on the box
func (cs *GRPCTransport) ListVnodes(host string) ([]*Vnode, error) {
	// Get a conn
	out, err := cs.getConn(host)
	if err != nil {
		return nil, err
	}

	// Response channels
	respChan := make(chan []*Vnode, 1)
	errChan := make(chan error, 1)

	go func() {
		le, err := out.client.ListVnodesServe(context.Background(), &StringParam{Value: host})
		// Return the connection
		cs.returnConn(out)
		if err == nil {
			if le.Err == "" {
				respChan <- le.Vnodes
				return
			}
			err = fmt.Errorf(le.Err)
		}
		errChan <- err
	}()

	select {
	case <-time.After(cs.timeout):
		return nil, fmt.Errorf("Command timed out!")
	case err := <-errChan:
		return nil, err
	case res := <-respChan:
		return res, nil
	}
}

// Ping a Vnode, check for liveness
func (cs *GRPCTransport) Ping(target *Vnode) (bool, error) {
	out, err := cs.getConn(target.Host)
	if err != nil {
		return false, err
	}

	// Response channels
	respChan := make(chan bool, 1)
	errChan := make(chan error, 1)

	go func() {

		be, err := out.client.PingServe(context.Background(), target)
		// Return the connection
		cs.returnConn(out)

		if err == nil {
			if be.Err == "" {
				respChan <- be.Ok
				return
			}
			err = fmt.Errorf(be.Err)
		}
		errChan <- err
	}()

	select {
	case <-time.After(cs.timeout):
		return false, fmt.Errorf("command timed out")
	case err := <-errChan:
		return false, err
	case res := <-respChan:
		return res, nil
	}
}

// Request a nodes predecessor
func (cs *GRPCTransport) GetPredecessor(vn *Vnode) (*Vnode, error) {
	// Get a conn
	out, err := cs.getConn(vn.Host)
	if err != nil {
		return nil, err
	}

	respChan := make(chan *Vnode, 1)
	errChan := make(chan error, 1)

	go func() {
		vnErr, err := out.client.GetPredecessorServe(context.Background(), vn)
		// Return the connection
		cs.returnConn(out)
		if err == nil {
			if vnErr.Err == "" {
				respChan <- vnErr.VN
				return
			}
			err = fmt.Errorf(vnErr.Err)
		}
		errChan <- err
	}()

	select {
	case <-time.After(cs.timeout):
		return nil, fmt.Errorf("command timed out")
	case err := <-errChan:
		return nil, err
	case res := <-respChan:
		return res, nil
	}
}

// Notify our successor of ourselves
func (cs *GRPCTransport) Notify(target, self *Vnode) ([]*Vnode, error) {
	// Get a conn
	out, err := cs.getConn(target.Host)
	if err != nil {
		return nil, err
	}

	respChan := make(chan []*Vnode, 1)
	errChan := make(chan error, 1)

	go func() {
		le, err := out.client.NotifyServe(context.Background(), &VnodePair{Target: target, Self: self})
		cs.returnConn(out)
		if err == nil {
			if le.Err == "" {
				respChan <- le.Vnodes
				return
			}
			err = fmt.Errorf(le.Err)
		}
		errChan <- err
	}()

	select {
	case <-time.After(cs.timeout):
		return nil, fmt.Errorf("Command timed out!")
	case err := <-errChan:
		return nil, err
	case res := <-respChan:
		return res, nil
	}
}

// Find a successor
func (cs *GRPCTransport) FindSuccessors(vn *Vnode, n int, k []byte) ([]*Vnode, error) {
	// Get a conn
	out, err := cs.getConn(vn.Host)
	if err != nil {
		return nil, err
	}

	respChan := make(chan []*Vnode, 1)
	errChan := make(chan error, 1)

	go func() {
		le, err := out.client.FindSuccessorsServe(context.Background(),
			&FindSuccReq{VN: vn, Count: int32(n), Key: k})
		// Return the connection
		cs.returnConn(out)
		if err == nil {
			if le.Err == "" {
				respChan <- le.Vnodes
				return
			}
			err = fmt.Errorf(le.Err)
		}
		errChan <- err
	}()

	select {
	case <-time.After(cs.timeout):
		return nil, fmt.Errorf("Command timed out!")
	case err := <-errChan:
		return nil, err
	case res := <-respChan:
		return res, nil
	}
}

// Clears a predecessor if it matches a given vnode. Used to leave.
func (cs *GRPCTransport) ClearPredecessor(target, self *Vnode) error {
	// Get a conn
	out, err := cs.getConn(target.Host)
	if err != nil {
		return err
	}

	respChan := make(chan bool, 1)
	errChan := make(chan error, 1)

	go func() {
		er, err := out.client.ClearPredecessorServe(context.Background(), &VnodePair{Target: target, Self: self})
		// Return the connection
		cs.returnConn(out)
		if err == nil {
			if er.Err == "" {
				respChan <- true
				return
			}
			err = fmt.Errorf(er.Err)
		}
		errChan <- err

	}()

	select {
	case <-time.After(cs.timeout):
		return fmt.Errorf("command timed out")
	case err := <-errChan:
		return err
	case <-respChan:
		return nil
	}
}

// SkipSuccessor instructs a node to skip a given successor. Used to leave.
func (cs *GRPCTransport) SkipSuccessor(target, self *Vnode) error {

	// Get a conn
	out, err := cs.getConn(target.Host)
	if err != nil {
		return err
	}

	respChan := make(chan bool, 1)
	errChan := make(chan error, 1)

	go func() {

		er, err := out.client.SkipSuccessorServe(context.Background(), &VnodePair{Target: target, Self: self})
		// Return the connection
		cs.returnConn(out)
		if err == nil {
			if er.Err == "" {
				respChan <- true
			}
			err = fmt.Errorf(er.Err)
		}
		errChan <- err
	}()

	select {
	case <-time.After(cs.timeout):
		return fmt.Errorf("command timed out")
	case err := <-errChan:
		return err
	case <-respChan:
		return nil
	}
}

// Gets an outbound connection to a host
func (cs *GRPCTransport) getConn(host string) (*rpcOutConn, error) {
	// Check if we have a conn cached
	var out *rpcOutConn
	cs.poolLock.Lock()
	if atomic.LoadInt32(&cs.shutdown) == 1 {
		cs.poolLock.Unlock()
		return nil, fmt.Errorf("TCP transport is shutdown")
	}
	list, ok := cs.pool[host]
	if ok && len(list) > 0 {
		out = list[len(list)-1]
		list = list[:len(list)-1]
		cs.pool[host] = list
	}
	cs.poolLock.Unlock()
	// Make a new connection
	if out == nil {
		conn, err := grpc.Dial(host, grpc.WithInsecure())
		if err == nil {
			return &rpcOutConn{
				host:   host,
				client: NewChordClient(conn),
				conn:   conn,
				used:   time.Now(),
			}, nil
		}
		return nil, err
	}
	// return an existing connection
	return out, nil
}

func (cs *GRPCTransport) returnConn(o *rpcOutConn) {
	// Update the last used time
	o.used = time.Now()
	// Push back into the pool
	cs.poolLock.Lock()
	defer cs.poolLock.Unlock()
	if atomic.LoadInt32(&cs.shutdown) == 1 {
		o.conn.Close()
		return
	}
	list, _ := cs.pool[o.host]
	cs.pool[o.host] = append(list, o)
}

// Checks for a local vnode
func (cs *GRPCTransport) get(vn *Vnode) (VnodeRPC, bool) {
	key := vn.StringID()

	cs.lock.RLock()
	defer cs.lock.RUnlock()

	w, ok := cs.local[key]
	if ok {
		return w.obj, ok
	}
	return nil, ok
}

// ListVnodesServe is the server side call
func (cs *GRPCTransport) ListVnodesServe(ctx context.Context, in *StringParam) (*VnodeListErr, error) {
	// Generate all the local clients
	resp := &VnodeListErr{Vnodes: make([]*Vnode, 0, len(cs.local))}
	// Build list
	cs.lock.RLock()
	for _, v := range cs.local {
		resp.Vnodes = append(resp.Vnodes, v.vnode)
	}
	cs.lock.RUnlock()
	return resp, nil
}
func (cs *GRPCTransport) PingServe(ctx context.Context, in *Vnode) (*BoolErr, error) {
	_, ok := cs.get(in)
	if ok {
		return &BoolErr{Ok: ok}, nil
	}
	return &BoolErr{Err: fmt.Sprintf("target vnode not found: %s/%s", in.Host, in.Id)}, nil
}
func (cs *GRPCTransport) NotifyServe(ctx context.Context, in *VnodePair) (*VnodeListErr, error) {
	obj, ok := cs.get(in.Target)
	resp := &VnodeListErr{}
	if ok {
		nodes, err := obj.Notify(in.Self)
		if err == nil {
			resp.Vnodes = trimSlice(nodes)
		} else {
			resp.Err = err.Error()
		}
	} else {
		resp.Err = fmt.Sprintf("target vnode not found: %s/%s", in.Target.Host, in.Target.Id)
	}
	return resp, nil
}
func (cs *GRPCTransport) GetPredecessorServe(ctx context.Context, in *Vnode) (*VnodeError, error) {
	obj, ok := cs.get(in)
	resp := &VnodeError{}
	if ok {
		vn, err := obj.GetPredecessor()
		if err == nil {
			resp.VN = vn
		} else {
			resp.Err = err.Error()
		}
	} else {
		resp.Err = fmt.Sprintf("target vnode not found: %s/%s", in.Host, in.Id)
	}
	return resp, nil
}
func (cs *GRPCTransport) FindSuccessorsServe(ctx context.Context, in *FindSuccReq) (*VnodeListErr, error) {
	obj, ok := cs.get(in.VN)
	resp := &VnodeListErr{}
	if ok {
		nodes, err := obj.FindSuccessors(int(in.Count), in.Key)
		if err == nil {
			resp.Vnodes = trimSlice(nodes)
		} else {
			resp.Err = err.Error()
		}
	} else {
		resp.Err = fmt.Sprintf("target vnode not found: %s/%s", in.VN.Host, in.VN.Id)
	}
	return resp, nil
}
func (cs *GRPCTransport) ClearPredecessorServe(ctx context.Context, in *VnodePair) (*ErrResponse, error) {
	obj, ok := cs.get(in.Target)
	resp := &ErrResponse{}
	if ok {
		if err := obj.ClearPredecessor(in.Self); err != nil {
			resp.Err = err.Error()
		}
	} else {
		resp.Err = fmt.Sprintf("target vnode not found: %s/%s", in.Target.Host, in.Target.Id)
	}
	return resp, nil
}
func (cs *GRPCTransport) SkipSuccessorServe(ctx context.Context, in *VnodePair) (*ErrResponse, error) {
	obj, ok := cs.get(in.Target)
	resp := &ErrResponse{}
	if ok {
		if err := obj.SkipSuccessor(in.Self); err != nil {
			resp.Err = err.Error()
		}
	} else {
		resp.Err = fmt.Sprintf("target vnode not found: %s/%s", in.Target.Host, in.Target.Id)
	}
	return resp, nil
}

// Shutdown the TCP transport
func (cs *GRPCTransport) Shutdown() {
	atomic.StoreInt32(&cs.shutdown, 1)
	cs.server.GracefulStop()
	//cs.server.Stop()
	cs.sock.Close()

	// Close all the outbound
	cs.poolLock.Lock()
	for _, conns := range cs.pool {
		for _, out := range conns {
			out.conn.Close()
		}
	}
	cs.pool = nil
	cs.poolLock.Unlock()
}
