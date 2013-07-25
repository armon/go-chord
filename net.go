package chord

import (
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

/**
 * The TCPTransport provides a TCP based Chord transport layer. This allows Chord
 * to be implemented over a network, instead of only using the LocalTransport. It is
 * meant to be a simple implementation, optimizing for simplicity instead of performance.
 * Messages are sent with a header frame, followed by a body frame. All data is encoded
 * using the GOB format for simplicity.
 *
 * Internally, there is 1 Goroutine listening for inbound connections, 1 Goroutine PER
 * inbound connection.
 */
type TCPTransport struct {
	sock     *net.TCPListener
	timeout  time.Duration
	lock     sync.RWMutex
	local    map[string]*localRPC
	inbound  map[*net.TCPConn]struct{}
	poolLock sync.Mutex
	pool     map[string][]*tcpOutConn
	shutdown bool
}

type tcpOutConn struct {
	host   string
	sock   *net.TCPConn
	header tcpHeader
	enc    *gob.Encoder
	dec    *gob.Decoder
}

const (
	tcpPing = iota
	tcpListReq
	tcpGetPredReq
	tcpNotifyReq
	tcpFindSucReq
	tcpClearPredReq
	tcpSkipSucReq
)

type tcpHeader struct {
	reqType int
}

// Potential body types
type tcpBodyError struct {
	err error
}
type tcpBodyString struct {
	s string
}
type tcpBodyVnode struct {
	vn *Vnode
}
type tcpBodyTwoVnode struct {
	target *Vnode
	vn     *Vnode
}
type tcpBodyFindSuc struct {
	target *Vnode
	num    int
	key    []byte
}
type tcpBodyVnodeError struct {
	vnode *Vnode
	err   error
}
type tcpBodyVnodeListError struct {
	vnodes []*Vnode
	err    error
}
type tcpBodyBoolError struct {
	b   bool
	err error
}

// Creates a new TCP transport on the given listen address with the
// configured timeout duration.
func InitTCPTransport(listen string, timeout time.Duration) (*TCPTransport, error) {
	// Try to start the listener
	sock, err := net.Listen("tcp", listen)
	if err != nil {
		return nil, err
	}

	// allocate maps
	local := make(map[string]*localRPC)
	inbound := make(map[*net.TCPConn]struct{})
	pool := make(map[string][]*tcpOutConn)

	// Setup the transport
	tcp := &TCPTransport{sock: sock.(*net.TCPListener),
		timeout: timeout,
		local:   local,
		inbound: inbound,
		pool:    pool}

	// Listen for connections
	go tcp.listen()

	// Done
	return tcp, nil
}

// Checks for a local vnode
func (t *TCPTransport) get(vn *Vnode) (VnodeRPC, bool) {
	key := vn.String()
	t.lock.RLock()
	defer t.lock.RUnlock()
	w, ok := t.local[key]
	if ok {
		return w.obj, ok
	} else {
		return nil, ok
	}
}

// Gets an outbound connection to a host
func (t *TCPTransport) getConn(host string) (*tcpOutConn, error) {
	// Check if we have a conn cached
	var out *tcpOutConn
	t.poolLock.Lock()
	if t.shutdown {
		t.poolLock.Unlock()
		return nil, fmt.Errorf("TCP transport is shutdown")
	}
	list, ok := t.pool[host]
	if ok && len(list) > 0 {
		out = list[len(list)-1]
		list = list[:len(list)-1]
		t.pool[host] = list
	}
	t.poolLock.Unlock()
	if out != nil {
		// Verify that the socket is valid. Might be closed.
		if _, err := out.sock.Read(nil); err == nil {
			return out, nil
		} else {
			log.Printf("[INFO] Socket closed by %s", host)
		}
	}

	// Try to establish a connection
	conn, err := net.DialTimeout("tcp", host, t.timeout)
	if err != nil {
		return nil, err
	}

	// Setup the socket
	sock := conn.(*net.TCPConn)
	t.setupConn(sock)
	enc := gob.NewEncoder(sock)
	dec := gob.NewDecoder(sock)

	// Wrap the sock
	out = &tcpOutConn{host: host, sock: sock, enc: enc, dec: dec}
	return out, nil
}

// Returns an outbound TCP connection to the pool
func (t *TCPTransport) returnConn(o *tcpOutConn) {
	t.poolLock.Lock()
	defer t.poolLock.Unlock()
	if t.shutdown {
		o.sock.Close()
		return
	}
	list, _ := t.pool[o.host]
	t.pool[o.host] = append(list, o)
}

// Setup a connection
func (t *TCPTransport) setupConn(c *net.TCPConn) {
	c.SetNoDelay(true)
	c.SetKeepAlive(true)
}

// Gets a list of the vnodes on the box
func (t *TCPTransport) ListVnodes(host string) ([]*Vnode, error) {
	// Get a conn
	out, err := t.getConn(host)
	if err != nil {
		return nil, err
	}

	// Send a list command
	out.header.reqType = tcpListReq
	body := tcpBodyString{s: host}
	if err := out.enc.Encode(&out.header); err != nil {
		return nil, err
	}
	if err := out.enc.Encode(&body); err != nil {
		return nil, err
	}

	// Read in the response
	resp := tcpBodyVnodeListError{}
	if err := out.dec.Decode(&resp); err != nil {
		return nil, err
	}

	// Return the connection
	t.returnConn(out)
	return resp.vnodes, resp.err
}

// Ping a Vnode, check for liveness
func (t *TCPTransport) Ping(vn *Vnode) (bool, error) {
	// Get a conn
	out, err := t.getConn(vn.Host)
	if err != nil {
		return false, err
	}

	// Send a list command
	out.header.reqType = tcpPing
	body := tcpBodyVnode{vn: vn}
	if err := out.enc.Encode(&out.header); err != nil {
		return false, err
	}
	if err := out.enc.Encode(&body); err != nil {
		return false, err
	}

	// Read in the response
	resp := tcpBodyBoolError{}
	if err := out.dec.Decode(&resp); err != nil {
		return false, err
	}

	// Return the connection
	t.returnConn(out)
	return resp.b, resp.err
}

// Request a nodes predecessor
func (t *TCPTransport) GetPredecessor(vn *Vnode) (*Vnode, error) {
	// Get a conn
	out, err := t.getConn(vn.Host)
	if err != nil {
		return nil, err
	}

	// Send a list command
	out.header.reqType = tcpGetPredReq
	body := tcpBodyVnode{vn: vn}
	if err := out.enc.Encode(&out.header); err != nil {
		return nil, err
	}
	if err := out.enc.Encode(&body); err != nil {
		return nil, err
	}

	// Read in the response
	resp := tcpBodyVnodeError{}
	if err := out.dec.Decode(&resp); err != nil {
		return nil, err
	}

	// Return the connection
	t.returnConn(out)
	return resp.vnode, resp.err
}

// Notify our successor of ourselves
func (t *TCPTransport) Notify(target, self *Vnode) ([]*Vnode, error) {
	// Get a conn
	out, err := t.getConn(target.Host)
	if err != nil {
		return nil, err
	}

	// Send a list command
	out.header.reqType = tcpNotifyReq
	body := tcpBodyTwoVnode{target: target, vn: self}
	if err := out.enc.Encode(&out.header); err != nil {
		return nil, err
	}
	if err := out.enc.Encode(&body); err != nil {
		return nil, err
	}

	// Read in the response
	resp := tcpBodyVnodeListError{}
	if err := out.dec.Decode(&resp); err != nil {
		return nil, err
	}

	// Return the connection
	t.returnConn(out)
	return resp.vnodes, resp.err
}

// Find a successor
func (t *TCPTransport) FindSuccessors(vn *Vnode, n int, k []byte) ([]*Vnode, error) {
	// Get a conn
	out, err := t.getConn(vn.Host)
	if err != nil {
		return nil, err
	}

	// Send a list command
	out.header.reqType = tcpFindSucReq
	body := tcpBodyFindSuc{target: vn, num: n, key: k}
	if err := out.enc.Encode(&out.header); err != nil {
		return nil, err
	}
	if err := out.enc.Encode(&body); err != nil {
		return nil, err
	}

	// Read in the response
	resp := tcpBodyVnodeListError{}
	if err := out.dec.Decode(&resp); err != nil {
		return nil, err
	}

	// Return the connection
	t.returnConn(out)
	return resp.vnodes, resp.err
}

// Clears a predecessor if it matches a given vnode. Used to leave.
func (t *TCPTransport) ClearPredecessor(target, self *Vnode) error {
	// Get a conn
	out, err := t.getConn(target.Host)
	if err != nil {
		return err
	}

	// Send a list command
	out.header.reqType = tcpClearPredReq
	body := tcpBodyTwoVnode{target: target, vn: self}
	if err := out.enc.Encode(&out.header); err != nil {
		return err
	}
	if err := out.enc.Encode(&body); err != nil {
		return err
	}

	// Read in the response
	resp := tcpBodyError{}
	if err := out.dec.Decode(&resp); err != nil {
		return err
	}

	// Return the connection
	t.returnConn(out)
	return resp.err
}

// Instructs a node to skip a given successor. Used to leave.
func (t *TCPTransport) SkipSuccessor(target, self *Vnode) error {
	// Get a conn
	out, err := t.getConn(target.Host)
	if err != nil {
		return err
	}

	// Send a list command
	out.header.reqType = tcpSkipSucReq
	body := tcpBodyTwoVnode{target: target, vn: self}
	if err := out.enc.Encode(&out.header); err != nil {
		return err
	}
	if err := out.enc.Encode(&body); err != nil {
		return err
	}

	// Read in the response
	resp := tcpBodyError{}
	if err := out.dec.Decode(&resp); err != nil {
		return err
	}

	// Return the connection
	t.returnConn(out)
	return resp.err
}

// Register for an RPC callbacks
func (t *TCPTransport) Register(v *Vnode, o VnodeRPC) {
	key := v.String()
	t.lock.Lock()
	t.local[key] = &localRPC{v, o}
	t.lock.Unlock()
}

// Shutdown the TCP transport
func (t *TCPTransport) Shutdown() {
	t.shutdown = true
	t.sock.Close()

	// Close all the inbound connections
	t.lock.RLock()
	for conn := range t.inbound {
		conn.Close()
	}
	t.lock.RUnlock()

	// Close all the outbound
	t.poolLock.Lock()
	for _, conns := range t.pool {
		for _, out := range conns {
			out.sock.Close()
		}
	}
	t.pool = nil
	t.poolLock.Unlock()
}

// Listens for inbound connections
func (t *TCPTransport) listen() {
	for {
		conn, err := t.sock.AcceptTCP()
		if err != nil {
			if !t.shutdown {
				fmt.Printf("[ERR] Error accepting TCP connection! %s", err)
				continue
			} else {
				return
			}
		}

		// Setup the conn
		t.setupConn(conn)

		// Register the inbound conn
		t.lock.Lock()
		t.inbound[conn] = struct{}{}
		t.lock.Unlock()

		// Start handler
		go t.handleConn(conn)
	}
}

// Handles inbound TCP connections
func (t *TCPTransport) handleConn(conn *net.TCPConn) {
	// Defer the cleanup
	defer func() {
		t.lock.Lock()
		delete(t.inbound, conn)
		t.lock.Unlock()
		conn.Close()
	}()

	dec := gob.NewDecoder(conn)
	enc := gob.NewEncoder(conn)
	header := tcpHeader{}
	var sendResp interface{}
	for {
		// Get the header
		if err := dec.Decode(&header); err != nil {
			log.Printf("Failed to decode TCP header! Got %s", err)
			return
		}

		// Read in the body and process request
		switch header.reqType {
		case tcpPing:
			body := tcpBodyVnode{}
			if err := dec.Decode(&body); err != nil {
				log.Printf("Failed to decode TCP body! Got %s", err)
				return
			}

			// Generate a response
			_, ok := t.get(body.vn)
			if ok {
				sendResp = tcpBodyBoolError{b: ok, err: nil}
			} else {
				sendResp = tcpBodyBoolError{b: ok, err: fmt.Errorf("Target VN not found! Target %s:%s",
					body.vn.Host, body.vn.String())}
			}

		case tcpListReq:
			body := tcpBodyString{}
			if err := dec.Decode(&body); err != nil {
				log.Printf("Failed to decode TCP body! Got %s", err)
				return
			}

			// Generate all the local clients
			res := make([]*Vnode, 0, len(t.local))

			// Build list
			t.lock.RLock()
			for _, v := range t.local {
				res = append(res, v.vnode)
			}
			t.lock.RUnlock()

			// Make response
			sendResp = tcpBodyVnodeListError{vnodes: res}

		case tcpGetPredReq:
			body := tcpBodyVnode{}
			if err := dec.Decode(&body); err != nil {
				log.Printf("Failed to decode TCP body! Got %s", err)
				return
			}

			// Generate a response
			obj, ok := t.get(body.vn)
			resp := tcpBodyVnodeError{}
			sendResp = &resp
			if ok {
				node, err := obj.GetPredecessor()
				resp.vnode = node
				resp.err = err
			} else {
				resp.err = fmt.Errorf("Target VN not found! Target %s:%s",
					body.vn.Host, body.vn.String())
			}

		case tcpNotifyReq:
			body := tcpBodyTwoVnode{}
			if err := dec.Decode(&body); err != nil {
				log.Printf("Failed to decode TCP body! Got %s", err)
				return
			}

			// Generate a response
			obj, ok := t.get(body.target)
			resp := tcpBodyVnodeListError{}
			sendResp = &resp
			if ok {
				nodes, err := obj.Notify(body.vn)
				resp.vnodes = nodes
				resp.err = err
			} else {
				resp.err = fmt.Errorf("Target VN not found! Target %s:%s",
					body.target.Host, body.target.String())
			}

		case tcpFindSucReq:
			body := tcpBodyFindSuc{}
			if err := dec.Decode(&body); err != nil {
				log.Printf("Failed to decode TCP body! Got %s", err)
				return
			}

			// Generate a response
			obj, ok := t.get(body.target)
			resp := tcpBodyVnodeListError{}
			sendResp = &resp
			if ok {
				nodes, err := obj.FindSuccessors(body.num, body.key)
				resp.vnodes = nodes
				resp.err = err
			} else {
				resp.err = fmt.Errorf("Target VN not found! Target %s:%s",
					body.target.Host, body.target.String())
			}

		case tcpClearPredReq:
			body := tcpBodyTwoVnode{}
			if err := dec.Decode(&body); err != nil {
				log.Printf("Failed to decode TCP body! Got %s", err)
				return
			}

			// Generate a response
			obj, ok := t.get(body.target)
			resp := tcpBodyError{}
			sendResp = &resp
			if ok {
				resp.err = obj.ClearPredecessor(body.vn)
			} else {
				resp.err = fmt.Errorf("Target VN not found! Target %s:%s",
					body.target.Host, body.target.String())
			}

		case tcpSkipSucReq:
			body := tcpBodyTwoVnode{}
			if err := dec.Decode(&body); err != nil {
				log.Printf("Failed to decode TCP body! Got %s", err)
				return
			}

			// Generate a response
			obj, ok := t.get(body.target)
			resp := tcpBodyError{}
			sendResp = &resp
			if ok {
				resp.err = obj.SkipSuccessor(body.vn)
			} else {
				resp.err = fmt.Errorf("Target VN not found! Target %s:%s",
					body.target.Host, body.target.String())
			}

		default:
			log.Printf("Unknown request type! Got %d", header.reqType)
			return
		}

		// Send the response
		if err := enc.Encode(sendResp); err != nil {
			log.Printf("Failed to send TCP body! Got %s", err)
			return
		}
	}
}
