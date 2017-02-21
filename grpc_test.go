package chord

import (
	"fmt"
	"testing"
	"time"
)

type delegateTest struct{}

func (dt *delegateTest) NewPredecessor(local, remoteNew, remotePrev *Vnode) {}
func (dt *delegateTest) Leaving(local, pred, succ *Vnode)                   {}
func (dt *delegateTest) PredecessorLeaving(local, remote *Vnode)            {}
func (dt *delegateTest) SuccessorLeaving(local, remote *Vnode)              {}
func (dt *delegateTest) Shutdown()                                          {}

func prepRingGrpc(port int) (*Config, *GRPCTransport, error) {
	listen := fmt.Sprintf("127.0.0.1:%d", port)
	conf := DefaultConfig(listen)
	conf.Delegate = &delegateTest{}
	conf.StabilizeMin = time.Duration(15 * time.Millisecond)
	conf.StabilizeMax = time.Duration(45 * time.Millisecond)
	timeout := time.Duration(20 * time.Millisecond)
	trans, err := InitGRPCTransport(listen, timeout)
	if err != nil {
		return nil, nil, err
	}
	return conf, trans, nil
}

func TestGRPCJoin(t *testing.T) {
	// Prepare to create 2 nodes
	c1, t1, err := prepRingGrpc(20025)
	if err != nil {
		t.Fatalf("unexpected err. %s", err)
	}
	c2, t2, err := prepRingGrpc(20026)
	if err != nil {
		t.Fatalf("unexpected err. %s", err)
	}

	// Create initial ring
	r1, err := Create(c1, t1)
	if err != nil {
		t.Fatalf("unexpected err. %s", err)
	}

	// Join ring
	r2, err := Join(c2, t2, c1.Hostname)
	if err != nil {
		t.Fatalf("failed to join local node! Got %s", err)
	}

	// Shutdown
	r1.Shutdown()
	r2.Shutdown()
	t1.Shutdown()
	t2.Shutdown()
}

func TestGRPCLeave(t *testing.T) {
	// Prepare to create 2 nodes
	c1, t1, err := prepRingGrpc(20027)
	if err != nil {
		t.Fatalf("unexpected err. %s", err)
	}
	c2, t2, err := prepRingGrpc(20028)
	if err != nil {
		t.Fatalf("unexpected err. %s", err)
	}

	// Create initial ring
	r1, err := Create(c1, t1)
	if err != nil {
		t.Fatalf("unexpected err. %s", err)
	}

	// Join ring
	r2, err := Join(c2, t2, c1.Hostname)
	if err != nil {
		t.Fatalf("failed to join local node! Got %s", err)
	}

	// Wait for some stabilization
	<-time.After(100 * time.Millisecond)

	// Node 1 should leave
	r1.Leave()
	t1.Shutdown()

	// Wait for stabilization
	<-time.After(100 * time.Millisecond)

	// Verify r2 ring is still in tact
	for _, vn := range r2.vnodes {
		if vn.successors[0].Host != r2.config.Hostname {
			t.Fatalf("bad successor! Got:%s:%s want: %s", vn.successors[0].Host,
				vn.successors[0].StringID(), r2.config.Hostname)
		}
	}
}
